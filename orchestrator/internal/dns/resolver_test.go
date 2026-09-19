package dns

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/clofour/trellis/internal/api"
)

type mockLookup struct {
	services *api.ServiceListResponse
}

func (m *mockLookup) ListDiscovery(_ context.Context) (*api.ServiceListResponse, error) {
	return m.services, nil
}

func TestResolveJobNamespace(t *testing.T) {
	services := api.ServiceListResponse{
		{Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Job: "web", Namespace: "acme", Address: "10.0.0.2"},
		{Job: "db", Namespace: "acme", Address: "10.0.0.3"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("web.acme.trellis.")
	if len(ips) != 2 {
		t.Fatalf("expected 2 IPs for web.acme, got %d", len(ips))
	}

	ips = r.resolve("db.acme.trellis.")
	if len(ips) != 1 {
		t.Fatalf("expected 1 IPs for db.acme, got %d", len(ips))
	}

	ips = r.resolve("missing.acme.trellis.")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for missing job, got %d", len(ips))
	}

	ips = r.resolve("web.other.trellis.")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for wrong namespace, got %d", len(ips))
	}

	ips = r.resolve("web.acme.example.com.")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for wrong domain, got %d", len(ips))
	}
}

func TestHandleQuery(t *testing.T) {
	services := api.ServiceListResponse{
		{Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Job: "web", Namespace: "acme", Address: "10.0.0.2"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, "trellis")
	r.refresh(context.Background())

	query := buildQuery("web.acme.trellis.")
	resp := r.handleQuery(query)
	if resp == nil {
		t.Fatal("expected response")
	}

	ancount := binary.BigEndian.Uint16(resp[6:8])
	if ancount != 2 {
		t.Fatalf("expected 2 answers, got %d", ancount)
	}

	flags := binary.BigEndian.Uint16(resp[2:4])
	rcode := flags & 0x000F
	if rcode != 0 {
		t.Fatalf("expected NOERROR, got rcode %d", rcode)
	}
}

func TestHandleQueryNXDomain(t *testing.T) {
	r := NewResolver(nil, &mockLookup{services: &api.ServiceListResponse{}}, "trellis")
	r.refresh(context.Background())

	query := buildQuery("missing.acme.trellis.")
	resp := r.handleQuery(query)
	if resp == nil {
		t.Fatal("expected response")
	}

	flags := binary.BigEndian.Uint16(resp[2:4])
	rcode := flags & 0x000F
	if rcode != 3 {
		t.Fatalf("expected NXDOMAIN (3), got rcode %d", rcode)
	}
}

func TestEncodeDecodeName(t *testing.T) {
	original := "web.acme.trellis."
	encoded := encodeName(original)
	decoded, offset := decodeName(encoded, 0)
	if decoded != original {
		t.Fatalf("roundtrip failed: got %q, want %q", decoded, original)
	}
	if offset != len(encoded) {
		t.Fatalf("offset %d != len %d", offset, len(encoded))
	}
}

func TestResolveIgnoresEmptyAddresses(t *testing.T) {
	services := api.ServiceListResponse{
		{Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Job: "web", Namespace: "acme", Address: ""},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("web.acme.trellis.")
	if len(ips) != 1 {
		t.Fatalf("expected 1 IP (empty address skipped), got %d", len(ips))
	}
}

func buildQuery(name string) []byte {
	var buf []byte

	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], 0x1234)
	binary.BigEndian.PutUint16(header[4:6], 1) // QDCOUNT
	buf = append(buf, header...)

	buf = append(buf, encodeName(name)...)

	trailer := make([]byte, 4)
	binary.BigEndian.PutUint16(trailer[0:2], 1)
	binary.BigEndian.PutUint16(trailer[2:4], 1)
	buf = append(buf, trailer...)

	return buf
}

func TestResolveMultipleNamespaces(t *testing.T) {
	services := api.ServiceListResponse{
		{Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Job: "web", Namespace: "staging", Address: "10.0.1.1"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("web.acme.trellis.")
	if len(ips) != 1 || !ips[0].Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected 10.0.0.1 for acme, got %v", ips)
	}

	ips = r.resolve("web.staging.trellis.")
	if len(ips) != 1 || !ips[0].Equal(net.ParseIP("10.0.1.1")) {
		t.Fatalf("expected 10.0.1.1 for staging, got %v", ips)
	}
}


func TestForwardsExternalQueriesToUpstream(t *testing.T) {
	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()

	go func() {
		buf := make([]byte, maxDNSMessageSize)
		n, remote, err := upstream.ReadFromUDP(buf)
		if err != nil {
			return
		}
		response := append([]byte(nil), buf[:n]...)
		flags := binary.BigEndian.Uint16(response[2:4])
		binary.BigEndian.PutUint16(response[2:4], flags|0x8000|0x0080)
		_, _ = upstream.WriteToUDP(response, remote)
	}()

	r := NewResolver(nil, &mockLookup{services: &api.ServiceListResponse{}}, "trellis", upstream.LocalAddr().String())
	resp := r.handleQuery(buildQuery("example.com."))
	if resp == nil {
		t.Fatal("expected forwarded response")
	}
	if flags := binary.BigEndian.Uint16(resp[2:4]); flags&0x8000 == 0 {
		t.Fatalf("expected response bit, flags=%#x", flags)
	}
}

func TestExternalQueryWithoutUpstreamReturnsServfail(t *testing.T) {
	r := NewResolver(nil, &mockLookup{services: &api.ServiceListResponse{}}, "trellis")
	resp := r.handleQuery(buildQuery("example.com."))
	if resp == nil {
		t.Fatal("expected SERVFAIL response")
	}
	if rcode := binary.BigEndian.Uint16(resp[2:4]) & 0x000f; rcode != 2 {
		t.Fatalf("expected SERVFAIL (2), got %d", rcode)
	}
}

func TestSystemResolversParsesNameservers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(path, []byte("# generated\nnameserver 127.0.0.53\nnameserver 2001:db8::53\nnameserver 127.0.0.53\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := SystemResolvers(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"127.0.0.53:53", "[2001:db8::53]:53"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolvers = %#v, want %#v", got, want)
	}
}
