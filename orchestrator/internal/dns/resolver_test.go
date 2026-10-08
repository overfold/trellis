package dns

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

type mockLookup struct {
	services *nodeapi.ServiceListResponse
}

type namespaceLookup map[netip.Prefix]string

func (n namespaceLookup) NamespaceForIP(address netip.Addr) (string, bool) {
	for prefix, namespace := range n {
		if prefix.Contains(address) {
			return namespace, true
		}
	}
	return "", false
}

func (m *mockLookup) ListDiscovery(_ context.Context) (*nodeapi.ServiceListResponse, error) {
	return m.services, nil
}

func TestResolveGroupJobNamespace(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.2"},
		{Group: "primary", Job: "db", Namespace: "acme", Address: "10.0.0.3"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("frontend.web.acme.trellis.", "")
	if len(ips) != 2 {
		t.Fatalf("expected 2 IPs for web.acme, got %d", len(ips))
	}

	ips = r.resolve("primary.db.acme.trellis.", "")
	if len(ips) != 1 {
		t.Fatalf("expected 1 IPs for db.acme, got %d", len(ips))
	}

	ips = r.resolve("missing.web.acme.trellis.", "")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for missing job, got %d", len(ips))
	}

	ips = r.resolve("frontend.web.other.trellis.", "")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for wrong namespace, got %d", len(ips))
	}

	ips = r.resolve("frontend.web.acme.example.com.", "")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for wrong domain, got %d", len(ips))
	}
}

func TestHandleQuery(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.2"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(context.Background())

	query := buildQuery("frontend.web.acme.trellis.")
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

func TestHandleQueryRestrictsDiscoveryToSourceNamespace(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Group: "frontend", Job: "web", Namespace: "other", Address: "10.1.0.1"},
	}
	namespaces := namespaceLookup{
		netip.MustParsePrefix("10.42.1.0/24"): "acme",
		netip.MustParsePrefix("10.42.2.0/24"): "other",
	}
	r := NewResolver(nil, &mockLookup{services: &services}, namespaces, "trellis")
	r.refresh(context.Background())

	allowed := r.handleQueryNetwork(buildQuery("frontend.web.acme.trellis."), "udp", &net.UDPAddr{IP: net.ParseIP("10.42.1.9"), Port: 53000})
	if got := binary.BigEndian.Uint16(allowed[6:8]); got != 1 {
		t.Fatalf("same-namespace answers = %d, want 1", got)
	}
	denied := r.handleQueryNetwork(buildQuery("frontend.web.other.trellis."), "udp", &net.UDPAddr{IP: net.ParseIP("10.42.1.9"), Port: 53000})
	if got := binary.BigEndian.Uint16(denied[6:8]); got != 0 {
		t.Fatalf("cross-namespace answers = %d, want 0", got)
	}
	unknown := r.handleQueryNetwork(buildQuery("frontend.web.acme.trellis."), "tcp", &net.TCPAddr{IP: net.ParseIP("192.0.2.9"), Port: 53000})
	if got := binary.BigEndian.Uint16(unknown[6:8]); got != 0 {
		t.Fatalf("unknown-source answers = %d, want 0", got)
	}
}

func TestDiscoveryFailsClosedWithAmbiguousAttachmentJournals(t *testing.T) {
	for _, cidr := range []string{"10.42.1.0/24", "10.42.1.128/25", "10.42.0.0/23"} {
		t.Run(cidr, func(t *testing.T) {
			stateDir := t.TempDir()
			manager, err := network.NewAutomatedWireGuardManager(stateDir)
			if err != nil {
				t.Fatal(err)
			}
			journalDir := filepath.Join(stateDir, ".attachments")
			if err := os.MkdirAll(journalDir, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, record := range []map[string]string{
				{"allocation_id": "orphan", "namespace": "acme", "network": "acme", "cidr": "10.42.1.0/24"},
				{"allocation_id": "replacement", "namespace": "other", "network": "other", "cidr": cidr},
			} {
				raw, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(journalDir, record["allocation_id"]+".json"), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			services := nodeapi.ServiceListResponse{
				{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.42.1.10"},
				{Group: "frontend", Job: "web", Namespace: "other", Address: "10.42.1.20"},
			}
			r := NewResolver(nil, &mockLookup{services: &services}, manager, "trellis")
			r.refresh(context.Background())
			for range 2 {
				for _, ns := range []string{"acme", "other"} {
					for _, transport := range []string{"udp", "tcp"} {
						response := r.handleQueryNetwork(buildQuery("frontend.web."+ns+".trellis."), transport, &net.UDPAddr{IP: net.ParseIP("10.42.1.229"), Port: 53000})
						if len(response) < 12 || binary.BigEndian.Uint16(response[6:8]) != 0 {
							t.Fatalf("ambiguous %s source received %s records: %x", transport, ns, response)
						}
					}
				}
			}
		})
	}
}

func TestBuildResponseCountsOnlyIPv4Answers(t *testing.T) {
	resp := buildResponse(1, "frontend.web.acme.trellis.", 1, 1, []net.IP{
		net.ParseIP("10.0.0.1"),
		net.ParseIP("2001:db8::1"),
	})
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 1 {
		t.Fatalf("ANCOUNT = %d, want 1", got)
	}

	resp = buildResponse(1, "frontend.web.acme.trellis.", 1, 1, []net.IP{net.ParseIP("2001:db8::1")})
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 0 {
		t.Fatalf("ANCOUNT = %d, want 0", got)
	}
	if rcode := binary.BigEndian.Uint16(resp[2:4]) & 0x000f; rcode != 0 {
		t.Fatalf("rcode = %d, want NOERROR for an existing name", rcode)
	}
}

func TestBuildResponseBoundsAnswerCount(t *testing.T) {
	ips := make([]net.IP, 65536)
	for i := range ips {
		ips[i] = net.IPv4(10, 0, 0, 1)
	}
	response := buildResponse(1, "a.", 1, 1, ips)
	if count := binary.BigEndian.Uint16(response[6:8]); count != 4094 {
		t.Fatalf("answer count = %d, want 4094", count)
	}
	// Twelve header bytes, three name bytes, four question bytes, sixteen per A record.
	if len(response) != 19+16*4094 {
		t.Fatalf("response length = %d, inconsistent with bounded answer count", len(response))
	}
	if binary.BigEndian.Uint16(response[2:4])&0x0200 == 0 {
		t.Fatal("oversized TCP answer must indicate truncation")
	}
}

func TestEncodeNameBoundsLabels(t *testing.T) {
	if got := encodeName(strings.Repeat("a", 63) + "."); len(got) != 65 || got[0] != 63 || got[64] != 0 {
		t.Fatalf("maximum-length label encoding = %v", got)
	}
	if got := encodeName(strings.Repeat("a", 64) + "."); got != nil {
		t.Fatalf("oversized label accepted: %v", got)
	}
}

func TestHandleQueryRecordTypes(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "db", Job: "bower", Namespace: "platform", Address: "10.64.0.91"},
	}
	namespaces := namespaceLookup{netip.MustParsePrefix("10.64.0.0/24"): "platform"}
	r := NewResolver(nil, &mockLookup{services: &services}, namespaces, "trellis")
	r.refresh(t.Context())
	for _, tc := range []struct {
		name    string
		query   string
		source  string
		qtype   uint16
		rcode   uint16
		answers uint16
	}{
		{"A", "db.bower.platform.trellis.", "10.64.0.169", 1, 0, 1},
		{"AAAA NODATA", "db.bower.platform.trellis.", "10.64.0.169", 28, 0, 0},
		{"MX NODATA", "db.bower.platform.trellis.", "10.64.0.169", 15, 0, 0},
		{"missing A", "missing.bower.platform.trellis.", "10.64.0.169", 1, 3, 0},
		{"missing AAAA", "missing.bower.platform.trellis.", "10.64.0.169", 28, 3, 0},
		{"denied A", "db.bower.platform.trellis.", "192.0.2.9", 1, 3, 0},
		{"denied AAAA", "db.bower.platform.trellis.", "192.0.2.9", 28, 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := buildQuery(tc.query)
			binary.BigEndian.PutUint16(query[len(query)-4:], tc.qtype)
			resp := r.handleQueryNetwork(query, "udp", &net.UDPAddr{IP: net.ParseIP(tc.source), Port: 53000})
			if got := binary.BigEndian.Uint16(resp[2:4]) & 0x000f; got != tc.rcode {
				t.Fatalf("rcode = %d, want %d", got, tc.rcode)
			}
			if got := binary.BigEndian.Uint16(resp[6:8]); got != tc.answers {
				t.Fatalf("ANCOUNT = %d, want %d", got, tc.answers)
			}
			_, offset := decodeName(resp, 12)
			if got := binary.BigEndian.Uint16(resp[offset:]); got != tc.qtype {
				t.Fatalf("question type = %d, want %d", got, tc.qtype)
			}
			if tc.answers == 1 && !net.IP(resp[len(resp)-4:]).Equal(net.ParseIP("10.64.0.91")) {
				t.Fatalf("wrong A address: %v", resp[len(resp)-4:])
			}
			if tc.answers == 0 && len(resp) != offset+4 {
				t.Fatal("empty response contains unexpected answer data")
			}
		})
	}
}

func TestHandleQueryNXDomain(t *testing.T) {
	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis")
	r.refresh(context.Background())

	query := buildQuery("missing.web.acme.trellis.")
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

func TestDualStackLookupOfIPv4OnlyName(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "db", Job: "bower", Namespace: "platform", Address: "10.64.0.91"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(t.Context())
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.serveUDP(ctx, conn) }()
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		if err := <-done; err != nil {
			t.Errorf("serve UDP: %v", err)
		}
	})
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "udp", conn.LocalAddr().String())
		},
	}
	lookupCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	addresses, err := resolver.LookupIPAddr(lookupCtx, "db.bower.platform.trellis.")
	if err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 1 || !addresses[0].IP.Equal(net.ParseIP("10.64.0.91")) {
		t.Fatalf("addresses = %v, want only 10.64.0.91", addresses)
	}
}

func TestEncodeDecodeName(t *testing.T) {
	original := "frontend.web.acme.trellis."
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
	services := nodeapi.ServiceListResponse{
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Group: "frontend", Job: "web", Namespace: "acme", Address: ""},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("frontend.web.acme.trellis.", "")
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
	services := nodeapi.ServiceListResponse{
		{Group: "frontend", Job: "web", Namespace: "acme", Address: "10.0.0.1"},
		{Group: "frontend", Job: "web", Namespace: "staging", Address: "10.0.1.1"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(context.Background())

	ips := r.resolve("frontend.web.acme.trellis.", "")
	if len(ips) != 1 || !ips[0].Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected 10.0.0.1 for acme, got %v", ips)
	}

	ips = r.resolve("frontend.web.staging.trellis.", "")
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

	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis", upstream.LocalAddr().String())
	resp := r.handleQuery(buildQuery("example.com."))
	if resp == nil {
		t.Fatal("expected forwarded response")
	}
	if flags := binary.BigEndian.Uint16(resp[2:4]); flags&0x8000 == 0 {
		t.Fatalf("expected response bit, flags=%#x", flags)
	}
}

func TestUDPSlowUpstreamIsConcurrentBoundedAndRecovers(t *testing.T) {
	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()

	received := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }
	go func() {
		for {
			buf := make([]byte, maxDNSMessageSize)
			n, remote, err := upstream.ReadFromUDP(buf)
			if err != nil {
				return
			}
			packet := append([]byte(nil), buf[:n]...)
			received <- struct{}{}
			go func() {
				<-release
				binary.BigEndian.PutUint16(packet[2:4], binary.BigEndian.Uint16(packet[2:4])|0x8000|0x0080)
				_, _ = upstream.WriteToUDP(packet, remote)
			}()
		}
	}()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis", upstream.LocalAddr().String())
	r.udpSlots = make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() { done <- r.serveUDP(ctx, conn) }()
	defer func() {
		cancel()
		_ = conn.Close()
		releaseUpstream()
		<-done
	}()

	clients := make([]*net.UDPConn, 3)
	for i := range clients {
		clients[i], err = net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer func(client *net.UDPConn) { _ = client.Close() }(clients[i])
	}
	query := buildQuery("example.com.")
	for i := range 2 {
		if _, err := clients[i].Write(query); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatal("slow upstream queries were handled serially")
		}
	}

	if _, err := clients[2].Write(query); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, maxDNSMessageSize)
	if err := clients[2].SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := clients[2].Read(response)
	if err != nil {
		t.Fatalf("read overload response: %v", err)
	}
	if rcode := binary.BigEndian.Uint16(response[:n][2:4]) & 0x000f; rcode != 2 {
		t.Fatalf("overload rcode = %d, want SERVFAIL", rcode)
	}

	releaseUpstream()
	for i := range 2 {
		if err := clients[i].SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := clients[i].Read(response); err != nil {
			t.Fatalf("read forwarded response: %v", err)
		}
	}
	waitForSlots(t, r.udpSlots, 0)
	if _, err := clients[2].Write(query); err != nil {
		t.Fatal(err)
	}
	if err := clients[2].SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err = clients[2].Read(response)
	if err != nil {
		t.Fatalf("read recovery response: %v", err)
	}
	if rcode := binary.BigEndian.Uint16(response[:n][2:4]) & 0x000f; rcode != 0 {
		t.Fatalf("recovery rcode = %d, want NOERROR", rcode)
	}
}

func TestUDPCancellationStopsBlockedUpstreamWorker(t *testing.T) {
	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()
	received := make(chan struct{})
	go func() {
		buf := make([]byte, maxDNSMessageSize)
		if _, _, err := upstream.ReadFromUDP(buf); err == nil {
			close(received)
		}
	}()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis", upstream.LocalAddr().String())
	done := make(chan error, 1)
	go func() { done <- r.serveUDP(ctx, conn) }()

	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Write(buildQuery("example.com.")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive query")
	}

	cancel()
	_ = conn.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve UDP: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UDP server did not cancel blocked upstream worker")
	}
}

func TestTCPConnectionBurstIsBoundedAndCancellationClosesConnections(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis")
	r.tcpSlots = make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() { done <- r.serveTCP(ctx, listener) }()

	first := dialTCP(t, listener.Addr().String())
	second := dialTCP(t, listener.Addr().String())
	waitForSlots(t, r.tcpSlots, 2)
	for range 32 {
		conn := dialTCP(t, listener.Addr().String())
		if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatal("overloaded TCP connection remained open")
		}
		_ = conn.Close()
	}
	if got := len(r.tcpSlots); got != 2 {
		t.Fatalf("active TCP handlers = %d, want 2", got)
	}

	_ = first.Close()
	waitForSlots(t, r.tcpSlots, 1)
	recovery := dialTCP(t, listener.Addr().String())
	waitForSlots(t, r.tcpSlots, 2)
	query := buildQuery("missing.web.acme.trellis.")
	frame := make([]byte, len(query)+2)
	binary.BigEndian.PutUint16(frame[:2], uint16(len(query)))
	copy(frame[2:], query)
	if _, err := recovery.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := recovery.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(recovery, frame[:2]); err != nil {
		t.Fatalf("read recovery frame: %v", err)
	}

	cancel()
	_ = listener.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve TCP: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TCP server did not close active connections on cancellation")
	}
	_ = second.Close()
	_ = recovery.Close()
}

func dialTCP(t *testing.T, address string) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTCP("tcp", nil, mustResolveTCPAddr(t, address))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func mustResolveTCPAddr(t *testing.T, address string) *net.TCPAddr {
	t.Helper()
	resolved, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func waitForSlots(t *testing.T, slots chan struct{}, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(slots) != want {
		if time.Now().After(deadline) {
			t.Fatalf("active slots = %d, want %d", len(slots), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestExternalQueryWithoutUpstreamReturnsServfail(t *testing.T) {
	r := NewResolver(nil, &mockLookup{services: &nodeapi.ServiceListResponse{}}, nil, "trellis")
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

func TestDiscoveryCaseFoldingPreservesNamespaceIsolation(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "Frontend", Job: "Web", Namespace: "Acme", Address: "10.0.0.7"},
		{Group: "Frontend", Job: "Web", Namespace: "Acme", Address: "10.0.0.9"},
	}
	namespaces := namespaceLookup{
		netip.MustParsePrefix("10.42.1.0/24"): "Acme",
		netip.MustParsePrefix("10.42.2.0/24"): "acme",
	}
	r := NewResolver(nil, &mockLookup{services: &services}, namespaces, "TRELLIS")
	r.refresh(t.Context())
	query := buildQuery("fRoNtEnD.wEB.ACME.TrElLiS.")
	for _, tc := range []struct {
		source string
		count  uint16
	}{
		{"10.42.1.8", 2},
		{"10.42.2.8", 0},
	} {
		response := r.handleQueryNetwork(query, "udp", &net.UDPAddr{IP: net.ParseIP(tc.source)})
		if got := binary.BigEndian.Uint16(response[6:8]); got != tc.count {
			t.Fatalf("source %s: answers = %d, want %d", tc.source, got, tc.count)
		}
		if !reflect.DeepEqual(response[12:len(query)], query[12:]) {
			t.Fatal("question case was not preserved")
		}
	}
	// A case-distinct identity must not be merged into the existing RRset.
	services = append(services, nodeapi.ServiceEntry{Group: "frontend", Job: "Web", Namespace: "Acme", Address: "10.0.0.11"})
	r.refresh(t.Context())
	if got := r.resolve("frontend.web.acme.trellis.", "Acme"); len(got) != 0 {
		t.Fatalf("ambiguous name resolved: %v", got)
	}
}

func TestDiscoverySingleLabelAndLegacyIdentitiesOverUDPAndTCP(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "API_3", Job: "Web-1", Namespace: "Team_2", Address: "10.0.0.7"},
		{Group: "api.v1", Job: "web", Namespace: "Team_2", Address: "10.0.0.8"},
		{Group: "api", Job: "v1.web", Namespace: "Team_2", Address: "10.0.0.9"},
		{Group: "api", Job: "web", Namespace: "Team_2.prod", Address: "10.0.0.10"},
		{Group: "API_3", Job: "Web-1", Namespace: "Other", Address: "10.0.0.11"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, namespaceLookup{netip.MustParsePrefix("127.0.0.0/8"): "Team_2"}, "trellis")
	r.refresh(t.Context())
	if len(r.cache) != 2 {
		t.Fatalf("legacy identities entered cache: %#v", r.cache)
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	tcp, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		_ = udp.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 2)
	go func() { done <- r.serveUDP(ctx, udp) }()
	go func() { done <- r.serveTCP(ctx, tcp) }()
	t.Cleanup(func() {
		cancel()
		_ = udp.Close()
		_ = tcp.Close()
		for range 2 {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	})
	for _, transport := range []string{"udp", "tcp"} {
		for _, tc := range []struct {
			name  string
			count uint16
		}{
			{"aPI_3.wEB-1.tEAM_2.TrElLiS.", 1},
			{"api.v1.web.Team_2.trellis.", 0},
			{"api.web.Team_2.prod.trellis.", 0},
			{"API_3.Web-1.Other.trellis.", 0},
		} {
			address := udp.LocalAddr().String()
			if transport == "tcp" {
				address = tcp.Addr().String()
			}
			conn, err := (&net.Dialer{}).DialContext(ctx, transport, address)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			query := buildQuery(tc.name)
			wire := query
			if transport == "tcp" {
				wire = make([]byte, 2+len(query))
				binary.BigEndian.PutUint16(wire[:2], uint16(len(query)))
				copy(wire[2:], query)
			}
			if _, err := conn.Write(wire); err != nil {
				t.Fatal(err)
			}
			response := make([]byte, 512)
			if transport == "tcp" {
				if _, err := io.ReadFull(conn, response[:2]); err != nil {
					t.Fatal(err)
				}
				response = response[:binary.BigEndian.Uint16(response[:2])]
				if _, err := io.ReadFull(conn, response); err != nil {
					t.Fatal(err)
				}
			} else {
				n, err := conn.Read(response)
				if err != nil {
					t.Fatal(err)
				}
				response = response[:n]
			}
			_ = conn.Close()
			if got := binary.BigEndian.Uint16(response[6:8]); got != tc.count {
				t.Fatalf("%s %s: answers=%d want %d", transport, tc.name, got, tc.count)
			}
			if !reflect.DeepEqual(response[12:len(query)], query[12:]) {
				t.Fatal("question case changed")
			}
			if tc.count == 1 && !net.IP(response[len(response)-4:]).Equal(net.ParseIP("10.0.0.7")) {
				t.Fatalf("wrong endpoint: %x", response)
			}
		}
	}
}

func TestDiscoveryUDPTruncationAndTCPFallback(t *testing.T) {
	services := nodeapi.ServiceListResponse{}
	for i := range 30 {
		services = append(services, nodeapi.ServiceEntry{Group: "frontend", Job: "web", Namespace: "acme", Address: net.IPv4(10, 0, 0, byte(i+1)).String()})
	}
	r := NewResolver(nil, &mockLookup{services: &services}, nil, "trellis")
	r.refresh(t.Context())
	query := buildQuery("frontend.web.acme.trellis.")
	for _, tc := range []struct {
		count int
		tc    bool
	}{
		{29, false}, {30, true},
	} {
		partial := services[:tc.count]
		r.lookup = &mockLookup{services: &partial}
		r.refresh(t.Context())
		response := r.handleQuery(query)
		if len(response) > 512 || (binary.BigEndian.Uint16(response[2:4])&0x0200 != 0) != tc.tc {
			t.Fatalf("%d records: size = %d, flags = %x", tc.count, len(response), response[2:4])
		}
		if tc.tc && (len(response) != len(query) || binary.BigEndian.Uint16(response[6:8]) != 0) {
			t.Fatal("truncated response is not question-only")
		}
	}
	// Exercise a real client's UDP-to-TCP retry against both serving paths.
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	tcp, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: udp.LocalAddr().(*net.UDPAddr).Port})
	if err != nil {
		_ = udp.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 2)
	go func() { done <- r.serveUDP(ctx, udp) }()
	go func() { done <- r.serveTCP(ctx, tcp) }()
	t.Cleanup(func() {
		cancel()
		_ = udp.Close()
		_ = tcp.Close()
		for range 2 {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	})
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, udp.LocalAddr().String())
	}}
	lookupCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	addresses, err := resolver.LookupIPAddr(lookupCtx, "frontend.web.acme.trellis.")
	if err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 30 {
		t.Fatalf("answers = %d, want 30", len(addresses))
	}
	seen := map[string]bool{}
	for _, address := range addresses {
		seen[address.IP.String()] = true
	}
	for _, service := range services {
		if !seen[service.Address] {
			t.Fatalf("missing address %s", service.Address)
		}
	}
}

func TestDiscoveryAAAA(t *testing.T) {
	services := nodeapi.ServiceListResponse{
		{Group: "db", Job: "web", Namespace: "acme", Address: "2001:db8::7"},
		{Group: "db", Job: "web", Namespace: "acme", Address: "10.0.0.9"},
		{Group: "db", Job: "web", Namespace: "acme", Address: "::ffff:10.0.0.11"},
	}
	r := NewResolver(nil, &mockLookup{services: &services}, namespaceLookup{netip.MustParsePrefix("10.42.1.0/24"): "acme"}, "trellis")
	r.refresh(t.Context())
	query := buildQuery("db.web.acme.trellis.")
	binary.BigEndian.PutUint16(query[len(query)-4:], 28)
	response := r.handleQueryNetwork(query, "udp", &net.UDPAddr{IP: net.ParseIP("10.42.1.8")})
	if binary.BigEndian.Uint16(response[6:8]) != 1 || len(response) != len(query)+28 {
		t.Fatalf("invalid AAAA answer: %x", response)
	}
	answer := response[len(query):]
	if binary.BigEndian.Uint16(answer[2:4]) != 28 || binary.BigEndian.Uint16(answer[10:12]) != 16 || !net.IP(answer[12:]).Equal(net.ParseIP("2001:db8::7")) {
		t.Fatalf("wrong AAAA record: %x", answer)
	}
}

func TestMalformedNamesReturnFormerr(t *testing.T) {
	r := NewResolver(nil, nil, nil, "trellis")
	for _, name := range [][]byte{
		append([]byte{64}, append([]byte(strings.Repeat("a", 64)), encodeName("web.acme.trellis.")...)...),
		{128, 0}, {192, 255}, {192, 12}, {1, 'a'},
	} {
		packet := make([]byte, 12+len(name))
		binary.BigEndian.PutUint16(packet[4:6], 1)
		copy(packet[12:], name)
		response := r.handleQuery(packet)
		if len(response) != 12 || binary.BigEndian.Uint16(response[2:4])&15 != 1 || binary.BigEndian.Uint16(response[4:6]) != 0 {
			t.Fatalf("invalid FORMERR for %x: %x", name, response)
		}
	}
	if response := buildResponse(7, strings.Repeat("a", 64)+".trellis.", 1, 1, nil); len(response) != 12 || binary.BigEndian.Uint16(response[2:4])&15 != 1 {
		t.Fatalf("encoder failure produced malformed response: %x", response)
	}
}

func TestNameWireLengthAndCompressionBounds(t *testing.T) {
	// Three 63-byte labels plus a 61-byte label and root occupy 255 bytes.
	maximum := strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("b", 61) + "."
	encoded := encodeName(maximum)
	if len(encoded) != 255 {
		t.Fatalf("maximum name length = %d", len(encoded))
	}
	if name, offset := decodeName(encoded, 0); name != maximum || offset != 255 {
		t.Fatalf("maximum name decode = %q, %d", name, offset)
	}
	oversized := append([]byte(nil), encoded...)
	oversized[192] = 62
	oversized = append(oversized[:254], 'b', 0)
	if _, offset := decodeName(oversized, 0); offset != -1 {
		t.Fatal("256-byte name accepted")
	}
	if encodeName(maximum[:len(maximum)-1]+"b.") != nil {
		t.Fatal("256-byte name encoded")
	}
	// A compressed name must enforce expanded length, not just pointer size.
	compressed := append(append([]byte(nil), encoded...), 0xc0, 0)
	if name, offset := decodeName(compressed, 255); name != maximum || offset != 257 {
		t.Fatalf("compressed name decode = %q, %d", name, offset)
	}
	if name, offset := decodeName([]byte{0}, 0); name != "." || offset != 1 || !reflect.DeepEqual(encodeName(name), []byte{0}) {
		t.Fatal("root name did not roundtrip")
	}
}
