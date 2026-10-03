package dns

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

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

	ips := r.resolve("frontend.web.acme.trellis.")
	if len(ips) != 2 {
		t.Fatalf("expected 2 IPs for web.acme, got %d", len(ips))
	}

	ips = r.resolve("primary.db.acme.trellis.")
	if len(ips) != 1 {
		t.Fatalf("expected 1 IPs for db.acme, got %d", len(ips))
	}

	ips = r.resolve("missing.web.acme.trellis.")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for missing job, got %d", len(ips))
	}

	ips = r.resolve("frontend.web.other.trellis.")
	if len(ips) != 0 {
		t.Fatalf("expected 0 IPs for wrong namespace, got %d", len(ips))
	}

	ips = r.resolve("frontend.web.acme.example.com.")
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

	ips := r.resolve("frontend.web.acme.trellis.")
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

	ips := r.resolve("frontend.web.acme.trellis.")
	if len(ips) != 1 || !ips[0].Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected 10.0.0.1 for acme, got %v", ips)
	}

	ips = r.resolve("frontend.web.staging.trellis.")
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
	for i := 0; i < 2; i++ {
		if _, err := clients[i].Write(query); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
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
	for i := 0; i < 2; i++ {
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
