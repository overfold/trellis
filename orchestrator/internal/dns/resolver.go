// Package dns provides DNS-based Trellis service discovery.
package dns

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

const (
	// DefaultDomain is the default DNS suffix for Trellis services.
	DefaultDomain = "trellis"
	// DefaultTTL is the default lifetime of DNS answers, in seconds.
	DefaultTTL = 5

	defaultMaxConcurrentUDPQueries = 256
	defaultMaxTCPConnections       = 128
	maxDNSMessageSize              = 65535
	cacheLifetime                  = 15 * time.Second
)

// DiscoveryLookup lists service-discovery records.
type DiscoveryLookup interface {
	ListDiscovery(ctx context.Context) (*nodeapi.ServiceListResponse, error)
}

// NamespaceLookup identifies the namespace network that owns a workload IP.
type NamespaceLookup interface {
	NamespaceForIP(netip.Addr) (string, bool)
}

type record struct {
	addresses []net.IP
	identity  string
	namespace string
	ambiguous bool
}

// Resolver serves DNS records backed by service discovery.
type Resolver struct {
	log        *slog.Logger
	domain     string
	lookup     DiscoveryLookup
	namespaces NamespaceLookup
	upstreams  []string
	udpSlots   chan struct{}
	tcpSlots   chan struct{}

	mu           sync.RWMutex
	cache        map[string]*record // "group.job.namespace" -> record
	cacheExpires time.Time
	ready        atomic.Bool
}

// NewResolver creates a DNS resolver for the supplied discovery source.
// Queries outside the Trellis discovery suffix are forwarded to upstreams.
func NewResolver(log *slog.Logger, lookup DiscoveryLookup, namespaces NamespaceLookup, domain string, upstreams ...string) *Resolver {
	if domain == "" {
		domain = DefaultDomain
	}
	return &Resolver{
		log:        log,
		domain:     strings.ToLower(domain),
		lookup:     lookup,
		namespaces: namespaces,
		upstreams:  append([]string(nil), upstreams...),
		udpSlots:   make(chan struct{}, defaultMaxConcurrentUDPQueries),
		tcpSlots:   make(chan struct{}, defaultMaxTCPConnections),
		cache:      make(map[string]*record),
	}
}

// SystemResolvers reads standard nameserver entries from resolv.conf and
// returns explicit port-53 upstream addresses suitable for forwarding.
func SystemResolvers(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	seen := map[string]struct{}{}
	var result []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		ip := net.ParseIP(strings.TrimSpace(fields[1]))
		if ip == nil {
			continue
		}
		address := net.JoinHostPort(fields[1], "53")
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, address)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no nameservers found in %s", path)
	}
	return result, nil
}

// Ready reports whether both listeners are bound and serving.
func (r *Resolver) Ready() bool { return r.ready.Load() }

// RunSupervised retries listener failures until cancellation. Readiness is
// withdrawn during each failure, including failure to bind either protocol.
func (r *Resolver) RunSupervised(ctx context.Context, addr string) {
	for ctx.Err() == nil {
		if err := r.Run(ctx, addr); err != nil && r.log != nil {
			r.log.Error("dns resolver stopped; retrying", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// Run serves DNS over UDP and TCP on addr until ctx is canceled.
func (r *Resolver) Run(ctx context.Context, addr string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("resolve UDP listen address: %w", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		_ = udpConn.Close()
		return fmt.Errorf("resolve TCP listen address: %w", err)
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		_ = udpConn.Close()
		return fmt.Errorf("listen TCP: %w", err)
	}
	r.ready.Store(true)
	defer r.ready.Store(false)
	var background sync.WaitGroup
	background.Add(2)
	go func() {
		defer background.Done()
		r.refreshLoop(ctx)
	}()
	go func() {
		defer background.Done()
		<-ctx.Done()
		r.ready.Store(false)
		_ = udpConn.Close()
		_ = tcpListener.Close()
	}()
	defer func() {
		cancel()
		_ = udpConn.Close()
		_ = tcpListener.Close()
		background.Wait()
	}()

	if r.log != nil {
		r.log.Info("dns resolver started", "addr", addr, "domain", r.domain, "upstreams", r.upstreams)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- r.serveUDP(ctx, udpConn) }()
	go func() { errCh <- r.serveTCP(ctx, tcpListener) }()
	var runErr error
	for range 2 {
		if err := <-errCh; err != nil && ctx.Err() == nil && runErr == nil {
			runErr = err
			r.ready.Store(false)
			cancel()
		}
	}
	return runErr
}

func (r *Resolver) serveUDP(ctx context.Context, conn *net.UDPConn) error {
	buf := make([]byte, maxDNSMessageSize)
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read UDP DNS query: %w", err)
		}
		packet := append([]byte(nil), buf[:n]...)
		select {
		case r.udpSlots <- struct{}{}:
			workers.Go(func() {
				defer func() { <-r.udpSlots }()
				r.writeUDPResponse(ctx, conn, remote, r.handleQueryNetworkContext(ctx, packet, "udp", remote))
			})
		default:
			r.writeUDPResponse(ctx, conn, remote, buildErrorResponse(packet, 2))
		}
	}
}

func (r *Resolver) writeUDPResponse(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, response []byte) {
	if response == nil {
		return
	}
	if _, err := conn.WriteToUDP(response, remote); err != nil && ctx.Err() == nil && r.log != nil {
		r.log.Error("dns UDP write error", "error", err)
	}
}

func (r *Resolver) serveTCP(ctx context.Context, listener *net.TCPListener) error {
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept TCP DNS connection: %w", err)
		}
		select {
		case r.tcpSlots <- struct{}{}:
			connections.Go(func() {
				defer func() { <-r.tcpSlots }()
				r.serveTCPConnection(ctx, conn)
			})
		default:
			_ = conn.Close()
		}
	}
}

func (r *Resolver) serveTCPConnection(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	for {
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return
		}
		var length [2]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint16(length[:]))
		if size == 0 {
			return
		}
		packet := make([]byte, size)
		if _, err := io.ReadFull(conn, packet); err != nil {
			return
		}
		response := r.handleQueryNetworkContext(ctx, packet, "tcp", conn.RemoteAddr())
		if response == nil || len(response) > maxDNSMessageSize {
			return
		}
		binary.BigEndian.PutUint16(length[:], uint16(len(response))) //nolint:gosec // maxDNSMessageSize bounds the checked response length.
		if _, err := conn.Write(append(length[:], response...)); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func (r *Resolver) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	r.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
		}
	}
}

func (r *Resolver) refresh(ctx context.Context) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	resp, err := r.lookup.ListDiscovery(ctx)
	if err != nil {
		if r.log != nil {
			r.log.Error("dns refresh failed", "error", err)
		}
		return
	}
	if resp == nil {
		return
	}
	cache := make(map[string]*record)
	for _, svc := range *resp {
		if svc.Address == "" || !spec.ValidDiscoveryIdentifier(svc.Namespace) ||
			!spec.ValidDiscoveryIdentifier(svc.Job) || !spec.ValidDiscoveryIdentifier(svc.Group) {
			continue
		}
		ip := net.ParseIP(svc.Address)
		if ip == nil {
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, svc.Address)
			if err != nil || len(ips) == 0 {
				continue
			}
			ip = ips[0].IP
		}
		key := svc.Group + "." + svc.Job + "." + svc.Namespace
		foldedKey := strings.ToLower(key)
		rec, ok := cache[foldedKey]
		if !ok {
			rec = &record{identity: key, namespace: svc.Namespace}
			cache[foldedKey] = rec
		} else if rec.identity != key {
			// Case-distinct resource identities cannot share a DNS name.
			rec.ambiguous = true
		}
		rec.addresses = append(rec.addresses, ip)
	}
	if ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	r.cache = cache
	r.cacheExpires = started.Add(cacheLifetime)
	r.mu.Unlock()
}

func (r *Resolver) cacheFresh() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Now().Before(r.cacheExpires)
}

func (r *Resolver) resolve(name, sourceNamespace string) []net.IP {
	name = strings.ToLower(name)
	suffix := "." + r.domain + "."
	if !strings.HasSuffix(name, suffix) {
		return nil
	}
	query := strings.TrimSuffix(name, suffix)
	parts := strings.Split(query, ".")
	if len(parts) != 3 {
		return nil
	}
	group, job, namespace := parts[0], parts[1], parts[2]
	key := group + "." + job + "." + namespace

	r.mu.RLock()
	rec := r.cache[key]
	fresh := time.Now().Before(r.cacheExpires)
	r.mu.RUnlock()
	if !fresh || rec == nil || rec.ambiguous || (sourceNamespace != "" && rec.namespace != sourceNamespace) {
		return nil
	}
	return rec.addresses
}

// handleQuery parses a DNS query and produces a response.
func (r *Resolver) handleQuery(packet []byte) []byte {
	return r.handleQueryNetwork(packet, "udp", nil)
}

func (r *Resolver) handleQueryNetwork(packet []byte, network string, remote net.Addr) []byte {
	return r.handleQueryNetworkContext(context.Background(), packet, network, remote)
}

func (r *Resolver) handleQueryNetworkContext(ctx context.Context, packet []byte, network string, remote net.Addr) (response []byte) {
	authoritative := false
	defer func() {
		// EDNS is not negotiated; authoritative UDP replies use the classic limit.
		if authoritative && network == "udp" && len(response) > 512 {
			_, offset := decodeName(response, 12)
			if offset < 0 || offset+4 > 512 {
				response = nil
				return
			}
			response = response[:offset+4]
			binary.BigEndian.PutUint16(response[2:4], binary.BigEndian.Uint16(response[2:4])|0x0200)
			clear(response[6:12])
		}
	}()
	if len(packet) < 12 {
		return nil
	}
	id := binary.BigEndian.Uint16(packet[0:2])
	flags := binary.BigEndian.Uint16(packet[2:4])
	if flags&0x8000 != 0 {
		return nil // not a query
	}
	if binary.BigEndian.Uint16(packet[4:6]) == 0 {
		return nil
	}

	name, offset := decodeName(packet, 12)
	if offset < 0 || offset+4 > len(packet) {
		return buildErrorResponse(packet, 1)
	}
	foldedName := strings.ToLower(name)
	if !strings.HasSuffix(foldedName, "."+r.domain+".") {
		return r.forward(ctx, packet, network)
	}
	authoritative = true

	qtype := binary.BigEndian.Uint16(packet[offset : offset+2])
	qclass := binary.BigEndian.Uint16(packet[offset+2 : offset+4])
	sourceNamespace := ""
	if r.namespaces != nil {
		remoteIP, ok := remoteAddress(remote)
		if !ok {
			return buildResponse(id, name, qtype, qclass, nil)
		}
		namespace, ok := r.namespaces.NamespaceForIP(remoteIP)
		if !ok || !strings.EqualFold(queryNamespace(foldedName, r.domain), namespace) {
			return buildResponse(id, name, qtype, qclass, nil)
		}
		sourceNamespace = namespace
	}

	if qclass != 1 {
		return buildResponse(id, name, qtype, qclass, nil)
	}
	if !r.cacheFresh() {
		return buildErrorResponse(packet, 2) // Discovery unavailable, not NXDOMAIN.
	}
	ips := r.resolve(name, sourceNamespace)
	return buildResponse(id, name, qtype, qclass, ips)
}

func remoteAddress(remote net.Addr) (netip.Addr, bool) {
	if remote == nil {
		return netip.Addr{}, false
	}
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return netip.Addr{}, false
	}
	address, err := netip.ParseAddr(host)
	return address, err == nil
}

func queryNamespace(name, domain string) string {
	query := strings.TrimSuffix(name, "."+domain+".")
	parts := strings.Split(query, ".")
	if len(parts) != 3 {
		return ""
	}
	return parts[2]
}

func (r *Resolver) forward(ctx context.Context, packet []byte, network string) []byte {
	for _, upstream := range r.upstreams {
		response, err := exchangeDNS(ctx, network, upstream, packet)
		if err == nil {
			return response
		}
		if r.log != nil {
			r.log.Warn("dns upstream query failed", "upstream", upstream, "network", network, "error", err)
		}
	}
	return buildErrorResponse(packet, 2) // SERVFAIL
}

func exchangeDNS(ctx context.Context, network, upstream string, packet []byte) ([]byte, error) {
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, upstream)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return nil, err
	}

	if network == "tcp" {
		if len(packet) > maxDNSMessageSize {
			return nil, fmt.Errorf("DNS query exceeds TCP framing limit")
		}
		frame := make([]byte, 2+len(packet))
		binary.BigEndian.PutUint16(frame[:2], uint16(len(packet))) //nolint:gosec // maxDNSMessageSize bounds the checked packet length.
		copy(frame[2:], packet)
		if _, err := conn.Write(frame); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(conn, frame[:2]); err != nil {
			return nil, err
		}
		size := int(binary.BigEndian.Uint16(frame[:2]))
		response := make([]byte, size)
		if _, err := io.ReadFull(conn, response); err != nil {
			return nil, err
		}
		return response, nil
	}

	if _, err := conn.Write(packet); err != nil {
		return nil, err
	}
	response := make([]byte, maxDNSMessageSize)
	n, err := conn.Read(response)
	if err != nil {
		return nil, err
	}
	return response[:n], nil
}

func buildErrorResponse(packet []byte, rcode uint16) []byte {
	if len(packet) < 12 {
		return nil
	}
	_, offset := decodeName(packet, 12)
	if offset < 0 || offset+4 > len(packet) {
		response := append([]byte(nil), packet[:12]...)
		binary.BigEndian.PutUint16(response[2:4], 0x8000|(binary.BigEndian.Uint16(packet[2:4])&0x0100)|(rcode&0x000f))
		clear(response[4:12])
		return response
	}
	response := append([]byte(nil), packet[:offset+4]...)
	requestFlags := binary.BigEndian.Uint16(packet[2:4])
	flags := uint16(0x8000|0x0080) | (requestFlags & 0x0100) | (rcode & 0x000f)
	binary.BigEndian.PutUint16(response[2:4], flags)
	binary.BigEndian.PutUint16(response[6:8], 0)
	binary.BigEndian.PutUint16(response[8:10], 0)
	binary.BigEndian.PutUint16(response[10:12], 0)
	return response
}

func buildResponse(id uint16, name string, qtype, qclass uint16, ips []net.IP) []byte {
	questionName := encodeName(name)
	if questionName == nil {
		packet := make([]byte, 12)
		binary.BigEndian.PutUint16(packet[:2], id)
		return buildErrorResponse(packet, 1)
	}
	buf := make([]byte, 0, 512)
	addresses := make([]net.IP, 0, len(ips))
	recordSize := 16
	if qtype == 28 {
		recordSize = 28
	}
	maxAnswers := (maxDNSMessageSize - 12 - len(questionName) - 4) / recordSize
	truncated := false
	if (qtype == 1 || qtype == 28) && qclass == 1 {
		for _, ip := range ips {
			var address net.IP
			if qtype == 1 {
				address = ip.To4()
			} else if ip.To4() == nil {
				address = ip.To16()
			}
			if address == nil {
				continue
			}
			if len(addresses) == maxAnswers {
				truncated = true
				break
			}
			addresses = append(addresses, address)
		}
	}

	// Header
	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], id)
	flags := uint16(0x8000) // QR=1 (response)
	flags |= 0x0400         // AA=1 (authoritative)
	if truncated {
		flags |= 0x0200
	}
	// A known name with no records of the requested type is NODATA, not
	// NXDOMAIN. In particular, AAAA must not invalidate a successful A lookup.
	if len(ips) == 0 {
		flags |= 0x0003 // RCODE=NXDOMAIN
	}
	binary.BigEndian.PutUint16(header[2:4], flags)
	binary.BigEndian.PutUint16(header[4:6], 1)                      // QDCOUNT
	binary.BigEndian.PutUint16(header[6:8], uint16(len(addresses))) //nolint:gosec // Message size bounds ANCOUNT.
	buf = append(buf, header...)

	// Question section
	buf = append(buf, questionName...)
	qtypeBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(qtypeBytes[0:2], qtype)
	binary.BigEndian.PutUint16(qtypeBytes[2:4], qclass)
	buf = append(buf, qtypeBytes...)

	// Answer section
	for _, address := range addresses {
		// Name pointer to offset 12 (start of question name)
		buf = append(buf, 0xC0, 0x0C)
		ans := make([]byte, 10)
		binary.BigEndian.PutUint16(ans[0:2], qtype)                 // TYPE A or AAAA
		binary.BigEndian.PutUint16(ans[2:4], 1)                     // CLASS IN
		binary.BigEndian.PutUint32(ans[4:8], DefaultTTL)            // TTL
		binary.BigEndian.PutUint16(ans[8:10], uint16(len(address))) //nolint:gosec // Addresses are 4 or 16 bytes.
		buf = append(buf, ans...)
		buf = append(buf, address...)
	}

	return buf
}

func encodeName(name string) []byte {
	if name == "." {
		return []byte{0}
	}
	name = strings.TrimSuffix(name, ".")
	var buf []byte
	for label := range strings.SplitSeq(name, ".") {
		if len(label) == 0 || len(label) > 63 {
			return nil
		}
		buf = append(buf, byte(len(label))) //nolint:gosec // DNS labels are bounded to 63 bytes above.
		buf = append(buf, []byte(label)...)
	}
	buf = append(buf, 0)
	if len(buf) > 255 {
		return nil
	}
	return buf
}

func decodeName(packet []byte, offset int) (string, int) {
	var labels []string
	nameLength := 1 // Root terminator.
	visited := make(map[int]bool)
	origOffset := -1
	for offset < len(packet) {
		if visited[offset] {
			return "", -1 // loop
		}
		visited[offset] = true
		length := int(packet[offset])
		if length == 0 {
			offset++
			if origOffset >= 0 {
				offset = origOffset
			}
			return strings.Join(labels, ".") + ".", offset
		}
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(packet) {
				return "", -1
			}
			ptr := int(binary.BigEndian.Uint16(packet[offset:offset+2])) & 0x3FFF
			if origOffset < 0 {
				origOffset = offset + 2
			}
			offset = ptr
			continue
		}
		if length > 63 {
			return "", -1 // Reserved label encodings.
		}
		nameLength += 1 + length
		if nameLength > 255 {
			return "", -1
		}
		offset++
		if offset+length > len(packet) {
			return "", -1
		}
		labels = append(labels, string(packet[offset:offset+length]))
		offset += length
	}
	return "", -1 // Missing root terminator or out-of-bounds pointer.
}
