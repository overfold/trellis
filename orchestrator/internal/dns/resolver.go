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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/api"
)

const (
	// DefaultDomain is the default DNS suffix for Trellis services.
	DefaultDomain = "trellis"
	// DefaultTTL is the default lifetime of DNS answers, in seconds.
	DefaultTTL = 5
	maxUDPSize = 512
)

// DiscoveryLookup lists service-discovery records.
type DiscoveryLookup interface {
	ListDiscovery(ctx context.Context) (*api.ServiceListResponse, error)
}

type record struct {
	addresses []net.IP
}

// Resolver serves DNS records backed by service discovery.
type Resolver struct {
	log       *slog.Logger
	domain    string
	lookup    DiscoveryLookup
	upstreams []string

	mu    sync.RWMutex
	cache map[string]*record // "job.namespace" -> record
}

// NewResolver creates a DNS resolver for the supplied discovery source.
// Queries outside the Trellis discovery suffix are forwarded to upstreams.
func NewResolver(log *slog.Logger, lookup DiscoveryLookup, domain string, upstreams ...string) *Resolver {
	if domain == "" {
		domain = DefaultDomain
	}
	return &Resolver{
		log:       log,
		domain:    domain,
		lookup:    lookup,
		upstreams: append([]string(nil), upstreams...),
		cache:     make(map[string]*record),
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

// Run serves DNS over UDP and TCP on addr until ctx is canceled.
func (r *Resolver) Run(ctx context.Context, addr string) error {
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
	defer func() {
		_ = udpConn.Close()
		_ = tcpListener.Close()
	}()

	go r.refreshLoop(ctx)
	go func() {
		<-ctx.Done()
		_ = udpConn.Close()
		_ = tcpListener.Close()
	}()

	if r.log != nil {
		r.log.Info("dns resolver started", "addr", addr, "domain", r.domain, "upstreams", r.upstreams)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- r.serveUDP(ctx, udpConn) }()
	go func() { errCh <- r.serveTCP(ctx, tcpListener) }()
	for range 2 {
		if err := <-errCh; err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

func (r *Resolver) serveUDP(ctx context.Context, conn *net.UDPConn) error {
	buf := make([]byte, maxDNSMessageSize)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read UDP DNS query: %w", err)
		}
		response := r.handleQueryNetwork(buf[:n], "udp")
		if response != nil {
			if _, err := conn.WriteToUDP(response, remote); err != nil && ctx.Err() == nil && r.log != nil {
				r.log.Error("dns UDP write error", "error", err)
			}
		}
	}
}

func (r *Resolver) serveTCP(ctx context.Context, listener *net.TCPListener) error {
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept TCP DNS connection: %w", err)
		}
		go r.serveTCPConnection(ctx, conn)
	}
}

func (r *Resolver) serveTCPConnection(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
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
		response := r.handleQueryNetwork(packet, "tcp")
		if response == nil || len(response) > maxDNSMessageSize {
			return
		}
		binary.BigEndian.PutUint16(length[:], uint16(len(response)))
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
	resp, err := r.lookup.ListDiscovery(ctx)
	if err != nil {
		r.log.Error("dns refresh failed", "error", err)
		return
	}
	if resp == nil {
		return
	}
	cache := make(map[string]*record)
	for _, svc := range *resp {
		if svc.Address == "" {
			continue
		}
		ip := net.ParseIP(svc.Address)
		if ip == nil {
			ips, err := net.LookupIP(svc.Address)
			if err != nil || len(ips) == 0 {
				continue
			}
			ip = ips[0]
		}
		key := svc.Job + "." + svc.Namespace
		rec, ok := cache[key]
		if !ok {
			rec = &record{}
			cache[key] = rec
		}
		rec.addresses = append(rec.addresses, ip)
	}
	r.mu.Lock()
	r.cache = cache
	r.mu.Unlock()
}

func (r *Resolver) resolve(name string) []net.IP {
	suffix := "." + r.domain + "."
	if !strings.HasSuffix(name, suffix) {
		return nil
	}
	query := strings.TrimSuffix(name, suffix)
	parts := strings.SplitN(query, ".", 2)
	if len(parts) != 2 {
		return nil
	}
	job, namespace := parts[0], parts[1]
	key := job + "." + namespace

	r.mu.RLock()
	rec := r.cache[key]
	r.mu.RUnlock()
	if rec == nil {
		return nil
	}
	return rec.addresses
}

// handleQuery parses a DNS query and produces a response.
func (r *Resolver) handleQuery(packet []byte) []byte {
	return r.handleQueryNetwork(packet, "udp")
}

func (r *Resolver) handleQueryNetwork(packet []byte, network string) []byte {
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
		return nil
	}
	if !strings.HasSuffix(name, "."+r.domain+".") {
		return r.forward(packet, network)
	}

	qtype := binary.BigEndian.Uint16(packet[offset : offset+2])
	qclass := binary.BigEndian.Uint16(packet[offset+2 : offset+4])
	if qtype != 1 || qclass != 1 {
		return buildResponse(id, name, qtype, qclass, nil)
	}
	ips := r.resolve(name)
	return buildResponse(id, name, qtype, qclass, ips)
}

func (r *Resolver) forward(packet []byte, network string) []byte {
	for _, upstream := range r.upstreams {
		response, err := exchangeDNS(network, upstream, packet)
		if err == nil {
			return response
		}
		if r.log != nil {
			r.log.Warn("dns upstream query failed", "upstream", upstream, "network", network, "error", err)
		}
	}
	return buildErrorResponse(packet, 2) // SERVFAIL
}

func exchangeDNS(network, upstream string, packet []byte) ([]byte, error) {
	conn, err := net.DialTimeout(network, upstream, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return nil, err
	}

	if network == "tcp" {
		if len(packet) > maxDNSMessageSize {
			return nil, fmt.Errorf("DNS query exceeds TCP framing limit")
		}
		frame := make([]byte, 2+len(packet))
		binary.BigEndian.PutUint16(frame[:2], uint16(len(packet)))
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
		return nil
	}
	response := append([]byte(nil), packet[:offset+4]...)
	requestFlags := binary.BigEndian.Uint16(packet[2:4])
	flags := uint16(0x8000 | 0x0080) | (requestFlags & 0x0100) | (rcode & 0x000f)
	binary.BigEndian.PutUint16(response[2:4], flags)
	binary.BigEndian.PutUint16(response[6:8], 0)
	binary.BigEndian.PutUint16(response[8:10], 0)
	binary.BigEndian.PutUint16(response[10:12], 0)
	return response
}

func buildResponse(id uint16, name string, qtype, qclass uint16, ips []net.IP) []byte {
	buf := make([]byte, 0, 512)

	// Header
	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], id)
	flags := uint16(0x8000) // QR=1 (response)
	flags |= 0x0400         // AA=1 (authoritative)
	if len(ips) == 0 {
		flags |= 0x0003 // RCODE=NXDOMAIN
	}
	binary.BigEndian.PutUint16(header[2:4], flags)
	binary.BigEndian.PutUint16(header[4:6], 1)                // QDCOUNT
	binary.BigEndian.PutUint16(header[6:8], uint16(len(ips))) // ANCOUNT
	buf = append(buf, header...)

	// Question section
	buf = append(buf, encodeName(name)...)
	qtypeBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(qtypeBytes[0:2], qtype)
	binary.BigEndian.PutUint16(qtypeBytes[2:4], qclass)
	buf = append(buf, qtypeBytes...)

	// Answer section
	for _, ip := range ips {
		ipv4 := ip.To4()
		if ipv4 == nil {
			continue
		}
		// Name pointer to offset 12 (start of question name)
		buf = append(buf, 0xC0, 0x0C)
		ans := make([]byte, 10)
		binary.BigEndian.PutUint16(ans[0:2], 1)          // TYPE A
		binary.BigEndian.PutUint16(ans[2:4], 1)          // CLASS IN
		binary.BigEndian.PutUint32(ans[4:8], DefaultTTL) // TTL
		binary.BigEndian.PutUint16(ans[8:10], 4)         // RDLENGTH
		buf = append(buf, ans...)
		buf = append(buf, ipv4...)
	}

	return buf
}

func encodeName(name string) []byte {
	name = strings.TrimSuffix(name, ".")
	var buf []byte
	for _, label := range strings.Split(name, ".") {
		buf = append(buf, byte(len(label)))
		buf = append(buf, []byte(label)...)
	}
	buf = append(buf, 0)
	return buf
}

func decodeName(packet []byte, offset int) (string, int) {
	var labels []string
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
			break
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
		offset++
		if offset+length > len(packet) {
			return "", -1
		}
		labels = append(labels, string(packet[offset:offset+length]))
		offset += length
	}
	if origOffset >= 0 {
		offset = origOffset
	}
	return strings.Join(labels, ".") + ".", offset
}
