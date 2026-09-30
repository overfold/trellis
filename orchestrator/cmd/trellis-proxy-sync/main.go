// Command trellis-proxy-sync synchronizes proxy configuration from Trellis.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/client"
	"golang.org/x/sys/unix"
)

type upstream struct {
	Address string
	Port    int
	Weight  int
}

type templateData struct {
	Upstreams []upstream
}

func main() {
	var (
		labelFilter   string
		templateFile  string
		outputFile    string
		reloadCmd     string
		containerPort int
		interval      time.Duration
	)

	flag.StringVar(&labelFilter, "label", "", "label filter for allocations (e.g. route:my-app)")
	flag.StringVar(&templateFile, "template", "", "path to proxy config template")
	flag.StringVar(&outputFile, "output", "", "path to write rendered config")
	flag.StringVar(&reloadCmd, "reload-cmd", "", "command to run after config update")
	flag.IntVar(&containerPort, "container-port", 0, "container port to select when allocations expose multiple ports")
	flag.DurationVar(&interval, "interval", 5*time.Second, "poll interval")
	flag.Parse()

	if labelFilter == "" || templateFile == "" || outputFile == "" {
		fmt.Fprintln(os.Stderr, "usage: trellis-proxy-sync -label <key:value> -template <path> -output <path> [-container-port <port>] [-reload-cmd <cmd>] [-interval <duration>]")
		os.Exit(1)
	}
	if containerPort < 0 || containerPort > 65535 {
		fmt.Fprintln(os.Stderr, "container-port must be between 1 and 65535 when set")
		os.Exit(1)
	}

	token := os.Getenv("TRELLIS_TOKEN")
	addr := os.Getenv("TRELLIS_ADDR")
	namespace := os.Getenv("TRELLIS_NAMESPACE")
	if token == "" || addr == "" || namespace == "" {
		fmt.Fprintln(os.Stderr, "TRELLIS_TOKEN, TRELLIS_ADDR, and TRELLIS_NAMESPACE must be set (use api_access: namespace on the task group)")
		os.Exit(1)
	}

	tlsConfig, err := apiTLSConfig(addr, os.Getenv("TRELLIS_CA_CERT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "configure Trellis API TLS:", err)
		os.Exit(1)
	}

	log := slog.Default()

	tmplContent, err := os.ReadFile(templateFile)
	if err != nil {
		log.Error("read template", "error", err)
		os.Exit(1)
	}
	tmpl, err := template.New("config").Parse(string(tmplContent))
	if err != nil {
		log.Error("parse template", "error", err)
		os.Exit(1)
	}

	c := client.NewNamespaceServerClient(token, addr, namespace, tlsConfig)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var lastConfig string
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sync := func() {
		allocs, err := c.ListAllocations(ctx, labelFilter)
		if err != nil {
			log.Error("poll allocations", "error", err)
			return
		}

		upstreams := selectUpstreams(*allocs, containerPort)

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, templateData{Upstreams: upstreams}); err != nil {
			log.Error("render template", "error", err)
			return
		}
		rendered := buf.String()

		if rendered == lastConfig {
			return
		}

		if err := writeConfig(outputFile, []byte(rendered)); err != nil {
			log.Error("write config", "error", err)
			return
		}
		log.Info("config updated", "upstreams", len(upstreams), "output", outputFile)

		if reloadCmd != "" {
			if out, err := exec.CommandContext(ctx, "sh", "-c", reloadCmd).CombinedOutput(); err != nil {
				log.Error("reload command failed", "error", err, "output", string(out))
				return
			}
			log.Info("proxy reloaded")
		}
		lastConfig = rendered
	}

	sync()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sync()
		}
	}
}

func writeConfig(path string, data []byte) (retErr error) {
	mode := os.FileMode(0644)
	var owner *syscall.Stat_t
	for i := 0; i < 255; i++ {
		// Resolve directories before cleaning: symlink/.. refers to the
		// symlink target's parent, not the lexical parent of the link.
		dir, base := filepath.Split(path)
		if dir == "" {
			dir = "."
		}
		dir, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return err
		}
		path = dir + string(os.PathSeparator) + base
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("output config %s is not a regular file", path)
			}
			mode = info.Mode()
			var ok bool
			owner, ok = info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("read ownership of %s: unsupported file information", path)
			}
			break
		}
		target, err := os.Readlink(path)
		if err != nil {
			return err
		}
		if !filepath.IsAbs(target) {
			target = dir + string(os.PathSeparator) + target
		}
		path = target
		if i == 254 {
			return fmt.Errorf("too many symlinks in output path")
		}
	}

	// New configs retain the old WriteFile creation semantics (0644 filtered
	// by umask and default ACL). Replacements start private until metadata is set.
	createMode := os.FileMode(0644)
	if owner != nil {
		createMode = 0600
	}
	tmp, err := os.OpenFile(filepath.Join(filepath.Dir(path), ".trellis-proxy-sync-"+rand.Text()), os.O_RDWR|os.O_CREATE|os.O_EXCL, createMode)
	if err != nil {
		return err
	}
	closed := false
	renamed := false
	defer func() {
		if !closed {
			if err := tmp.Close(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close temporary config %s: %w", tmp.Name(), err))
			}
		}
		if !renamed {
			if err := os.Remove(tmp.Name()); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("remove temporary config %s: %w", tmp.Name(), err))
			}
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if owner != nil {
		if err := tmp.Chown(int(owner.Uid), int(owner.Gid)); err != nil {
			return fmt.Errorf("preserve ownership of %s: %w", path, err)
		}
		if err := tmp.Chmod(mode); err != nil {
			return err
		}
		if err := copyXattrs(path, tmp); err != nil {
			return fmt.Errorf("preserve extended attributes of %s: %w", path, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	closeErr := tmp.Close()
	closed = true
	if closeErr != nil {
		return fmt.Errorf("close temporary config %s: %w", tmp.Name(), closeErr)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	renamed = true
	return nil
}

func copyXattrs(path string, dst *os.File) error {
	source, err := listXattrNames(func(names []byte) (int, error) { return unix.Listxattr(path, names) })
	if err != nil {
		return err
	}
	destination, err := listXattrNames(func(names []byte) (int, error) { return unix.Flistxattr(int(dst.Fd()), names) })
	if err != nil {
		return err
	}
	for name := range destination {
		if _, ok := source[name]; !ok {
			if err := unix.Fremovexattr(int(dst.Fd()), name); err != nil {
				return fmt.Errorf("remove %s: %w", name, err)
			}
		}
	}
	for name := range source {
		size, err := unix.Getxattr(path, name, nil)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		value := make([]byte, size)
		n, err := unix.Getxattr(path, name, value)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if n > len(value) {
			return fmt.Errorf("read %s: %w", name, unix.ERANGE)
		}
		if err := unix.Fsetxattr(int(dst.Fd()), name, value[:n], 0); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}
	return nil
}

func listXattrNames(list func([]byte) (int, error)) (map[string]struct{}, error) {
	size, err := list(nil)
	if errors.Is(err, unix.EOPNOTSUPP) {
		return map[string]struct{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]byte, size)
	n, err := list(names)
	if errors.Is(err, unix.EOPNOTSUPP) {
		return map[string]struct{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	// A zero-length buffer is another size query. If attributes appeared
	// since the first query, fail this update rather than slicing past names.
	if n > len(names) {
		return nil, unix.ERANGE
	}
	result := make(map[string]struct{})
	for _, name := range strings.Split(string(names[:n]), "\x00") {
		if name != "" {
			result[name] = struct{}{}
		}
	}
	return result, nil
}

func selectUpstreams(allocs []api.AllocationResponse, containerPort int) []upstream {
	var upstreams []upstream
	for _, alloc := range allocs {
		if alloc.Phase != "running" || alloc.Health != "healthy" || alloc.Address == "" {
			continue
		}
		port, ok := selectPort(alloc.Ports, containerPort)
		if !ok {
			continue
		}
		weight := 1
		if w, ok := alloc.Labels["trellis/weight"]; ok {
			if parsed, err := strconv.Atoi(w); err == nil && parsed > 0 {
				weight = parsed
			}
		}
		upstreams = append(upstreams, upstream{
			Address: alloc.Address,
			Port:    port,
			Weight:  weight,
		})
	}
	return upstreams
}

// selectPort returns the port to dial at an allocation's address: the port
// the task listens on. Host-networked tasks listen on the node port itself;
// namespace-networked tasks listen on it at their namespace address, where
// the published host port does not apply. An allocation that declares no
// ports is dialed at containerPort, since namespace peers need no declared
// port to reach it.
func selectPort(ports []api.PortMapping, containerPort int) (int, bool) {
	if len(ports) == 0 {
		return containerPort, containerPort > 0
	}
	if containerPort == 0 {
		if len(ports) == 0 || ports[0].ContainerPort <= 0 {
			return 0, false
		}
		return ports[0].ContainerPort, true
	}
	for _, port := range ports {
		if port.ContainerPort == containerPort {
			return port.ContainerPort, true
		}
	}
	return 0, false
}

func apiTLSConfig(addr, caPEM string) (*tls.Config, error) {
	if strings.HasPrefix(strings.TrimSpace(addr), "http://") || caPEM == "" {
		return nil, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, fmt.Errorf("TRELLIS_CA_CERT does not contain a valid PEM certificate")
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, nil
}
