// Command trellis-proxy-sync synchronizes proxy configuration from Trellis.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
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

		if err := os.WriteFile(outputFile, []byte(rendered), 0644); err != nil {
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

func selectUpstreams(allocs []api.AllocationResponse, containerPort int) []upstream {
	var upstreams []upstream
	for _, alloc := range allocs {
		if alloc.Phase != "running" || alloc.Health != "healthy" || alloc.Address == "" || len(alloc.Ports) == 0 {
			continue
		}
		port, ok := selectHostPort(alloc.Ports, containerPort)
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

func selectHostPort(ports []api.PortMapping, containerPort int) (int, bool) {
	if containerPort == 0 {
		if len(ports) == 0 || ports[0].HostPort <= 0 {
			return 0, false
		}
		return ports[0].HostPort, true
	}
	for _, port := range ports {
		if port.ContainerPort == containerPort && port.HostPort > 0 {
			return port.HostPort, true
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
