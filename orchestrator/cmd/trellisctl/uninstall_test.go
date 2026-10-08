package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

// Run the real CLI and uninstall entrypoint; only host operations and the API
// are fixtures. Verify TLS pinning, bearer auth, and actual Ed25519 signatures.
func TestUninstallRealCLIAuthentication(t *testing.T) {
	tmp := t.TempDir()
	ctl := filepath.Join(tmp, "trellisctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", ctl, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	key := base64.RawStdEncoding.EncodeToString(der)
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, certKey, err := tlsutil.GenerateAPICert(ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, certKey)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	var mu sync.Mutex
	var calls []string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: "test-challenge"})
			return
		}
		body, _ := io.ReadAll(r.Body)
		signature, _ := base64.RawURLEncoding.DecodeString(r.Header.Get(adminsign.SignatureHeader))
		admin := ed25519.Verify(public, adminsign.Payload("test-challenge", r.Method, r.URL.RequestURI(), body), signature)
		operator := r.Header.Get("Authorization") == "Bearer saved-operator"
		if !operator && !admin {
			http.Error(w, "rejected credential", http.StatusUnauthorized)
			return
		}
		if strings.Contains(r.URL.Path, "/raft/") && !admin {
			http.Error(w, "administrator required", http.StatusForbidden)
			return
		}
		calls = append(calls, fmt.Sprintf("%s %s admin=%t", r.Method, r.URL.Path, admin))
		if r.Method == http.MethodGet && r.URL.Path == "/v1/nodes" {
			_ = json.NewEncoder(w).Encode(api.NodeListResponse{{ID: id, Host: "local"}, {ID: uuid.New(), Host: "other"}})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	_ = server.Listener.Close()
	server.Listener, err = net.Listen("tcp", "127.0.0.1:8128")
	if err != nil {
		t.Fatal(err)
	}
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	for _, scenario := range []string{"sudo-user", "explicit", "missing-config", "missing-token", "rejected-token", "missing-admin", "rejected-admin"} {
		t.Run(scenario, func(t *testing.T) {
			mu.Lock()
			calls = nil
			mu.Unlock()
			root := filepath.Join(tmp, scenario)
			write := func(path, value string, mode os.FileMode) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(value), mode); err != nil {
					t.Fatal(err)
				}
			}
			data, run, bin := filepath.Join(root, "data"), filepath.Join(root, "run"), filepath.Join(root, "bin")
			write(filepath.Join(data, "node-id"), id.String(), 0600)
			write(filepath.Join(data, "retained"), "durable", 0600)
			write(filepath.Join(root, "state", "install-state"), "complete=true\n", 0600)
			write(filepath.Join(run, "ca.crt"), string(ca), 0644)
			for _, name := range []string{"systemctl", "ctr", "trellis", "sleep"} {
				write(filepath.Join(bin, name), "#!/bin/sh\nprintf '%s\\n' \"$0 $*\" >>\"$HOST_LOG\"\n", 0700)
			}
			write(filepath.Join(bin, "getent"), "#!/bin/sh\nprintf 'operator:x:1000:1000::%s:/bin/bash\\n' \"$OPERATOR_HOME\"\n", 0700)
			if err := os.Symlink(ctl, filepath.Join(bin, "trellisctl")); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(root, "operator", ".config", "trellis", "config.yaml")
			token := "saved-operator"
			if scenario == "missing-token" {
				token = ""
			}
			if scenario == "rejected-token" {
				token = "rejected"
			}
			// Deliberately wrong remote address and trust; uninstall must override both.
			if scenario != "missing-config" {
				write(config, "current_context: remote\ncontexts:\n  local:\n    server_addr: https://unreachable.invalid:8128\n    ca_cert: /missing/ca\n    token: "+token+"\n", 0600)
			}
			common, err := os.ReadFile("../../../scripts/common.sh")
			if err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(root, "common.sh"), string(common)+"\nrequire_root_linux_amd64() { :; }\nremove_owned_dependencies() { :; }\n", 0600)
			script, err := os.ReadFile("../../../scripts/uninstall.sh")
			if err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(root, "uninstall.sh"), string(script), 0600)
			env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + root, "SUDO_USER=operator", "OPERATOR_HOME=" + filepath.Join(root, "operator"), "INSTALL_DIR=" + bin, "STATE_ROOT=" + filepath.Join(root, "state"), "DATA_DIR=" + data, "CONFIG_DIR=" + filepath.Join(root, "etc"), "RUN_DIR=" + run, "SERVICE_FILE=" + filepath.Join(root, "service"), "HOST_LOG=" + filepath.Join(root, "host.log"), "TRELLIS_TOKEN=ambient-invalid"}
			if scenario == "explicit" {
				env = append(env, "TRELLIS_CONFIG="+config, "OPERATOR_HOME=/wrong")
			}
			if scenario != "missing-admin" {
				adminKey := key
				if scenario == "rejected-admin" {
					_, other, _ := ed25519.GenerateKey(rand.Reader)
					otherDER, _ := x509.MarshalPKCS8PrivateKey(other)
					adminKey = base64.RawStdEncoding.EncodeToString(otherDER)
				}
				env = append(env, "TRELLIS_ADMINISTRATOR_KEY="+adminKey)
			}
			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "uninstall.sh"), "--yes")
			cmd.Env = env
			output, err := cmd.CombinedOutput()
			success := scenario == "sudo-user" || scenario == "explicit"
			if (err == nil) != success {
				t.Fatalf("uninstall: %v\n%s", err, output)
			}
			mu.Lock()
			got := strings.Join(calls, "\n")
			mu.Unlock()
			if success {
				for _, want := range []string{"GET /v1/nodes admin=false", "POST /v1/nodes/" + id.String() + "/drain admin=false", "POST /v1/raft/leadership-transfer admin=true", "DELETE /v1/raft/members/" + id.String() + " admin=true"} {
					if !strings.Contains(got, want) {
						t.Fatalf("missing %s in %s\n%s", want, got, output)
					}
				}
			} else {
				if _, err := os.Stat(filepath.Join(data, "retained")); err != nil {
					t.Fatal("failure deleted state")
				}
				host, _ := os.ReadFile(filepath.Join(root, "host.log"))
				if strings.Contains(string(host), "stop trellis") {
					t.Fatal("failure stopped service")
				}
				if scenario == "missing-admin" && (strings.Contains(got, "/drain") || !strings.Contains(string(output), "TRELLIS_ADMINISTRATOR_KEY")) {
					t.Fatalf("missing authority drained or unhelpful error: %s", output)
				}
				if scenario == "rejected-admin" && !strings.Contains(string(output), "rejected credential") {
					t.Fatalf("hidden auth error: %s", output)
				}
			}
		})
	}
}
