package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestBackupRestoreLargeFormattedInput(t *testing.T) {
	previous := config
	t.Cleanup(func() { config = previous })
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	// Legal, individually small job records expand substantially when the
	// actual backup-create command pretty-prints their environment maps.
	env := make(map[string]string, 8192)
	for i := range 8192 {
		env[fmt.Sprintf("V%04d", i)] = "x"
	}
	job := &spec.JobSpec{Namespace: "default", Name: "job", TaskGroups: []spec.TaskGroupSpec{{Name: "group", Count: 1, Tasks: []spec.TaskSpec{{Name: "task", Image: "app", Env: env}}}}}
	if err := spec.Canonicalize(job, spec.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	snapshot := api.BackupSnapshot{FormatVersion: api.BackupFormatVersion, Jobs: map[string]json.RawMessage{}}
	for i := range 512 {
		job.Name = fmt.Sprintf("job-%d", i)
		record, err := json.Marshal(map[string]any{"Spec": job, "Revision": 1, "Version": 1, "incarnation": "test", "resolved_images": map[string]string{"app": "docker.io/library/app:latest@sha256:" + strings.Repeat("a", 64)}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot.Jobs[url.QueryEscape("default\x00"+job.Name)] = record
	}
	compact, err := json.Marshal(snapshot)
	if err != nil || len(compact) >= 64<<20 {
		t.Fatalf("fixture compact bytes=%d, %v", len(compact), err)
	}
	var restored api.BackupSnapshot
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: "test"})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/backup" {
			_ = json.NewEncoder(w).Encode(snapshot)
			return
		}
		if r.URL.Path != "/v1/backup/restore" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&restored); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	config = CLIConfig{ServerAddr: server.URL, AdminKeyData: base64.RawStdEncoding.EncodeToString(der)}
	var formatted bytes.Buffer
	create := newBackupCreateCmd()
	create.SetOut(&formatted)
	create.SetArgs([]string{"-"})
	if err := create.Execute(); err != nil {
		t.Fatal(err)
	}
	if formatted.Len() <= 64<<20 {
		t.Fatalf("fixture did not expand beyond old cap: %d", formatted.Len())
	}
	cmd := newBackupRestoreCmd()
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{"-"})
	cmd.SetIn(&formatted)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if restored.FormatVersion != api.BackupFormatVersion || len(restored.Jobs) != len(snapshot.Jobs) || string(restored.Jobs["default%00job-0"]) != string(snapshot.Jobs["default%00job-0"]) {
		t.Fatal("formatted backup did not round trip")
	}
}
