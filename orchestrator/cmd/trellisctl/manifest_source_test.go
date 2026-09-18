package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseGitHubManifestSource(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantRemote bool
		want       githubManifestSource
		wantErr    string
	}{
		{name: "local file", input: "trellis.yaml"},
		{
			name:       "github shorthand",
			input:      "github.com/overfold/bower",
			wantRemote: true,
			want:       githubManifestSource{Owner: "overfold", Repo: "bower"},
		},
		{
			name:       "https git suffix",
			input:      "https://github.com/overfold/bower.git",
			wantRemote: true,
			want:       githubManifestSource{Owner: "overfold", Repo: "bower"},
		},
		{
			name:       "pinned ref",
			input:      "github.com/overfold/bower@v1.2.3",
			wantRemote: true,
			want:       githubManifestSource{Owner: "overfold", Repo: "bower", Ref: "v1.2.3"},
		},
		{
			name:       "tree ref",
			input:      "https://github.com/overfold/bower/tree/main",
			wantRemote: true,
			want:       githubManifestSource{Owner: "overfold", Repo: "bower", Ref: "main"},
		},
		{name: "insecure github", input: "http://github.com/overfold/bower", wantErr: "must use https"},
		{name: "unsupported remote", input: "https://example.com/trellis.yml", wantErr: "only GitHub repository URLs are supported"},
		{name: "blob URL", input: "https://github.com/overfold/bower/blob/main/trellis.yml", wantErr: "repository root or /tree/REF"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, remote, err := parseGitHubManifestSource(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if remote != tt.wantRemote {
				t.Fatalf("remote = %v, want %v", remote, tt.wantRemote)
			}
			if got != tt.want {
				t.Fatalf("source = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestReadGitHubManifestFallsBackToYAMLAndUsesToken(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		if got := r.URL.Query().Get("ref"); got != "v1.2.3" {
			t.Errorf("ref = %q, want v1.2.3", got)
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.github.raw+json" {
			t.Errorf("Accept = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Errorf("Authorization = %q", got)
		}
		switch r.URL.Path {
		case "/repos/overfold/demo/contents/trellis.yml":
			http.NotFound(w, r)
		case "/repos/overfold/demo/contents/trellis.yaml":
			_, _ = w.Write([]byte("name: hello\nnamespace: default\ntask_groups: []\n"))
		default:
			http.Error(w, "unexpected path", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	previousBase := githubAPIBaseURL
	githubAPIBaseURL = server.URL
	t.Cleanup(func() { githubAPIBaseURL = previousBase })
	t.Setenv("GH_TOKEN", "secret-token")

	content, label, err := readGitHubManifest(context.Background(), githubManifestSource{
		Owner: "overfold",
		Repo:  "demo",
		Ref:   "v1.2.3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(content); !strings.Contains(got, "name: hello") {
		t.Fatalf("content = %q", got)
	}
	if label != "github.com/overfold/demo@v1.2.3/trellis.yaml" {
		t.Fatalf("label = %q", label)
	}
	wantRequests := []string{
		"/repos/overfold/demo/contents/trellis.yml",
		"/repos/overfold/demo/contents/trellis.yaml",
	}
	if strings.Join(requested, ",") != strings.Join(wantRequests, ",") {
		t.Fatalf("requests = %v, want %v", requested, wantRequests)
	}
}

func TestReadGitHubManifestReportsMissingManifest(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	previousBase := githubAPIBaseURL
	githubAPIBaseURL = server.URL
	t.Cleanup(func() { githubAPIBaseURL = previousBase })
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")

	_, _, err := readGitHubManifest(context.Background(), githubManifestSource{Owner: "overfold", Repo: "demo"})
	if err == nil || !strings.Contains(err.Error(), "no trellis.yml or trellis.yaml found") {
		t.Fatalf("error = %v", err)
	}
}
