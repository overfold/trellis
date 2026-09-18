package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/clofour/trellis/internal/spec"
)

const maxRemoteManifestBytes int64 = 1 << 20

var (
	errRemoteManifestNotFound = errors.New("remote manifest not found")
	githubAPIBaseURL           = "https://api.github.com"
	remoteManifestHTTPClient   = &http.Client{Timeout: 15 * time.Second}
)

type githubManifestSource struct {
	Owner string
	Repo  string
	Ref   string
}

func readJobManifest(ctx context.Context, source string) (*spec.JobSpec, error) {
	content, label, err := readManifestSource(ctx, source)
	if err != nil {
		return nil, err
	}
	job, err := spec.ParseYAML(content)
	if err != nil {
		return nil, fmt.Errorf("parse job manifest %s: %w", label, err)
	}
	if err := spec.Validate(job); err != nil {
		return nil, fmt.Errorf("validate job manifest %s: %w", label, err)
	}
	return job, nil
}

func readManifestSource(ctx context.Context, source string) ([]byte, string, error) {
	repository, remote, err := parseGitHubManifestSource(source)
	if err != nil {
		return nil, "", err
	}
	if remote {
		return readGitHubManifest(ctx, repository)
	}
	content, err := os.ReadFile(source)
	if err != nil {
		return nil, "", fmt.Errorf("read file %s: %w", source, err)
	}
	return content, source, nil
}

func parseGitHubManifestSource(source string) (githubManifestSource, bool, error) {
	candidate := strings.TrimSpace(source)
	lowerCandidate := strings.ToLower(candidate)
	if strings.HasPrefix(lowerCandidate, "github.com/") || strings.HasPrefix(lowerCandidate, "www.github.com/") {
		candidate = "https://" + candidate
	} else if !strings.Contains(candidate, "://") {
		return githubManifestSource{}, false, nil
	}

	parsed, err := url.Parse(candidate)
	if err != nil {
		return githubManifestSource{}, false, fmt.Errorf("parse remote manifest source %q: %w", source, err)
	}
	if !strings.EqualFold(parsed.Hostname(), "github.com") && !strings.EqualFold(parsed.Hostname(), "www.github.com") {
		return githubManifestSource{}, false, fmt.Errorf("unsupported remote manifest source %q: only GitHub repository URLs are supported", source)
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return githubManifestSource{}, false, fmt.Errorf("GitHub repository URL must use https: %q", source)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return githubManifestSource{}, false, fmt.Errorf("GitHub repository URL must not contain credentials, query parameters, or fragments: %q", source)
	}

	rawParts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return githubManifestSource{}, false, fmt.Errorf("parse GitHub repository URL %q: %w", source, err)
		}
		parts = append(parts, decoded)
	}

	var owner, repoPart, ref string
	switch {
	case len(parts) == 2:
		owner, repoPart = parts[0], parts[1]
	case len(parts) == 4 && parts[2] == "tree":
		owner, repoPart, ref = parts[0], parts[1], parts[3]
	default:
		return githubManifestSource{}, false, fmt.Errorf("GitHub source must point to a repository root or /tree/REF: %q", source)
	}

	hadInlineRef := false
	if at := strings.LastIndex(repoPart, "@"); at >= 0 {
		hadInlineRef = true
		if ref != "" {
			return githubManifestSource{}, false, fmt.Errorf("GitHub source cannot specify a ref twice: %q", source)
		}
		ref = repoPart[at+1:]
		repoPart = repoPart[:at]
	}
	if hadInlineRef && ref == "" {
		return githubManifestSource{}, false, fmt.Errorf("GitHub repository ref must not be empty: %q", source)
	}
	repo := strings.TrimSuffix(repoPart, ".git")
	if owner == "" || repo == "" {
		return githubManifestSource{}, false, fmt.Errorf("invalid GitHub repository source %q", source)
	}
	if strings.Contains(owner, "@") || strings.Contains(repo, "@") {
		return githubManifestSource{}, false, fmt.Errorf("invalid GitHub repository source %q", source)
	}
	return githubManifestSource{Owner: owner, Repo: repo, Ref: ref}, true, nil
}

func readGitHubManifest(ctx context.Context, repository githubManifestSource) ([]byte, string, error) {
	for _, name := range []string{"trellis.yml", "trellis.yaml"} {
		content, err := fetchGitHubManifestFile(ctx, repository, name)
		if errors.Is(err, errRemoteManifestNotFound) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return content, githubManifestLabel(repository, name), nil
	}
	ref := repository.Ref
	if ref == "" {
		ref = "the default branch"
	} else {
		ref = fmt.Sprintf("ref %q", ref)
	}
	return nil, "", fmt.Errorf("no trellis.yml or trellis.yaml found at the repository root of github.com/%s/%s on %s", repository.Owner, repository.Repo, ref)
}

func fetchGitHubManifestFile(ctx context.Context, repository githubManifestSource, name string) ([]byte, error) {
	endpoint, err := url.Parse(strings.TrimRight(githubAPIBaseURL, "/") + "/repos/" +
		url.PathEscape(repository.Owner) + "/" + url.PathEscape(repository.Repo) + "/contents/" + url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("build GitHub manifest URL: %w", err)
	}
	if repository.Ref != "" {
		query := endpoint.Query()
		query.Set("ref", repository.Ref)
		endpoint.RawQuery = query.Encode()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build GitHub manifest request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github.raw+json")
	request.Header.Set("User-Agent", "trellisctl")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token := githubToken(); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := remoteManifestHTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", githubManifestLabel(repository, name), err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode == http.StatusNotFound {
		return nil, errRemoteManifestNotFound
	}
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		message := strings.TrimSpace(string(detail))
		if message == "" {
			message = response.Status
		}
		return nil, fmt.Errorf("fetch %s: GitHub returned %s: %s", githubManifestLabel(repository, name), response.Status, message)
	}
	if response.ContentLength > maxRemoteManifestBytes {
		return nil, fmt.Errorf("fetch %s: manifest exceeds %d bytes", githubManifestLabel(repository, name), maxRemoteManifestBytes)
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, maxRemoteManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", githubManifestLabel(repository, name), err)
	}
	if int64(len(content)) > maxRemoteManifestBytes {
		return nil, fmt.Errorf("fetch %s: manifest exceeds %d bytes", githubManifestLabel(repository, name), maxRemoteManifestBytes)
	}
	return content, nil
}

func githubManifestLabel(repository githubManifestSource, name string) string {
	ref := ""
	if repository.Ref != "" {
		ref = "@" + repository.Ref
	}
	return fmt.Sprintf("github.com/%s/%s%s/%s", repository.Owner, repository.Repo, ref, name)
}

func githubToken() string {
	if token := strings.TrimSpace(os.Getenv("GH_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
}
