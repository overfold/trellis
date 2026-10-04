package server

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/distribution/reference"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// SetImageResolver installs the registry resolver before the server starts.
// The resolver returns a digest-qualified reference, retaining the source tag.
func (s *Server) SetImageResolver(resolve func(context.Context, string) (string, error)) {
	s.resolveImage = resolve
}

func resolveRegistryImage(ctx context.Context, image string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", fmt.Errorf("invalid image reference")
	}
	named = reference.TagNameOnly(named)
	if _, pinned := named.(reference.Digested); pinned {
		return named.String(), nil
	}
	_, descriptor, err := docker.NewResolver(docker.ResolverOptions{}).Resolve(ctx, named.String())
	if err != nil {
		// Registry errors can contain authentication URLs or response bodies.
		return "", fmt.Errorf("registry resolution failed; check image availability and registry access")
	}
	pinned, err := reference.WithDigest(named, descriptor.Digest)
	if err != nil {
		return "", fmt.Errorf("registry returned an invalid image digest")
	}
	return pinned.String(), nil
}

// resolveJobImages resolves each distinct reference once, or validates the
// complete pins supplied by a plan/history consumer without contacting a registry.
func (s *Server) resolveJobImages(ctx context.Context, job *spec.JobSpec, supplied map[string]string) (map[string]string, error) {
	images := make(map[string]string)
	for _, group := range job.TaskGroups {
		for _, task := range group.Tasks {
			if _, exists := images[task.Image]; exists {
				continue
			}
			var pinned string
			var err error
			if supplied != nil {
				pinned = supplied[task.Image]
			} else {
				pinned, err = s.resolveImage(ctx, task.Image)
			}
			if err == nil {
				pinned, err = validateImagePin(task.Image, pinned)
			}
			if err != nil {
				return nil, fmt.Errorf("task group %q task %q image: %w", group.Name, task.Name, err)
			}
			images[task.Image] = pinned
		}
	}
	if supplied != nil && len(supplied) != len(images) {
		return nil, fmt.Errorf("resolved_images must contain exactly the job's image references")
	}
	return images, nil
}

func validateImagePin(image, pinned string) (string, error) {
	source, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", fmt.Errorf("invalid image reference")
	}
	source = reference.TagNameOnly(source)
	resolved, err := reference.ParseNormalizedNamed(pinned)
	if err != nil {
		return "", fmt.Errorf("resolved image must be a digest-qualified reference")
	}
	digested, ok := resolved.(reference.Digested)
	if !ok {
		return "", fmt.Errorf("resolved image must be a digest-qualified reference")
	}
	expected, err := reference.WithDigest(source, digested.Digest())
	if err != nil || expected.String() != resolved.String() {
		return "", fmt.Errorf("resolved image must retain the authored repository and tag")
	}
	if original, ok := source.(reference.Digested); ok && original.Digest() != digested.Digest() {
		return "", fmt.Errorf("resolved image must retain the authored digest")
	}
	return resolved.String(), nil
}

// executionSpec copies only the slices whose image fields it replaces. Authored
// specs and scheduler inputs remain immutable; other task fields are read-only.
func executionSpec(job *spec.JobSpec, images map[string]string) *spec.JobSpec {
	result := *job
	result.TaskGroups = slices.Clone(job.TaskGroups)
	for i := range result.TaskGroups {
		group := &result.TaskGroups[i]
		group.Tasks = slices.Clone(group.Tasks)
		for j := range group.Tasks {
			if pinned, ok := images[group.Tasks[j].Image]; ok {
				group.Tasks[j].Image = pinned
			}
		}
	}
	return &result
}
