package catalog

import (
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
)

func TestLookupByJob(t *testing.T) {
	c := New()
	c.Update("acme", []ServiceInstance{
		{ID: "a1", Job: "web", Group: "frontend", Address: "10.0.0.1", Ports: []api.PortMapping{{HostPort: 8080, ContainerPort: 80}}},
		{ID: "a2", Job: "web", Group: "frontend", Address: "10.0.0.2", Ports: []api.PortMapping{{HostPort: 8081, ContainerPort: 80}}},
		{ID: "a3", Job: "db", Group: "primary", Address: "10.0.0.3", Ports: []api.PortMapping{{HostPort: 5432, ContainerPort: 5432}}},
	})

	results := c.Lookup("acme", "web")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	results = c.Lookup("acme", "db")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	results = c.Lookup("acme", "missing")
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}

	results = c.Lookup("other", "web")
	if len(results) != 0 {
		t.Fatalf("expected 0 results for wrong namespace, got %d", len(results))
	}
}

func TestListWithFilters(t *testing.T) {
	c := New()
	c.Update("acme", []ServiceInstance{
		{ID: "a1", Job: "web", Group: "frontend", Address: "10.0.0.1", Labels: map[string]string{"trellis.expose": "true", "trellis/domain": "example.com"}},
		{ID: "a2", Job: "web", Group: "api", Address: "10.0.0.2", Labels: map[string]string{"trellis.expose": "true"}},
		{ID: "a3", Job: "db", Group: "primary", Address: "10.0.0.3", Labels: map[string]string{"trellis/engine": "postgres"}},
	})

	all := c.List("acme", nil)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	byJob := c.List("acme", &ListFilter{Job: "web"})
	if len(byJob) != 2 {
		t.Fatalf("expected 2 by job, got %d", len(byJob))
	}

	byLabel := c.List("acme", &ListFilter{Label: "trellis.expose:true"})
	if len(byLabel) != 2 {
		t.Fatalf("expected 2 by label, got %d", len(byLabel))
	}

	byLabelKey := c.List("acme", &ListFilter{Label: "trellis/engine"})
	if len(byLabelKey) != 1 {
		t.Fatalf("expected 1 by label key, got %d", len(byLabelKey))
	}

	byBoth := c.List("acme", &ListFilter{Job: "web", Label: "trellis/domain:example.com"})
	if len(byBoth) != 1 {
		t.Fatalf("expected 1 by job+label, got %d", len(byBoth))
	}
}

func TestListAllNamespaces(t *testing.T) {
	c := New()
	c.Update("acme", []ServiceInstance{
		{ID: "a1", Job: "web", Group: "api", Address: "10.0.0.1"},
		{ID: "a2", Job: "db", Group: "primary", Address: "10.0.0.2"},
	})
	c.Update("staging", []ServiceInstance{
		{ID: "s1", Job: "web", Group: "api", Address: "10.0.1.1"},
	})

	all := c.List("", nil)
	if len(all) != 3 {
		t.Fatalf("expected 3 across all namespaces, got %d", len(all))
	}

	byJob := c.List("", &ListFilter{Job: "web"})
	if len(byJob) != 2 {
		t.Fatalf("expected 2 web instances across namespaces, got %d", len(byJob))
	}

	scoped := c.List("acme", nil)
	if len(scoped) != 2 {
		t.Fatalf("expected 2 in acme namespace, got %d", len(scoped))
	}
}

func TestUpdateReplacesNamespace(t *testing.T) {
	c := New()
	c.Update("acme", []ServiceInstance{{ID: "a1", Job: "web", Group: "api", Address: "10.0.0.1"}})
	if len(c.Lookup("acme", "web")) != 1 {
		t.Fatal("expected 1 after initial update")
	}

	c.Update("acme", []ServiceInstance{
		{ID: "a2", Job: "web", Group: "api", Address: "10.0.0.2"},
		{ID: "a3", Job: "web", Group: "api", Address: "10.0.0.3"},
	})
	if len(c.Lookup("acme", "web")) != 2 {
		t.Fatal("expected 2 after replacement")
	}

	c.Update("acme", nil)
	if len(c.Lookup("acme", "web")) != 0 {
		t.Fatal("expected 0 after clearing")
	}
}

func TestCatalogExcludesLegacyDottedIdentities(t *testing.T) {
	valid := ServiceInstance{ID: "valid", Job: "Web_1", Group: "API-2", Address: "10.0.0.7"}
	instances := []ServiceInstance{valid,
		{ID: "bad-job", Job: "web.v1", Group: "api", Address: "10.0.0.8"},
		{ID: "bad-group", Job: "web", Group: "api.v1", Address: "10.0.0.9"}}
	for _, mode := range []string{"update", "replace", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			c := New()
			switch mode {
			case "update":
				c.Update("Acme", instances)
				c.Update("Acme.prod", []ServiceInstance{valid})
			case "replace":
				c.Replace(map[string][]ServiceInstance{"Acme": instances, "Acme.prod": {valid}})
			case "incremental":
				c.ReplaceInstances(map[string]bool{"valid": true}, map[string][]ServiceInstance{"Acme": instances, "Acme.prod": {valid}})
			}
			entries := c.List("", nil)
			if len(entries) != 1 || entries[0].ID != "valid" || entries[0].Namespace != "Acme" {
				t.Fatalf("unexpected discovery: %#v", entries)
			}
			if len(c.Lookup("Acme.prod", valid.Job)) != 0 || len(c.Lookup("Acme", "web.v1")) != 0 {
				t.Fatal("legacy identity exposed by lookup")
			}
			valid.Group = "API.v2"
			c.ReplaceInstances(map[string]bool{"valid": true}, map[string][]ServiceInstance{"Acme": {valid}})
			if len(c.List("", nil)) != 0 {
				t.Fatal("invalid replacement retained stale instance")
			}
			valid.Group = "API-2"
		})
	}
}
