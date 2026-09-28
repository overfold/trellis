package specschema

import (
	"bytes"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/clofour/trellis/internal/spec"
)

func TestGenerateDeterministic(t *testing.T) {
	apiA, yamlA, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	apiB, yamlB, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(apiA, apiB) || !bytes.Equal(yamlA, yamlB) {
		t.Fatal("schema generation is not deterministic")
	}
}

func TestYAMLSchemaOnlyAddsAuthoringRepresentation(t *testing.T) {
	apiRaw, yamlRaw, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	var api, yaml map[string]any
	if err := json.Unmarshal(apiRaw, &api); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(yamlRaw, &yaml); err != nil {
		t.Fatal(err)
	}

	apiDefs := api["$defs"].(map[string]any)
	yamlDefs := yaml["$defs"].(map[string]any)
	apiMemory := property(t, apiDefs, "ResourcesSpec", "memory")
	yamlMemory := property(t, yamlDefs, "ResourcesSpec", "memory")
	if apiMemory["type"] != "integer" {
		t.Fatalf("API memory schema = %#v", apiMemory)
	}
	if _, ok := yamlMemory["oneOf"]; !ok {
		t.Fatalf("YAML memory schema does not accept human quantities: %#v", yamlMemory)
	}
	if yamlMemory["description"] == nil {
		t.Fatalf("YAML memory schema has no authoring guidance: %#v", yamlMemory)
	}

	apiMode := property(t, apiDefs, "TaskNetworkingSpec", "mode")
	yamlMode := property(t, yamlDefs, "TaskNetworkingSpec", "mode")
	if !contains(apiMode["enum"].([]any), "") || contains(yamlMode["enum"].([]any), "") {
		t.Fatalf("default network mode representation was not derived correctly: API=%#v YAML=%#v", apiMode, yamlMode)
	}
	if !contains(yamlMode["enum"].([]any), "namespace") {
		t.Fatalf("YAML network modes = %#v, want namespace", yamlMode["enum"])
	}
	if yamlMode["description"] == nil {
		t.Fatalf("YAML networking mode has no authoring guidance: %#v", yamlMode)
	}

	rootProperties := yaml["properties"].(map[string]any)
	if rootProperties["task_groups"].(map[string]any)["description"] == nil {
		t.Fatal("YAML task_groups field has no authoring guidance")
	}
}

func property(t *testing.T, defs map[string]any, definition, name string) map[string]any {
	t.Helper()
	def, ok := defs[definition].(map[string]any)
	if !ok {
		t.Fatalf("definition %s missing", definition)
	}
	properties, ok := def["properties"].(map[string]any)
	if !ok {
		t.Fatalf("definition %s has no properties", definition)
	}
	property, ok := properties[name].(map[string]any)
	if !ok {
		t.Fatalf("property %s.%s missing", definition, name)
	}
	return property
}

func contains(values []any, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestHTTPHealthCheckPathPatternMatchesValidator(t *testing.T) {
	_, yamlRaw, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	var yaml map[string]any
	if err := json.Unmarshal(yamlRaw, &yaml); err != nil {
		t.Fatal(err)
	}
	check := yaml["$defs"].(map[string]any)["HealthCheckSpec"].(map[string]any)
	var pattern string
	for _, condition := range check["allOf"].([]any) {
		condition := condition.(map[string]any)
		typeCondition := condition["if"].(map[string]any)["properties"].(map[string]any)["type"].(map[string]any)
		if typeCondition["const"] != "http" {
			continue
		}
		path := condition["then"].(map[string]any)["properties"].(map[string]any)["path"].(map[string]any)
		pattern = path["pattern"].(string)
	}
	if pattern == "" {
		t.Fatal("HTTP health-check path pattern missing")
	}
	matcher := regexp.MustCompile(pattern)

	for path, valid := range map[string]bool{
		"":                        true,
		"/":                       true,
		"/health?ready=1":         true,
		"/@169.254.169.254/x":     true,
		"health":                  false,
		"@169.254.169.254/latest": false,
		"/health check":           false,
		"/health\r\n":             false,
		"/health#fragment":        false,
		"/héalth":                 false,
	} {
		job := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "example/server:1", HealthCheck: &spec.HealthCheckSpec{Type: spec.HealthCheckHTTP, Port: 8080, Path: path}}},
		}}}
		if got := spec.Validate(job) == nil; got != valid {
			t.Fatalf("validator accepts %q = %v, want %v", path, got, valid)
		}
		if got := matcher.MatchString(path); got != valid {
			t.Fatalf("schema pattern matches %q = %v, want %v", path, got, valid)
		}
	}
}
