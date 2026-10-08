package specschema

import (
	"bytes"
	"encoding/json"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"regexp"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/probepath"
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

func TestDiscoveryIdentifierPatterns(t *testing.T) {
	apiRaw, yamlRaw, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{apiRaw, yamlRaw} {
		var root map[string]any
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatal(err)
		}
		defs := root["$defs"].(map[string]any)
		props := root["properties"].(map[string]any)
		for _, p := range []map[string]any{props["name"].(map[string]any), props["namespace"].(map[string]any), property(t, defs, "TaskGroupSpec", "name")} {
			pattern := regexp.MustCompile(p["pattern"].(string))
			for _, name := range []string{"Web_1-", "a.b", "", "-a", "a" + string(bytes.Repeat([]byte("b"), 63))} {
				if pattern.MatchString(name) != spec.ValidDiscoveryIdentifier(name) {
					t.Fatalf("schema/validator disagree on %q", name)
				}
			}
		}
		for _, def := range []string{"TaskSpec", "SecretRefSpec", "VolumeSpec"} {
			if !regexp.MustCompile(property(t, defs, def, "name")["pattern"].(string)).MatchString("a.b") {
				t.Fatalf("%s name lost dot support", def)
			}
		}
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

func TestHTTPHealthCheckPathUsesProbePathRules(t *testing.T) {
	_, yamlRaw, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	var yaml map[string]any
	if err := json.Unmarshal(yamlRaw, &yaml); err != nil {
		t.Fatal(err)
	}
	check := yaml["$defs"].(map[string]any)["HealthCheckSpec"].(map[string]any)
	for _, condition := range check["allOf"].([]any) {
		condition := condition.(map[string]any)
		typeCondition := condition["if"].(map[string]any)["properties"].(map[string]any)["type"].(map[string]any)
		if typeCondition["const"] != "http" {
			continue
		}
		path := condition["then"].(map[string]any)["properties"].(map[string]any)["path"].(map[string]any)
		if path["pattern"] != probepath.Pattern || path["maxLength"] != float64(probepath.MaxLength) {
			t.Fatalf("HTTP path schema = %#v, want probepath rules", path)
		}
		return
	}
	t.Fatal("HTTP health-check path condition missing")
}

func TestByteSizeSchemaMatchesParser(t *testing.T) {
	_, yamlRaw, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`"pattern":\s*"(\^\[0-9\]\+\(\?:\\\\\.\[0-9\]\+\)\?\\\\s\*[^"]*)"`).FindSubmatch(yamlRaw)
	if m == nil {
		t.Fatal("byte-size pattern not found in schema")
	}
	var pattern string
	if err := json.Unmarshal([]byte(`"`+string(m[1])+`"`), &pattern); err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(pattern)
	for _, v := range []string{"64", "1B", "7K", "7KB", "7Ki", "7KiB", "64M", "64MB", "64Mi", "64MiB", "5G", "5GB", "2T", "2TB", "0.5GB", "64 MB", "1x", "64MBB", "64kk"} {
		_, err := spec.ParseByteSize(v)
		if re.MatchString(v) && err != nil {
			t.Errorf("schema accepts %q but parser rejects it: %v", v, err)
		}
		if !re.MatchString(v) && err == nil {
			t.Errorf("parser accepts %q but schema rejects it", v)
		}
	}
}
