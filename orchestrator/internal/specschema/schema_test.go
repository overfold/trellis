package specschema

import (
	"bytes"
	"encoding/json"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"regexp"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/probepath"
)

func TestResourcesAndHostPortSchemaMatchAdmission(t *testing.T) {
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
		required := defs["ResourcesSpec"].(map[string]any)["required"].([]any)
		if !contains(required, "cpu") || !contains(required, "memory") {
			t.Fatal("supplied resources must require both CPU and memory")
		}
		port := property(t, defs, "PortSpec", "host_port")
		if port["minimum"] != float64(0) || port["maximum"] != float64(65535) {
			t.Fatalf("host_port must allow the zero sentinel: %+v", port)
		}
		conditions := defs["TaskNetworkingSpec"].(map[string]any)["allOf"].([]any)
		host := conditions[1].(map[string]any)["then"].(map[string]any)["properties"].(map[string]any)["ports"].(map[string]any)["items"].(map[string]any)
		if host["properties"].(map[string]any)["host_port"].(map[string]any)["const"] != float64(0) {
			t.Fatal("host networking must allow omission or zero, not nonzero host_port")
		}
	}
}

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
	for _, defs := range []map[string]any{apiDefs, yamlDefs} {
		if cpu := property(t, defs, "ResourcesSpec", "cpu"); cpu["minimum"] != float64(10) {
			t.Fatalf("CPU schema minimum = %#v, want 10", cpu)
		}
	}
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
	if memory := property(t, api["$defs"].(map[string]any), "ResourcesSpec", "memory"); memory["type"] != "integer" {
		t.Fatalf("canonical memory is not numeric: %v", memory)
	}
	memory := property(t, yaml["$defs"].(map[string]any), "ResourcesSpec", "memory")
	pattern := memory["oneOf"].([]any)[1].(map[string]any)["pattern"].(string)
	re := regexp.MustCompile(pattern)
	// Exhaust every casing of every supported unit, and plausible invalid
	// units. Amounts are bounded: overflow remains semantic parser validation,
	// not something a JSON Schema string pattern can enforce.
	for _, unit := range []string{"", "b", "kb", "mb", "gb", "tb", "ki", "kib", "mi", "mib", "gi", "gib", "ti", "tib", "k", "m", "g", "t", "ib", "mibb", "kk", "pb", "pib", "x"} {
		for casing := 0; casing < 1<<len(unit); casing++ {
			letters := []byte(unit)
			for i := range letters {
				if casing&(1<<i) != 0 {
					letters[i] -= 'a' - 'A'
				}
			}
			for _, amount := range []string{"0", "64", "0.5", "01.25", "-1", "+1", ".5", "1.", "1e2", ""} {
				for _, inner := range []string{"", " ", "\t\n\f\r", "\v", "\u00a0"} {
					for _, outer := range []string{"", " \t\n", "\v\u0085\u00a0\u1680\u2000\u200a\u2028\u2029\u202f\u205f\u3000", "\u200b", "\ufeff"} {
						v := outer + amount + inner + string(letters) + outer
						_, err := spec.ParseByteSize(v)
						if re.MatchString(v) != (err == nil) {
							t.Errorf("schema/parser mismatch for %q: parser error %v", v, err)
						}
					}
				}
			}
		}
	}
}
