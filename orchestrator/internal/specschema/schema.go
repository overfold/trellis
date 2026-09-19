// Package specschema generates the machine JSON and first-party YAML schemas
// for Trellis job specifications from the canonical Go model.
package specschema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/clofour/trellis/internal/spec"
)

const (
	apiID  = "https://raw.githubusercontent.com/clofour/trellis/main/schemas/trellis-job-api.schema.json"
	yamlID = "https://raw.githubusercontent.com/clofour/trellis/main/schemas/trellis-job.schema.json"
)

var (
	durationType = reflect.TypeOf(time.Duration(0))
	byteSizeType = reflect.TypeOf(spec.ByteSize(0))
)

type schema = map[string]any

var requiredFields = map[reflect.Type][]string{
	reflect.TypeOf(spec.JobSpec{}):           {"name", "namespace", "task_groups"},
	reflect.TypeOf(spec.TaskGroupSpec{}):     {"name", "count", "tasks"},
	reflect.TypeOf(spec.APIAccessSpec{}):     {"scope", "access"},
	reflect.TypeOf(spec.ConstraintSpec{}):    {"attribute", "value"},
	reflect.TypeOf(spec.RestartPolicySpec{}): {"window"},
	reflect.TypeOf(spec.TaskSpec{}):          {"name", "image"},
	reflect.TypeOf(spec.SecretRefSpec{}):     {"name", "target"},
	reflect.TypeOf(spec.PortSpec{}):          {"port"},
	reflect.TypeOf(spec.HealthCheckSpec{}):   {"type"},
	reflect.TypeOf(spec.VolumeSpec{}):        {"name", "host_path", "container_path"},
}

var enumValues = map[reflect.Type][]string{
	reflect.TypeOf(spec.UpdateStrategy("")):  {"", "recreate", "rolling"},
	reflect.TypeOf(spec.Runtime("")):         {"", "runc", "runsc"},
	reflect.TypeOf(spec.APIAccessScope("")):  {"namespace", "cluster"},
	reflect.TypeOf(spec.APIAccessLevel("")):  {"read", "write"},
	reflect.TypeOf(spec.TaskNetworkMode("")): {"", "isolated", "host", "namespace"},
	reflect.TypeOf(spec.HealthCheckType("")): {"http", "tcp", "script"},
	reflect.TypeOf(spec.SecretTarget("")):    {"env", "file"},
}

type generator struct {
	defs schema
}

// Generate returns deterministic canonical-JSON and first-party YAML schemas.
func Generate() ([]byte, []byte, error) {
	g := &generator{defs: schema{}}
	root, err := g.structSchema(reflect.TypeOf(spec.JobSpec{}))
	if err != nil {
		return nil, nil, err
	}
	root["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	root["$id"] = apiID
	root["title"] = "Trellis canonical JSON JobSpec"
	root["description"] = "Canonical JSON representation accepted by the Trellis API. Human authoring syntaxes must convert quantities and durations before submission."
	root["$defs"] = g.defs
	applySemanticConstraints(root)

	apiBytes, err := marshal(root)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal API schema: %w", err)
	}

	yamlRoot, err := clone(root)
	if err != nil {
		return nil, nil, fmt.Errorf("clone API schema: %w", err)
	}
	deriveYAML(yamlRoot)
	yamlBytes, err := marshal(yamlRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal YAML schema: %w", err)
	}
	return apiBytes, yamlBytes, nil
}

func (g *generator) structSchema(t reflect.Type) (schema, error) {
	properties := schema{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := jsonFieldName(field)
		if name == "" {
			continue
		}
		child, err := g.typeSchema(field.Type)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t.Name(), field.Name, err)
		}
		properties[name] = child
	}
	out := schema{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if required := requiredFields[t]; len(required) > 0 {
		out["required"] = required
	}
	return out, nil
}

func (g *generator) typeSchema(t reflect.Type) (schema, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == durationType {
		return schema{"type": "integer", "description": "Nanoseconds."}, nil
	}
	if t == byteSizeType {
		return schema{"type": "integer", "minimum": 0, "description": "Bytes."}, nil
	}
	if values, ok := enumValues[t]; ok {
		return schema{"type": "string", "enum": values}, nil
	}

	switch t.Kind() {
	case reflect.Struct:
		name := t.Name()
		if name == "" {
			return nil, fmt.Errorf("anonymous structs are not supported")
		}
		if _, exists := g.defs[name]; !exists {
			g.defs[name] = schema{}
			definition, err := g.structSchema(t)
			if err != nil {
				return nil, err
			}
			g.defs[name] = definition
		}
		return schema{"$ref": "#/$defs/" + name}, nil
	case reflect.Slice, reflect.Array:
		item, err := g.typeSchema(t.Elem())
		if err != nil {
			return nil, err
		}
		return schema{"type": "array", "items": item}, nil
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("only string-keyed maps are supported")
		}
		value, err := g.typeSchema(t.Elem())
		if err != nil {
			return nil, err
		}
		return schema{"type": "object", "additionalProperties": value}, nil
	case reflect.String:
		return schema{"type": "string"}, nil
	case reflect.Bool:
		return schema{"type": "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return schema{"type": "integer"}, nil
	default:
		return nil, fmt.Errorf("unsupported Go type %s", t)
	}
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return ""
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "-" {
		return ""
	}
	if name == "" {
		return field.Name
	}
	return name
}

func applySemanticConstraints(root schema) {
	set(root, []string{"properties", "name"}, identifier())
	set(root, []string{"properties", "namespace"}, identifier())
	patch(root, []string{"properties", "task_groups"}, schema{"minItems": 1})

	patchDef(root, "TaskGroupSpec", "name", identifier())
	patchDef(root, "TaskGroupSpec", "count", schema{"minimum": 1})
	patchDef(root, "TaskGroupSpec", "tasks", schema{"minItems": 1})
	patchDef(root, "TaskGroupSpec", "labels", schema{
		"propertyNames":        schema{"pattern": `^[a-zA-Z][a-zA-Z0-9._/-]{0,62}$`},
		"additionalProperties": schema{"type": "string", "maxLength": 256},
	})

	patchDef(root, "ConstraintSpec", "attribute", schema{"pattern": `^[a-zA-Z][a-zA-Z0-9._/-]{0,62}$`})
	patchDef(root, "ConstraintSpec", "value", schema{"minLength": 1})
	patchDef(root, "RestartPolicySpec", "max_restarts", schema{"minimum": 0})
	patchDef(root, "RestartPolicySpec", "window", schema{"minimum": 1})
	patchDef(root, "UpdateSpec", "max_parallel", schema{"minimum": 0})

	patchDef(root, "TaskSpec", "name", identifier())
	patchDef(root, "TaskSpec", "image", schema{"minLength": 1})
	patchDef(root, "ResourcesSpec", "cpu", schema{"minimum": 1})
	patchDef(root, "ResourcesSpec", "memory", schema{"minimum": 1})

	patchDef(root, "PortSpec", "port", schema{"minimum": 1, "maximum": 65535})
	patchDef(root, "HealthCheckSpec", "port", schema{"minimum": 0, "maximum": 65535})
	patchDef(root, "HealthCheckSpec", "interval", schema{"minimum": 0})
	patchDef(root, "HealthCheckSpec", "timeout", schema{"minimum": 0})
	patchDef(root, "HealthCheckSpec", "threshold", schema{"minimum": 0})
	addHealthCheckConditions(def(root, "HealthCheckSpec"))

	patchDef(root, "SecretRefSpec", "name", identifier())
	patchDef(root, "SecretRefSpec", "env", schema{"pattern": `^[A-Za-z_][A-Za-z0-9_]*$`})
	patchDef(root, "SecretRefSpec", "path", schema{"pattern": `^/run/trellis-secrets/`})
	setDef(root, "SecretRefSpec", "mode", schema{"type": "integer", "enum": []int{0, 256, 384}})
	addSecretConditions(def(root, "SecretRefSpec"))

	patchDef(root, "VolumeSpec", "name", identifier())
	patchDef(root, "VolumeSpec", "host_path", schema{"pattern": `^(?:/|@/)`})
	patchDef(root, "VolumeSpec", "container_path", schema{"pattern": `^/`})
	addNetworkingConditions(def(root, "TaskNetworkingSpec"))
}

func addHealthCheckConditions(check schema) {
	check["allOf"] = []schema{
		{
			"if": schema{"properties": schema{"type": schema{"enum": []string{"http", "tcp"}}}},
			"then": schema{
				"required":   []string{"port"},
				"properties": schema{"port": schema{"minimum": 1}},
			},
		},
		{
			"if": schema{"properties": schema{"type": schema{"const": "script"}}},
			"then": schema{
				"required":   []string{"command"},
				"properties": schema{"command": schema{"minItems": 1}},
			},
		},
	}
}

func addSecretConditions(secret schema) {
	secret["allOf"] = []schema{
		{
			"if":   schema{"properties": schema{"target": schema{"const": "env"}}},
			"then": schema{"required": []string{"env"}},
		},
		{
			"if":   schema{"properties": schema{"target": schema{"const": "file"}}},
			"then": schema{"required": []string{"path"}},
		},
	}
}

func addNetworkingConditions(networking schema) {
	networking["allOf"] = []schema{
		{
			"if": schema{
				"not": schema{
					"required":   []string{"mode"},
					"properties": schema{"mode": schema{"const": "host"}},
				},
			},
			"then": schema{"properties": schema{"ports": schema{"maxItems": 0}}},
		},
	}
}

func deriveYAML(root schema) {
	root["$id"] = yamlID
	root["title"] = "Trellis job manifest"
	root["description"] = "Schema for the first-party human-authored YAML representation. Trellis itself consumes canonical JSON after the consumer converts human quantities."

	removeEmptyEnum(defProperty(root, "TaskGroupSpec", "runtime"))
	removeEmptyEnum(defProperty(root, "UpdateSpec", "strategy"))
	removeEmptyEnum(defProperty(root, "TaskNetworkingSpec", "mode"))

	setDef(root, "ResourcesSpec", "memory", humanByteSize(defProperty(root, "ResourcesSpec", "memory")))
	setDef(root, "RestartPolicySpec", "window", humanDuration(defProperty(root, "RestartPolicySpec", "window")))
	setDef(root, "HealthCheckSpec", "interval", humanDuration(defProperty(root, "HealthCheckSpec", "interval")))
	setDef(root, "HealthCheckSpec", "timeout", humanDuration(defProperty(root, "HealthCheckSpec", "timeout")))
	describeAuthoringFields(root)
}

func describeAuthoringFields(root schema) {
	describe(root, []string{"properties", "name"}, "Job identifier, unique within its namespace.")
	describe(root, []string{"properties", "namespace"}, "Namespace that owns the job and all runtime allocations created for it.")
	describe(root, []string{"properties", "task_groups"}, "One or more placement, scaling, update, restart, and draining units.")

	describeDef(root, "TaskGroupSpec", "name", "Task-group identifier, unique within this job.")
	describeDef(root, "TaskGroupSpec", "count", "Desired number of allocations for this task group.")
	describeDef(root, "TaskGroupSpec", "runtime", "Optional OCI runtime override. Omit to use the node default; supported explicit values are runc and runsc.")
	describeDef(root, "TaskGroupSpec", "tasks", "Containers colocated in every allocation of this group; they share placement, scaling, update, restart, and drain lifecycle.")
	describeDef(root, "TaskGroupSpec", "labels", "Discovery and routing metadata attached to allocations from this group.")
	describeDef(root, "TaskGroupSpec", "api_access", "Optional least-privilege Trellis API credential request for every task in this group.")
	describeDef(root, "TaskGroupSpec", "constraints", "Exact node attribute or label matches required for placement.")
	describeDef(root, "TaskGroupSpec", "restart", "Retry policy for task failures inside an allocation.")
	describeDef(root, "TaskGroupSpec", "update", "How allocations from an older job revision are replaced.")

	describeDef(root, "APIAccessSpec", "scope", "Where the injected workload credential may operate: only this namespace or the whole cluster.")
	describeDef(root, "APIAccessSpec", "access", "Whether the injected credential is read-only or may perform ordinary writes within its scope.")
	describeDef(root, "ConstraintSpec", "attribute", "Node attribute or label key to match, such as arch, os, or a custom label.")
	describeDef(root, "ConstraintSpec", "value", "Exact value the selected node must report for this attribute.")
	describeDef(root, "RestartPolicySpec", "max_restarts", "Maximum failures allowed inside the restart window. Zero disables retries.")
	describeDef(root, "RestartPolicySpec", "window", "Time window used to count restart attempts.")
	describeDef(root, "UpdateSpec", "strategy", "Replacement strategy. Omit for recreate; rolling starts healthy replacement capacity incrementally.")
	describeDef(root, "UpdateSpec", "max_parallel", "Maximum not-yet-healthy rolling replacements in flight. Zero uses Trellis's effective default of one.")

	describeDef(root, "TaskSpec", "name", "Task identifier, unique within this task group.")
	describeDef(root, "TaskSpec", "image", "Pullable OCI image reference. Pin a version or digest for reproducible deployments.")
	describeDef(root, "TaskSpec", "env", "Literal environment variables. Keep credentials in Trellis secrets instead of manifest text.")
	describeDef(root, "TaskSpec", "networking", "Network attachment and, for host mode, direct node-port reservations.")
	describeDef(root, "TaskSpec", "volumes", "Namespace-scoped named volumes. First placement registers a volume to one node; later allocations using the same namespace/name are scheduled there.")
	describeDef(root, "TaskSpec", "resources", "CPU and memory requested from the scheduler for each task instance.")
	describeDef(root, "TaskSpec", "health_check", "Optional HTTP, TCP, or script readiness/health observation. A running task without one is considered healthy.")
	describeDef(root, "TaskSpec", "secrets", "Stored namespace secrets delivered to the task as environment variables or files.")

	describeDef(root, "TaskNetworkingSpec", "mode", "Network attachment: isolated, host, or the private Trellis namespace network. Omission means isolated.")
	describeDef(root, "TaskNetworkingSpec", "ports", "Direct host-port reservations. Valid only with mode: host; Trellis does not perform NAT or port translation.")
	describeDef(root, "PortSpec", "port", "Node port Trellis reserves and the process must bind directly when using host networking.")
	describeDef(root, "ResourcesSpec", "cpu", "CPU request in millicores; 1000 represents one CPU core.")
	describeDef(root, "ResourcesSpec", "memory", "Memory request as bytes or a readable decimal/binary size such as 500MB or 256MiB.")

	describeDef(root, "HealthCheckSpec", "type", "Health-check implementation: http, tcp, or script.")
	describeDef(root, "HealthCheckSpec", "port", "Port checked by HTTP or TCP health checks.")
	describeDef(root, "HealthCheckSpec", "path", "HTTP request path; ignored by TCP and script checks.")
	describeDef(root, "HealthCheckSpec", "command", "Command argv executed for a script health check.")
	describeDef(root, "HealthCheckSpec", "interval", "Delay between health checks. Omit to use the Trellis default.")
	describeDef(root, "HealthCheckSpec", "timeout", "Maximum duration of one health check. Omit to use the Trellis default.")
	describeDef(root, "HealthCheckSpec", "threshold", "Consecutive failed checks required before unhealthy. Zero uses the Trellis default.")

	describeDef(root, "SecretRefSpec", "name", "Name of a stored secret in this job's namespace.")
	describeDef(root, "SecretRefSpec", "target", "Delivery mechanism: env or file.")
	describeDef(root, "SecretRefSpec", "env", "Environment variable name used by an env target.")
	describeDef(root, "SecretRefSpec", "path", "Destination path below /run/trellis-secrets/ used by a file target.")
	describeDef(root, "SecretRefSpec", "mode", "File mode for a file target: 0400 or 0600 (or decimal 256/384). Zero uses the default.")

	describeDef(root, "VolumeSpec", "name", "Stable namespace-scoped volume identity used for placement.")
	describeDef(root, "VolumeSpec", "host_path", "Node-side backing directory. @/ is resolved below Trellis's namespace volume root; absolute paths are used verbatim and are not namespace-isolated on disk.")
	describeDef(root, "VolumeSpec", "container_path", "Absolute mount path inside the container.")
	describeDef(root, "VolumeSpec", "read_only", "Mount this volume read-only when true.")
}

func humanDuration(base schema) schema {
	return schema{
		"description": "A Go-style duration such as 500ms, 10s, 1m30s, or a canonical nanosecond count.",
		"oneOf": []schema{
			cloneSchema(base),
			{"type": "string", "pattern": `^(?:0|(?:[0-9]+(?:\.[0-9]+)?(?:ns|us|µs|μs|ms|s|m|h))+)$`},
		},
	}
}

func humanByteSize(base schema) schema {
	return schema{
		"description": "A byte count or human-readable decimal/binary size such as 64MB or 64MiB.",
		"oneOf": []schema{
			cloneSchema(base),
			{"type": "string", "pattern": `^[0-9]+(?:\.[0-9]+)?\s*(?:[Bb]|[KkMmGgTt][Bb]?|[KkMmGgTt]i[Bb]?)?$`},
		},
	}
}

func removeEmptyEnum(value schema) {
	values, ok := value["enum"].([]string)
	if !ok {
		// Cloning through encoding/json converts []string into []any.
		generic, ok := value["enum"].([]any)
		if !ok {
			return
		}
		filtered := generic[:0]
		for _, item := range generic {
			if item != "" {
				filtered = append(filtered, item)
			}
		}
		value["enum"] = filtered
		return
	}
	filtered := values[:0]
	for _, item := range values {
		if item != "" {
			filtered = append(filtered, item)
		}
	}
	value["enum"] = filtered
}

func identifier() schema {
	return schema{"type": "string", "pattern": `^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`}
}

func def(root schema, name string) schema {
	return root["$defs"].(schema)[name].(schema)
}

func defProperty(root schema, name, property string) schema {
	return def(root, name)["properties"].(schema)[property].(schema)
}

func patchDef(root schema, name, property string, values schema) {
	patch(def(root, name), []string{"properties", property}, values)
}

func setDef(root schema, name, property string, value schema) {
	set(def(root, name), []string{"properties", property}, value)
}

func describeDef(root schema, name, property, description string) {
	describe(def(root, name), []string{"properties", property}, description)
}

func describe(root schema, path []string, description string) {
	patch(root, path, schema{"description": description})
}

func set(root schema, path []string, value schema) {
	current := root
	for _, key := range path[:len(path)-1] {
		current = current[key].(schema)
	}
	current[path[len(path)-1]] = value
}

func patch(root schema, path []string, values schema) {
	current := root
	for _, key := range path {
		current = current[key].(schema)
	}
	for key, value := range values {
		current[key] = value
	}
}

func cloneSchema(in schema) schema {
	out := schema{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func clone(in schema) (schema, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out schema
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func marshal(value schema) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}
