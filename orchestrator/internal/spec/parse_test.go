package spec

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

const numericManifest = `name: web
namespace: default
task_groups:
  - name: web
    count: 1
    update: {max_parallel: 1}
    restart: {max_restarts: 3, window: 600000000000}
    tasks:
      - name: app
        image: app:1
        resources: {cpu: 100, memory: 134217728}
        networking: {ports: [{port: 80, host_port: 8080}]}
        health_check: {type: tcp, port: 80, interval: 10000000000, timeout: 5000000000, threshold: 3}
        secrets: [{name: key, target: file, path: /run/trellis-secrets/key, mode: 256}]
`

func TestYAMLRejectsNullsRecursively(t *testing.T) {
	for _, value := range []string{"null", "Null", "NULL", "~", "", "!!null null"} {
		for _, field := range []string{"namespace: default", "count: 1", "cpu: 100", "window: 600000000000", "image: app:1"} {
			name, _, _ := strings.Cut(field, ":")
			raw := strings.Replace(numericManifest, field, name+": "+value, 1)
			if _, err := ParseYAML([]byte(raw)); err == nil || !strings.Contains(err.Error(), "YAML null") {
				t.Fatalf("%s = %q: %v", name, value, err)
			}
		}
	}
	for _, raw := range []string{
		"null\n", "task_groups: [null]\n", "task_groups: [{tasks: [null]}]\n",
		"task_groups: [{labels: {tier: null}}]\n",
		"task_groups: [{tasks: [{env: {TOKEN: null}}]}]\n",
		"task_groups: [{tasks: [{resources: null}]}]\n",
		"task_groups: [{tasks: [{command: [echo, null]}]}]\n",
		"task_groups: [{labels: {first: &empty null, second: *empty}}]\n",
		"task_groups: [{labels: {<<: &base {tier: null}, tier: web}}]\n",
	} {
		if _, err := ParseYAML([]byte(raw)); err == nil || !strings.Contains(err.Error(), "YAML null") {
			t.Fatalf("null accepted in %s: %v", raw, err)
		}
	}
	job, err := ParseYAML([]byte(numericManifest))
	if err != nil {
		t.Fatal(err)
	}
	if job.TaskGroups[0].Tasks[0].Env != nil {
		t.Fatal("omitted env changed")
	}
	raw := strings.Replace(numericManifest, "image: app:1", "image: app:1\n        env: {EMPTY: \"\", TEXT: \"null\", FIRST: &literal value, SECOND: *literal}", 1)
	job, err = ParseYAML([]byte(raw))
	if err != nil || job.TaskGroups[0].Tasks[0].Env["TEXT"] != "null" || job.TaskGroups[0].Tasks[0].Env["SECOND"] != "value" {
		t.Fatalf("literal strings/aliases changed: %+v, %v", job, err)
	}
}

func TestYAMLRejectsLossyNumericCoercion(t *testing.T) {
	for _, field := range []string{"count: 1", "max_parallel: 1", "max_restarts: 3", "window: 600000000000", "cpu: 100", "memory: 134217728", "port: 80", "host_port: 8080", "interval: 10000000000", "timeout: 5000000000", "threshold: 3", "mode: 256"} {
		name, _, _ := strings.Cut(field, ":")
		for _, value := range []string{"true", "false", "0.5", "-0.5", "18446744073709551616"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				raw := strings.Replace(numericManifest, field, name+": "+value, 1)
				if _, err := ParseYAML([]byte(raw)); err == nil {
					t.Fatal("invalid numeric value accepted")
				}
			})
		}
	}
	for _, value := range []string{"-1", "4294967296"} {
		if _, err := ParseYAML([]byte(strings.Replace(numericManifest, "mode: 256", "mode: "+value, 1))); err == nil {
			t.Fatalf("uint32 mode overflow %s accepted", value)
		}
	}
}

func TestYAMLExactlyOneDocument(t *testing.T) {
	for _, suffix := range []string{"---\n", "---\nname: hidden\n", "---\n[malformed\n"} {
		if _, err := ParseYAML([]byte(numericManifest + suffix)); err == nil {
			t.Fatalf("trailing document %q accepted", suffix)
		}
	}
	if _, err := ParseYAML([]byte(numericManifest + "...\n")); err != nil {
		t.Fatalf("single document with end marker: %v", err)
	}
}

func TestYAMLHumanQuantitiesMatchJSON(t *testing.T) {
	raw := strings.ReplaceAll(numericManifest, "134217728", "128MiB")
	raw = strings.ReplaceAll(raw, "600000000000", "10m")
	raw = strings.ReplaceAll(raw, "10000000000", "10s")
	raw = strings.ReplaceAll(raw, "5000000000", "5s")
	job, err := ParseYAML([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(job)
	var restored JobSpec
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if TaskGroupContentHash(&job.TaskGroups[0]) != TaskGroupContentHash(&restored.TaskGroups[0]) {
		t.Fatal("JSON round trip changed execution content")
	}
	if restored.TaskGroups[0].Tasks[0].Resources.Memory != 128<<20 || restored.TaskGroups[0].Restart.Window != 600000000000 {
		t.Fatal("human quantities did not convert exactly")
	}
}

func TestByteSizeInt64Boundaries(t *testing.T) {
	for _, input := range []string{"9007199254740993", "9223372036854775807"} {
		got, err := ParseByteSize(input)
		if err != nil || fmt.Sprint(int64(got)) != input {
			t.Fatalf("ParseByteSize(%s) = %d, %v", input, got, err)
		}
	}
	for _, input := range []string{"9223372036854775808", "9223372036854775807.1", "9223372036854775807.5", "99999999999999999999TiB"} {
		if _, err := ParseByteSize(input); err == nil {
			t.Fatalf("overflow %s accepted", input)
		}
	}
	job, err := ParseYAML([]byte(strings.Replace(numericManifest, "memory: 134217728", "memory: 9223372036854775807", 1)))
	if err != nil || job.TaskGroups[0].Tasks[0].Resources.Memory != math.MaxInt64 {
		t.Fatalf("YAML int64 boundary = %+v, %v", job, err)
	}
}
