package spec

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := map[string]ByteSize{
		"64MiB":   64 << 20,
		"512Mi":   512 << 20,
		"1GiB":    1 << 30,
		"100MB":   100_000_000,
		"1048576": 1 << 20,
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := ParseByteSize(input)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("ParseByteSize(%q) = %d, want %d", input, got, want)
			}
		})
	}
}

func TestParseYAMLHumanMemory(t *testing.T) {
	raw := []byte("namespace: default\nname: web\ntask_groups:\n  - name: web\n    count: 1\n    tasks:\n      - name: app\n        image: example/app:1\n        resources:\n          cpu: 100\n          memory: 64MiB\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(job); err != nil {
		t.Fatal(err)
	}
	if got := job.TaskGroups[0].Tasks[0].Resources.Memory; got != 64<<20 {
		t.Fatalf("memory = %d, want %d", got, 64<<20)
	}
}
