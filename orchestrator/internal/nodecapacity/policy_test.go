package nodecapacity

import "testing"

func TestDefaultReserveBounds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cpu        int
		memory     int64
		wantCPU    int
		wantMemory int64
	}{
		{"negative", -100, -1024, 0, 0},
		{"zero", 0, 0, 0, 0},
		{"half capacity", 199, (512 << 20) - 2, 99, (256 << 20) - 1},
		{"minimum", 1980, (5 << 30) - 20, 100, 256 << 20},
		{"above minimum", 2020, (5 << 30) + 20, 101, (256 << 20) + 1},
		{"below maximum", 19980, (40 << 30) - 20, 999, (2 << 30) - 1},
		{"maximum", 20020, (40 << 30) + 20, 1000, 2 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cpu, memory := defaultReserve(tc.cpu, tc.memory)
			if cpu != tc.wantCPU || memory != tc.wantMemory {
				t.Fatalf("reserve = %dm/%d, want %dm/%d", cpu, memory, tc.wantCPU, tc.wantMemory)
			}
		})
	}
}

func TestResolveUsesSensibleDefaults(t *testing.T) {
	t.Cleanup(func() { _ = ConfigureReserve(nil, nil) })
	if err := ConfigureReserve(nil, nil); err != nil {
		t.Fatal(err)
	}

	cpu, memory, err := Resolve(8000, 32<<30)
	if err != nil {
		t.Fatal(err)
	}
	if cpu != 7600 {
		t.Fatalf("allocatable CPU = %d, want 7600", cpu)
	}
	if memory != (32<<30)-(32<<30)/20 {
		t.Fatalf("allocatable memory = %d, want %d", memory, (32<<30)-(32<<30)/20)
	}
}

func TestResolveUsesPerResourceOverrides(t *testing.T) {
	reservedCPU := 500
	reservedMemory := int64(1 << 30)
	t.Cleanup(func() { _ = ConfigureReserve(nil, nil) })
	if err := ConfigureReserve(&reservedCPU, &reservedMemory); err != nil {
		t.Fatal(err)
	}

	cpu, memory, err := Resolve(8000, 32<<30)
	if err != nil {
		t.Fatal(err)
	}
	if cpu != 7500 || memory != 31<<30 {
		t.Fatalf("allocatable = %dm/%d, want 7500m/%d", cpu, memory, int64(31<<30))
	}
}

func TestResolveRejectsReserveLargerThanCapacity(t *testing.T) {
	reservedCPU := 2000
	t.Cleanup(func() { _ = ConfigureReserve(nil, nil) })
	if err := ConfigureReserve(&reservedCPU, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Resolve(1000, 1<<30); err == nil {
		t.Fatal("expected reserve larger than capacity to fail")
	}
}
