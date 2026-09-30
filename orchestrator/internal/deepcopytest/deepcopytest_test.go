package deepcopytest

import (
	"reflect"
	"testing"
)

type sample struct {
	Name    string
	Labels  map[string]string
	Items   []int
	Pointer *int
	private []string
}

func TestFillSetsEveryField(t *testing.T) {
	var value sample
	Fill(t, &value)
	fields := reflect.ValueOf(value)
	for i := 0; i < fields.NumField(); i++ {
		if fields.Field(i).IsZero() {
			t.Errorf("Fill left %s zero", fields.Type().Field(i).Name)
		}
	}
}

func TestDisjointReportsSharedMemory(t *testing.T) {
	var original sample
	Fill(t, &original)
	shallow := original
	shared := disjoint(reflect.ValueOf(original), reflect.ValueOf(shallow), "sample")
	want := []string{"sample.Labels", "sample.Items", "sample.Pointer", "sample.private"}
	if !reflect.DeepEqual(shared, want) {
		t.Fatalf("shared = %v, want %v", shared, want)
	}
	deep := sample{Name: original.Name, Labels: map[string]string{}, Items: []int{1}, Pointer: new(int), private: []string{"x"}}
	if shared := disjoint(reflect.ValueOf(original), reflect.ValueOf(deep), "sample"); len(shared) != 0 {
		t.Fatalf("deep copy reported shared memory: %v", shared)
	}
}
