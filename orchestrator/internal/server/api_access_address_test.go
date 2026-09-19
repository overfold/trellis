package server

import "testing"

func TestWorkloadAPIAddressUsesStableTrellisName(t *testing.T) {
	got, err := workloadAPIAddress("node-a.example:8128")
	if err != nil {
		t.Fatal(err)
	}
	if got != "trellis:8128" {
		t.Fatalf("workload API address = %q, want trellis:8128", got)
	}
}

func TestWorkloadAPIAddressRejectsMissingPort(t *testing.T) {
	if _, err := workloadAPIAddress("node-a.example"); err == nil {
		t.Fatal("expected advertise address without a port to be rejected")
	}
}
