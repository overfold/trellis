package client_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/overfold/trellis/orchestrator/client"
)

// This example lists the exposed allocations of a namespace, as a proxy
// integration does to build its upstreams.
func Example() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A stand-in for the control plane.
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"id":"web-1","job":"web","group":"api","namespace":"default",
			"address":"10.0.0.7","ports":[{"host_port":31000,"container_port":8080}],
			"phase":"running","health":"healthy","generation":1,"job_revision":1,"attempt":0}]`)
	}))
	defer server.Close()

	c, err := client.New(client.Config{
		Address:   server.URL,
		Token:     "trls_example",
		Namespace: "default",
	})
	if err != nil {
		log.Fatal(err)
	}
	allocations, err := c.ListAllocations(context.Background(), client.AllocationFilter{Label: "trellis.expose"})
	if err != nil {
		log.Fatal(err)
	}
	for _, allocation := range allocations {
		for _, port := range allocation.Ports {
			fmt.Printf("%s/%s %s %s:%d\n", allocation.Job, allocation.Group, allocation.Phase, allocation.Address, port.HostPort)
		}
	}
	// Output: web/api running 10.0.0.7:31000
}
