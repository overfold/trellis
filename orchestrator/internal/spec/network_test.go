package spec

import (
	"slices"
	"testing"
)

func TestGroupRequiredCapabilities(t *testing.T) {
	group := &TaskGroupSpec{Runtime: RuntimeRunsc, Tasks: []TaskSpec{{Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard}}}}
	if got, want := GroupRequiredCapabilities(group), []NodeCapability{CapabilityRunsc}; !slices.Equal(got, want) {
		t.Fatalf("capabilities = %v, want %v", got, want)
	}
	group.Runtime = RuntimeRunc
	if got := GroupRequiredCapabilities(group); len(got) != 0 {
		t.Fatalf("namespace networking requires capabilities %v; every node provides it", got)
	}
}

func TestNetworkingValidationMatrix(t *testing.T) {
	for _, test := range []struct {
		name  string
		mode  TaskNetworkMode
		ports []PortSpec
		path  string // empty when the task is valid
	}{
		{name: "none without ports", mode: TaskNetworkNone},
		{name: "none with port", mode: TaskNetworkNone, ports: []PortSpec{{Port: 8080}}, path: "networking.ports"},
		{name: "none with host port", mode: TaskNetworkNone, ports: []PortSpec{{Port: 8080, HostPort: 80}}, path: "networking.ports"},
		{name: "namespace without ports", mode: TaskNetworkWireGuard},
		{name: "namespace with port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080}}},
		{name: "namespace with host port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080, HostPort: 80}}},
		{name: "namespace host port out of range", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080, HostPort: 70000}}, path: "networking.ports[0].host_port"},
		{name: "namespace negative host port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080, HostPort: -1}}, path: "networking.ports[0].host_port"},
		{name: "namespace port out of range", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 0, HostPort: 80}}, path: "networking.ports[0].port"},
		{name: "namespace duplicate port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080, HostPort: 80}, {Port: 8080, HostPort: 81}}, path: "networking.ports[1].port"},
		{name: "namespace duplicate host port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 8080, HostPort: 80}, {Port: 8081, HostPort: 80}}, path: "networking.ports[1].host_port"},
		{name: "namespace host port matches another port", mode: TaskNetworkWireGuard, ports: []PortSpec{{Port: 80}, {Port: 8080, HostPort: 80}}, path: "networking.ports[1].host_port"},
		{name: "omitted mode with host port", ports: []PortSpec{{Port: 8080, HostPort: 80}}},
		{name: "host without ports", mode: TaskNetworkHost},
		{name: "host with port", mode: TaskNetworkHost, ports: []PortSpec{{Port: 8080}}},
		{name: "host with host port", mode: TaskNetworkHost, ports: []PortSpec{{Port: 8080, HostPort: 80}}, path: "networking.ports[0].host_port"},
		{name: "host with equal host port", mode: TaskNetworkHost, ports: []PortSpec{{Port: 8080, HostPort: 8080}}, path: "networking.ports[0].host_port"},
		{name: "isolated is not a mode", mode: "isolated", path: "networking.mode"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			job.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: test.mode, Ports: test.ports}
			err := Validate(job)
			if test.path == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want valid", err)
				}
				return
			}
			issues, ok := err.(ValidationErrors)
			want := "task_groups[api].tasks[server]." + test.path
			if !ok || len(issues) != 1 || issues[0].Path != want {
				t.Fatalf("Validate() = %#v, want one issue at %s", err, want)
			}
		})
	}
}

func TestNetworkingNodePortsAreUniqueInGroup(t *testing.T) {
	for _, test := range []struct {
		name  string
		tasks []TaskSpec
		path  string
	}{
		{
			name: "namespace host port and host port",
			tasks: []TaskSpec{
				{Name: "app", Image: "example/app:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard, Ports: []PortSpec{{Port: 8080, HostPort: 80}}}},
				{Name: "agent", Image: "example/agent:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 80}}}},
			},
			path: "task_groups[api].tasks[agent].networking.ports[0].port",
		},
		{
			name: "namespace default host ports",
			tasks: []TaskSpec{
				{Name: "app", Image: "example/app:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard, Ports: []PortSpec{{Port: 8080}}}},
				{Name: "sidecar", Image: "example/sidecar:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard, Ports: []PortSpec{{Port: 8080}}}},
			},
			path: "task_groups[api].tasks[sidecar].networking.ports[0].port",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			job.TaskGroups[0].Tasks = test.tasks
			issues, ok := Validate(job).(ValidationErrors)
			if !ok || len(issues) != 1 || issues[0].Path != test.path || issues[0].Code != "duplicate" {
				t.Fatalf("Validate() = %#v, want duplicate at %s", issues, test.path)
			}
		})
	}

	// Separate network namespaces may listen on the same port when they
	// publish it on different node ports.
	job := validJob()
	job.TaskGroups[0].Tasks = []TaskSpec{
		{Name: "app", Image: "example/app:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard, Ports: []PortSpec{{Port: 8080, HostPort: 80}}}},
		{Name: "admin", Image: "example/admin:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard, Ports: []PortSpec{{Port: 8080, HostPort: 81}}}},
	}
	if err := Validate(job); err != nil {
		t.Fatalf("same listen port in separate task namespaces rejected: %v", err)
	}
}

func TestCanonicalizeResolvesNetworking(t *testing.T) {
	job := validJob()
	job.TaskGroups[0].Tasks = []TaskSpec{
		{Name: "omitted", Image: "example/app:1"},
		{Name: "published", Image: "example/app:1", Networking: &TaskNetworkingSpec{Ports: []PortSpec{{Port: 8080}, {Port: 9090, HostPort: 90}}}},
		{Name: "host", Image: "example/app:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8081}}}},
		{Name: "none", Image: "example/app:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkNone}},
	}
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatalf("Canonicalize() = %v", err)
	}
	tasks := job.TaskGroups[0].Tasks
	if tasks[0].Networking == nil || tasks[0].Networking.Mode != TaskNetworkWireGuard || len(tasks[0].Networking.Ports) != 0 {
		t.Fatalf("omitted networking = %#v, want namespace", tasks[0].Networking)
	}
	if got, want := tasks[1].Networking.Ports, []PortSpec{{Port: 8080, HostPort: 8080}, {Port: 9090, HostPort: 90}}; tasks[1].Networking.Mode != TaskNetworkWireGuard || !slices.Equal(got, want) {
		t.Fatalf("published networking = %#v, want namespace with ports %v", tasks[1].Networking, want)
	}
	if got, want := tasks[2].Networking.Ports, []PortSpec{{Port: 8081}}; !slices.Equal(got, want) {
		t.Fatalf("host ports = %v, want %v without host_port", got, want)
	}
	if tasks[3].Networking.Mode != TaskNetworkNone {
		t.Fatalf("none networking = %#v", tasks[3].Networking)
	}
	if err := ValidateCanonical(job); err != nil {
		t.Fatalf("ValidateCanonical() = %v", err)
	}

	tasks[1].Networking.Ports[0].HostPort = 0
	issues, ok := ValidateCanonical(job).(ValidationErrors)
	if !ok || len(issues) != 1 || issues[0].Path != "task_groups[api].tasks[published].networking.ports[0].host_port" || issues[0].Code != "not_canonical" {
		t.Fatalf("ValidateCanonical() = %#v, want unresolved host_port", issues)
	}
}

func TestPortSpecNodePort(t *testing.T) {
	if got := (PortSpec{Port: 8080, HostPort: 80}).NodePort(); got != 80 {
		t.Fatalf("published NodePort() = %d, want 80", got)
	}
	if got := (PortSpec{Port: 8080}).NodePort(); got != 8080 {
		t.Fatalf("host NodePort() = %d, want 8080", got)
	}
}
