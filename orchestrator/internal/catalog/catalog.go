// Package catalog maintains the set of discoverable service instances.
package catalog

import (
	"sync"

	"github.com/clofour/trellis/internal/api"
)

// ServiceInstance describes one discoverable allocation endpoint.
type ServiceInstance struct {
	ID      string
	Job     string
	Group   string
	Address string
	Ports   []api.PortMapping
	Labels  map[string]string
}

// ServiceCatalog stores service instances by namespace.
type ServiceCatalog struct {
	mu       sync.RWMutex
	services map[string][]ServiceInstance // keyed by namespace
}

// New creates an empty service catalog.
func New() *ServiceCatalog {
	return &ServiceCatalog{
		services: make(map[string][]ServiceInstance),
	}
}

// Update replaces all service instances in a namespace.
func (c *ServiceCatalog) Update(namespace string, instances []ServiceInstance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(instances) == 0 {
		delete(c.services, namespace)
	} else {
		c.services[namespace] = instances
	}
}

// Replace atomically replaces the complete renewable catalog snapshot.
func (c *ServiceCatalog) Replace(services map[string][]ServiceInstance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.services = make(map[string][]ServiceInstance, len(services))
	for namespace, instances := range services {
		if len(instances) != 0 {
			c.services[namespace] = append([]ServiceInstance(nil), instances...)
		}
	}
}

// Lookup returns instances for a job in a namespace.
func (c *ServiceCatalog) Lookup(namespace, jobName string) []ServiceInstance {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []ServiceInstance
	for _, inst := range c.services[namespace] {
		if inst.Job == jobName {
			result = append(result, inst)
		}
	}
	return result
}

// ListFilter restricts service catalog results by job or label.
type ListFilter struct {
	Job   string
	Label string // "key:value" format
}

// List returns internal discovery records. Filtering is intentionally kept
// independent of any public "service" resource so it can be reused by DNS and
// other discovery implementations.
func (c *ServiceCatalog) List(namespace string, filter *ListFilter) []api.ServiceEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []api.ServiceEntry
	for ns, instances := range c.services {
		if namespace != "" && ns != namespace {
			continue
		}
		for _, inst := range instances {
			if filter != nil && filter.Job != "" && inst.Job != filter.Job {
				continue
			}
			if filter != nil && filter.Label != "" && !matchLabel(inst.Labels, filter.Label) {
				continue
			}
			result = append(result, api.ServiceEntry{
				ID:        inst.ID,
				Job:       inst.Job,
				Group:     inst.Group,
				Namespace: ns,
				Labels:    inst.Labels,
				Address:   inst.Address,
				Ports:     inst.Ports,
				Status:    "healthy",
			})
		}
	}
	return result
}

func matchLabel(labels map[string]string, filter string) bool {
	for i := range filter {
		if filter[i] == ':' {
			key, value := filter[:i], filter[i+1:]
			return labels[key] == value
		}
	}
	_, ok := labels[filter]
	return ok
}
