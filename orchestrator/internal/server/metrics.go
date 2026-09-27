package server

import (
	"time"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds instrumentation counters owned by the Server.
type Metrics struct {
	ReconcileDuration prometheus.Histogram
}

// RegisterMetrics registers all Trellis Prometheus metrics against reg and
// wires them to s. Call once at process start, before the leadership loop.
func RegisterMetrics(s *Server, reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ReconcileDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trellis_reconcile_duration_seconds",
			Help:    "Duration of each server reconcile loop run in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.ReconcileDuration)
	reg.MustRegister(&metricsCollector{server: s})
	s.metrics = m
	return m
}

// metricsCollector is a Prometheus Collector that snapshots server state on
// each scrape. All point-in-time gauges live here; the reconcile histogram
// lives on Metrics so Reconcile() can observe it.
type metricsCollector struct {
	server *Server

	allocationsDesc      *prometheus.Desc
	nodesDesc            *prometheus.Desc
	jobsDesc             *prometheus.Desc
	nodeCPUCapacityDesc  *prometheus.Desc
	nodeMemCapacityDesc  *prometheus.Desc
	nodeCPUAllocatedDesc *prometheus.Desc
	nodeMemAllocatedDesc *prometheus.Desc
	nodeHeartbeatAgeDesc *prometheus.Desc
}

func newDescriptors() (
	allocs, nodes, jobs, cpuCap, memCap, cpuAlloc, memAlloc, hbAge *prometheus.Desc,
) {
	allocs = prometheus.NewDesc(
		"trellis_allocations",
		"Number of allocations by namespace, job, phase, and health.",
		[]string{"namespace", "job", "phase", "health"}, nil,
	)
	nodes = prometheus.NewDesc(
		"trellis_nodes",
		"Number of nodes by status.",
		[]string{"status"}, nil,
	)
	jobs = prometheus.NewDesc(
		"trellis_jobs",
		"Number of registered jobs by namespace.",
		[]string{"namespace"}, nil,
	)
	cpuCap = prometheus.NewDesc(
		"trellis_node_cpu_capacity_millicores",
		"CPU capacity allocatable to Trellis workloads on a node in millicores.",
		[]string{"node_id"}, nil,
	)
	memCap = prometheus.NewDesc(
		"trellis_node_memory_capacity_bytes",
		"Memory capacity allocatable to Trellis workloads on a node in bytes.",
		[]string{"node_id"}, nil,
	)
	cpuAlloc = prometheus.NewDesc(
		"trellis_node_cpu_allocated_millicores",
		"CPU reserved by active allocations on a node in millicores.",
		[]string{"node_id"}, nil,
	)
	memAlloc = prometheus.NewDesc(
		"trellis_node_memory_allocated_bytes",
		"Memory reserved by active allocations on a node in bytes.",
		[]string{"node_id"}, nil,
	)
	hbAge = prometheus.NewDesc(
		"trellis_node_heartbeat_age_seconds",
		"Seconds elapsed since the node's last heartbeat.",
		[]string{"node_id"}, nil,
	)
	return
}

func (c *metricsCollector) init() {
	if c.allocationsDesc != nil {
		return
	}
	c.allocationsDesc, c.nodesDesc, c.jobsDesc,
		c.nodeCPUCapacityDesc, c.nodeMemCapacityDesc,
		c.nodeCPUAllocatedDesc, c.nodeMemAllocatedDesc,
		c.nodeHeartbeatAgeDesc = newDescriptors()
}

func (c *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	c.init()
	ch <- c.allocationsDesc
	ch <- c.nodesDesc
	ch <- c.jobsDesc
	ch <- c.nodeCPUCapacityDesc
	ch <- c.nodeMemCapacityDesc
	ch <- c.nodeCPUAllocatedDesc
	ch <- c.nodeMemAllocatedDesc
	ch <- c.nodeHeartbeatAgeDesc
}

func (c *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.init()

	s := c.server
	now := time.Now()

	s.mu.RLock()

	// --- allocation counts by (namespace, job, phase, health) ---
	type allocKey struct{ namespace, job, phase, health string }
	allocCounts := make(map[allocKey]float64)

	// --- per-node resource utilisation ---
	type nodeUtil struct {
		cpuMillicores int
		memBytes      int64
	}
	nodeUtils := make(map[string]*nodeUtil, len(s.nodes))
	for id := range s.nodes {
		nodeUtils[id.String()] = &nodeUtil{}
	}

	for _, alloc := range s.allocations {
		alloc.mu.Lock()
		key := allocKey{
			namespace: alloc.Namespace,
			job:       alloc.JobName,
			phase:     string(alloc.Phase),
			health:    string(alloc.Health),
		}
		allocCounts[key]++

		if alloc.Node != nil &&
			alloc.Phase != lifecycle.PhaseStopped &&
			alloc.Phase != lifecycle.PhaseFailed &&
			alloc.Phase != lifecycle.PhaseLost {
			nodeID := alloc.Node.ID.String()
			if u, ok := nodeUtils[nodeID]; ok {
				for i := range alloc.Tasks {
					r := alloc.Tasks[i].Resources
					if r != nil {
						u.cpuMillicores += r.CPU
						u.memBytes += int64(r.Memory)
					}
				}
			}
		}
		alloc.mu.Unlock()
	}

	// --- job counts by namespace ---
	jobsByNS := make(map[string]float64)
	for _, job := range s.jobs {
		jobsByNS[job.Spec.Namespace]++
	}

	// --- node status counts + per-node capacity / heartbeat ---
	nodeCounts := make(map[string]float64)
	for _, node := range s.nodes {
		nodeID := node.ID.String()
		nodeCounts[string(node.Status)]++

		ch <- prometheus.MustNewConstMetric(c.nodeCPUCapacityDesc, prometheus.GaugeValue, float64(node.CPUAllocatable), nodeID)
		ch <- prometheus.MustNewConstMetric(c.nodeMemCapacityDesc, prometheus.GaugeValue, float64(node.MemoryAllocatable), nodeID)

		if u := nodeUtils[nodeID]; u != nil {
			ch <- prometheus.MustNewConstMetric(c.nodeCPUAllocatedDesc, prometheus.GaugeValue, float64(u.cpuMillicores), nodeID)
			ch <- prometheus.MustNewConstMetric(c.nodeMemAllocatedDesc, prometheus.GaugeValue, float64(u.memBytes), nodeID)
		}

		if !node.LastHeartbeat.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.nodeHeartbeatAgeDesc, prometheus.GaugeValue, now.Sub(node.LastHeartbeat).Seconds(), nodeID)
		}
	}

	s.mu.RUnlock()

	for key, count := range allocCounts {
		ch <- prometheus.MustNewConstMetric(c.allocationsDesc, prometheus.GaugeValue, count, key.namespace, key.job, key.phase, key.health)
	}
	for status, count := range nodeCounts {
		ch <- prometheus.MustNewConstMetric(c.nodesDesc, prometheus.GaugeValue, count, status)
	}
	for ns, count := range jobsByNS {
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, count, ns)
	}
}
