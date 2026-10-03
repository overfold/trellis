package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
)

var errNetworkSubnetExhausted = errors.New("namespace network subnet pool is exhausted")

// maxNetworkSubnets bounds subnet indexes so each maps to a distinct
// WireGuard link address in 169.254.1.0-169.254.254.255, avoiding the first
// and last link-local /24 that RFC 3927 reserves.
const maxNetworkSubnets = 254 * 256

// NetworkSubnetRegistration durably assigns one namespace on one node a /24
// from the cluster WireGuard pool. Index selects both the subnet and the
// node's WireGuard link address for that namespace, so neither can collide
// with another registration.
type NetworkSubnetRegistration struct {
	Namespace string    `json:"namespace"`
	NodeID    uuid.UUID `json:"node_id"`
	Index     int       `json:"index"`
}

type networkSubnetKey struct {
	namespace string
	node      uuid.UUID
}

func (r *NetworkSubnetRegistration) key() networkSubnetKey {
	return networkSubnetKey{namespace: r.Namespace, node: r.NodeID}
}

func (r *NetworkSubnetRegistration) valid() bool {
	return r != nil && r.Namespace != "" && r.NodeID != uuid.Nil && r.Index >= 0
}

// listNetworkSubnetRegistrations loads the durable (namespace, node) ->
// subnet index map.
func (s *StateController) listNetworkSubnetRegistrations(ctx context.Context) (map[networkSubnetKey]int, error) {
	prefix := fmt.Sprintf("%s/%s/network-subnet-registrations/", trellisNamespace, s.cluster)
	values, err := listValues[NetworkSubnetRegistration](ctx, s.store, prefix)
	if err != nil {
		return nil, err
	}
	result := make(map[networkSubnetKey]int, len(values))
	for _, registration := range values {
		if !registration.valid() {
			return nil, fmt.Errorf("invalid persisted network subnet registration %#v", registration)
		}
		result[registration.key()] = registration.Index
	}
	return result, nil
}

func (s *StateController) networkSubnetRegistrationKey(namespace string, node uuid.UUID) string {
	return fmt.Sprintf(
		"%s/%s/network-subnet-registrations/%s/%s",
		trellisNamespace,
		s.cluster,
		url.QueryEscape(namespace),
		node,
	)
}

// networkSubnetCapacity is the number of distinct subnet indexes a pool
// supports.
func networkSubnetCapacity(pool netip.Prefix) int {
	if !pool.IsValid() || !pool.Addr().Is4() || pool.Bits() > 24 {
		return 0
	}
	return min(1<<(24-pool.Bits()), maxNetworkSubnets)
}

// networkSubnet returns the /24 selected by index within pool.
func networkSubnet(pool netip.Prefix, index int) netip.Prefix {
	base := pool.Masked().Addr().As4()
	value := uint32(base[0])<<24 | uint32(base[1])<<16 | uint32(base[2])<<8 | uint32(base[3])
	// Planned indexes are bounded by networkSubnetCapacity and maxNetworkSubnets.
	value += uint32(index) << 8 //nolint:gosec
	var address [4]byte
	binary.BigEndian.PutUint32(address[:], value)
	return netip.PrefixFrom(netip.AddrFrom4(address), 24)
}

// networkLinkAddress returns the WireGuard interface address selected by
// index. Indexes are unique cluster-wide, so link addresses are too.
func networkLinkAddress(index int) string {
	return fmt.Sprintf("169.254.%d.%d/32", 1+index/256, index%256)
}

// networkSubnetPlan is the outcome of planning namespace subnet registrations.
type networkSubnetPlan struct {
	// Subnets is the complete registration map after the plan commits.
	Subnets map[networkSubnetKey]int
	// Ready holds namespaces with a subnet on every node.
	Ready map[string]bool
	// Registrations and Deletions are the durable changes to commit.
	Registrations []*NetworkSubnetRegistration
	Deletions     []*NetworkSubnetRegistration
}

// planNetworkSubnetRegistrations assigns each namespace that needs a network
// path a subnet index on every node, keeping existing registrations and
// releasing those whose namespace or node is gone. New indexes are the lowest
// free ones, assigned in namespace then node ID order, so the plan depends only
// on its inputs. A namespace receives new registrations only when every
// missing node can be addressed; otherwise it is left unready and the returned
// error explains the exhaustion. The inputs are not modified.
func planNetworkSubnetRegistrations(pool netip.Prefix, current map[networkSubnetKey]int, namespaces []string, nodes []uuid.UUID) (*networkSubnetPlan, error) {
	wantedNamespaces := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == "" {
			return nil, fmt.Errorf("network namespace is required")
		}
		wantedNamespaces[namespace] = true
	}
	wantedNodes := make(map[uuid.UUID]bool, len(nodes))
	for _, node := range nodes {
		wantedNodes[node] = true
	}
	capacity := networkSubnetCapacity(pool)
	plan := &networkSubnetPlan{
		Subnets: make(map[networkSubnetKey]int, len(current)),
		Ready:   make(map[string]bool, len(wantedNamespaces)),
	}
	used := make(map[int]networkSubnetKey, len(current))
	for key, index := range current {
		if !wantedNamespaces[key.namespace] || !wantedNodes[key.node] {
			plan.Deletions = append(plan.Deletions, &NetworkSubnetRegistration{Namespace: key.namespace, NodeID: key.node, Index: index})
			continue
		}
		if index < 0 || index >= capacity {
			return nil, fmt.Errorf("namespace %q on node %s uses network subnet index %d outside pool %s", key.namespace, key.node, index, pool)
		}
		if previous, exists := used[index]; exists {
			return nil, fmt.Errorf("network subnet index %d is registered to both namespace %q on node %s and namespace %q on node %s", index, previous.namespace, previous.node, key.namespace, key.node)
		}
		used[index] = key
		plan.Subnets[key] = index
	}
	sort.Slice(plan.Deletions, func(i, j int) bool {
		a, b := plan.Deletions[i], plan.Deletions[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.NodeID.String() < b.NodeID.String()
	})

	sortedNamespaces := make([]string, 0, len(wantedNamespaces))
	for namespace := range wantedNamespaces {
		sortedNamespaces = append(sortedNamespaces, namespace)
	}
	sort.Strings(sortedNamespaces)
	sortedNodes := make([]uuid.UUID, 0, len(wantedNodes))
	for node := range wantedNodes {
		sortedNodes = append(sortedNodes, node)
	}
	slices.SortFunc(sortedNodes, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })

	next := 0
	var exhausted []string
	for _, namespace := range sortedNamespaces {
		var missing []uuid.UUID
		for _, node := range sortedNodes {
			if _, exists := plan.Subnets[networkSubnetKey{namespace: namespace, node: node}]; !exists {
				missing = append(missing, node)
			}
		}
		if len(missing) > capacity-len(used) {
			exhausted = append(exhausted, namespace)
			continue
		}
		for _, node := range missing {
			for {
				if _, occupied := used[next]; !occupied {
					break
				}
				next++
			}
			key := networkSubnetKey{namespace: namespace, node: node}
			used[next] = key
			plan.Subnets[key] = next
			plan.Registrations = append(plan.Registrations, &NetworkSubnetRegistration{Namespace: namespace, NodeID: node, Index: next})
		}
		plan.Ready[namespace] = true
	}
	if len(exhausted) > 0 {
		return plan, fmt.Errorf("%w: pool %s holds %d node subnets, namespaces %s cannot be addressed on every node; use a larger wireguard_pool or fewer namespace-networked namespaces", errNetworkSubnetExhausted, pool, capacity, strings.Join(exhausted, ", "))
	}
	return plan, nil
}
