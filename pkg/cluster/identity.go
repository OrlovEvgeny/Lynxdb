// Package cluster implements the distributed clustering foundation for LynxDB.
// It provides node identity, sharding primitives, gRPC communication,
// Raft consensus, shard map management, leases, and failure detection.
package cluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// NodeID is a unique identifier for a cluster node.
// Re-exported from sharding to provide a convenient top-level accessor
// while avoiding import cycles (sharding is a leaf package).
type NodeID = sharding.NodeID

// Role represents a function performed by a cluster node.
type Role int

const (
	// RoleMeta manages cluster metadata via Raft consensus.
	RoleMeta Role = iota
	// RoleIngest handles event ingestion and direct-to-part writes.
	RoleIngest
	// RoleQuery handles query execution (scatter-gather).
	RoleQuery
)

// String returns the human-readable name of the role.
func (r Role) String() string {
	switch r {
	case RoleMeta:
		return "meta"
	case RoleIngest:
		return "ingest"
	case RoleQuery:
		return "query"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}

// RoleSet is an ordered set of roles assigned to a node.
type RoleSet []Role

// Has reports whether the set contains the given role.
func (rs RoleSet) Has(r Role) bool {
	for _, existing := range rs {
		if existing == r {
			return true
		}
	}

	return false
}

// String returns a comma-separated list of role names.
func (rs RoleSet) String() string {
	names := make([]string, len(rs))
	for i, r := range rs {
		names[i] = r.String()
	}

	return strings.Join(names, ",")
}

// ParseRoles converts string role names to a RoleSet.
// Valid names: "meta", "ingest", "query" (case-insensitive).
func ParseRoles(ss []string) (RoleSet, error) {
	if len(ss) == 0 {
		return nil, fmt.Errorf("cluster.ParseRoles: no roles specified")
	}

	seen := make(map[Role]bool, len(ss))
	roles := make(RoleSet, 0, len(ss))

	for _, s := range ss {
		var r Role

		switch strings.ToLower(strings.TrimSpace(s)) {
		case "meta":
			r = RoleMeta
		case "ingest":
			r = RoleIngest
		case "query":
			r = RoleQuery
		default:
			return nil, fmt.Errorf("cluster.ParseRoles: unknown role %q (valid: meta, ingest, query)", s)
		}

		if seen[r] {
			continue // deduplicate
		}

		seen[r] = true
		roles = append(roles, r)
	}

	return roles, nil
}

// NodeInfo describes a cluster node's identity and metadata.
type NodeInfo struct {
	ID        NodeID    `json:"id" msgpack:"id"`
	Addr      string    `json:"addr" msgpack:"addr"`           // HTTP listen address
	GRPCAddr  string    `json:"grpc_addr" msgpack:"grpc_addr"` // gRPC listen address
	Roles     RoleSet   `json:"roles" msgpack:"roles"`
	StartedAt time.Time `json:"started_at" msgpack:"started_at"`
	Version   string    `json:"version" msgpack:"version"`
}
