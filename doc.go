// Package rink provides distributed role scheduling using etcd.
//
// A scheduler instance allows arbitrary roles to be assumed by only a single member of a cluster of distributed processes.
// A cluster is maintained as a "distributed sticky ranked ring" using etcd which assigns a rank (order) to each member.
// Roles are consistently hashed to the mth (of n) member.
//
// Etcd's concurrency.Session is used to identify members and act as liveliness proxy.
//
// Etcd's concurrency.Election is used to elect a leader to maintain the cluster and broadcast state updates to the members.
//
// Etcd's concurrency.Mutex is used to lock roles to prevent overlapping role assignment.
//
// When members join or leave the cluster, the size (n) changes which results in roles being rebalanced.
//
// Stickiness is provided by a configurable rebalance delay, allowing joining members to wait before triggering a rebalance of roles.
// If another member leaves while a member is waiting, its rank will be transferred immediately without affecting other members.
// If a member leaves and there are no waiting members, a rebalance is triggered affecting all instances.
package rink
