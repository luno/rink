package rink

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/dgryski/go-jump"
)

// ConsistentHashRole allocates roles to ranks (somewhat) evenly.
// We ensure that if the roleCount is decreased, then no roles with
// a rank lower than roleCount are reassigned.
func ConsistentHashRole(role string, roleCount int32) int32 {
	// Convert role string to uint64 hash key
	h := fnv.New64a()
	_, _ = h.Write([]byte(role))
	b := h.Sum(nil)
	key := binary.BigEndian.Uint64(b[len(b)-8:])

	// Get the role rank (hash the key to a bucket)
	return jump.Hash(key, int(roleCount))
}
