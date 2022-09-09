package rink

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsistentHashRole(t *testing.T) {
	testCases := []struct {
		name      string
		role      string
		roleCount int32

		expRank int32
	}{
		{name: "zero role count returns invalid role",
			expRank: -1},
		{name: "empty role is zero",
			roleCount: 1,
			expRank:   0,
		},
		{name: "'test' role is 1 when size is 10",
			role:      "test",
			roleCount: 10,
			expRank:   1,
		},
		{name: "'test' role is 1 when size is reduced to 5",
			role:      "test",
			roleCount: 5,
			expRank:   1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rank := ConsistentHashRole(tc.role, tc.roleCount)
			assert.Equal(t, tc.expRank, rank)
		})
	}
}

func TestConsistentHash_EvenDistribution(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	randString := func() string {
		alphabet := "abcdefghijklmnopqrstuvwxyz"
		var b strings.Builder
		for i := 0; i < 20; i++ {
			_ = b.WriteByte(alphabet[r.Intn(len(alphabet))])
		}
		return b.String()
	}

	roleCount := 100_000
	roles := make(map[string]bool, roleCount)

	for i := 0; i < 100_000; i++ {
		roles[randString()] = true
	}
	require.Len(t, roles, roleCount)

	rankCount := int32(20)
	roleRanks := make(map[int32]int)
	for role := range roles {
		rr := ConsistentHashRole(role, rankCount)
		roleRanks[rr] += 1
	}

	exp := float64(roleCount) / float64(rankCount)
	// We assert that all ranks should receive 95-105% of the role share
	fiveCent := exp * 0.05

	for rank, count := range roleRanks {
		//diff := math.Abs(float64(count) - exp)
		assert.InDelta(t, exp, count, fiveCent,
			"rank %d has %d of %d roles", rank, count, roleCount,
		)
	}
}
