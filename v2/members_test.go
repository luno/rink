package rink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ts(i int64) time.Time { return time.Unix(i, 0) }

func TestGetMemberChanges(t *testing.T) {
	testCases := []struct {
		name string

		members       map[string]time.Time
		last          ranks
		now           time.Time
		newMemberWait time.Duration

		expChanges memberChanges
	}{
		{name: "empty stays empty"},
		{name: "old members stayed",
			members: map[string]time.Time{"alice": ts(101)},
			last:    ranks{"alice": 0},
			now:     ts(200),
			expChanges: memberChanges{
				Remained: []string{"alice"},
			},
		},
		{name: "new member added",
			members: map[string]time.Time{"alice": ts(100)},
			now:     ts(200),
			expChanges: memberChanges{
				Added: []string{"alice"},
			},
		},
		{name: "new member waits",
			members:       map[string]time.Time{"alice": ts(200), "bob": ts(201)},
			last:          map[string]int32{"alice": 0},
			now:           ts(201),
			newMemberWait: 10 * time.Second,
			expChanges: memberChanges{
				Remained: []string{"alice"},
				Waiting:  []string{"bob"},
			},
		},
		{name: "new member replaces missing member",
			members: map[string]time.Time{"bob": ts(1000)},
			last:    map[string]int32{"alice": 0},
			now:     ts(200),
			expChanges: memberChanges{
				Replaced: map[string]string{"alice": "bob"},
			},
		},
		{name: "bad last state ignored",
			members: map[string]time.Time{"alice": ts(100)},
			last:    ranks{"alice": 100},
			now:     ts(200),
			expChanges: memberChanges{
				Remained: []string{"alice"},
			},
		},
		{name: "new cluster add members regardless of wait",
			members:       map[string]time.Time{"alice": ts(100), "bob": ts(101)},
			now:           ts(100),
			newMemberWait: time.Minute,
			expChanges: memberChanges{
				Added: []string{"alice", "bob"},
			},
		},
		{name: "new members, in order",
			members: map[string]time.Time{"alice": ts(102), "bob": ts(101)},
			now:     ts(200),
			expChanges: memberChanges{
				Added: []string{"bob", "alice"},
			},
		},
		{name: "new members get added after delay expired",
			members:       map[string]time.Time{"alice": ts(100), "bob": ts(200)},
			last:          map[string]int32{"alice": 0},
			now:           ts(201),
			newMemberWait: time.Second,
			expChanges: memberChanges{
				Remained: []string{"alice"},
				Added:    []string{"bob"},
			},
		},
		{name: "old members removed",
			members: map[string]time.Time{"bob": ts(101)},
			last:    ranks{"alice": 0, "bob": 1},
			now:     ts(200),
			expChanges: memberChanges{
				Remained: []string{"bob"},
				Removed:  []string{"alice"},
			},
		},
		{name: "old members replaced by new",
			members: map[string]time.Time{"bob": ts(101), "carol": ts(102)},
			last:    ranks{"alice": 0, "bob": 1},
			now:     ts(200),
			expChanges: memberChanges{
				Remained: []string{"bob"},
				Replaced: map[string]string{"alice": "carol"},
			},
		},
		{name: "full shuffle",
			members: map[string]time.Time{"bob": ts(101), "carol": ts(102), "dave": ts(103)},
			last:    ranks{"alice": 0, "bob": 1},
			now:     ts(200),
			expChanges: memberChanges{
				Added:    []string{"dave"},
				Remained: []string{"bob"},
				Replaced: map[string]string{"alice": "carol"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			changes := getMemberChanges(tc.members, tc.last, tc.now, tc.newMemberWait)
			assert.Equal(t, tc.expChanges, changes)

			sum := len(changes.Remained) +
				len(changes.Added) +
				len(changes.Replaced) +
				len(changes.Waiting)
			assert.Equal(t, len(tc.members), sum)
		})
	}
}

func TestGetNewRanks(t *testing.T) {
	testCases := []struct {
		name    string
		last    ranks
		changes memberChanges

		expRanks ranks
	}{
		{name: "no changes to empty"},
		{name: "no changes to existing",
			last:     ranks{"alice": 0},
			changes:  memberChanges{Remained: []string{"alice"}},
			expRanks: ranks{"alice": 0},
		},
		{name: "empty changes results in empty ranks",
			last: ranks{"alice": 0},
		},
		{name: "add new member",
			changes:  memberChanges{Added: []string{"alice"}},
			expRanks: ranks{"alice": 0},
		},
		{name: "add new member with existing",
			changes: memberChanges{
				Added:    []string{"alice"},
				Remained: []string{"bob"},
			},
			last:     ranks{"bob": 0},
			expRanks: ranks{"bob": 0, "alice": 1},
		},
		{name: "replace existing member",
			changes: memberChanges{
				Replaced: map[string]string{"alice": "bob"},
			},
			last:     ranks{"alice": 0},
			expRanks: ranks{"bob": 0},
		},
		{name: "lots of changes",
			changes: memberChanges{
				Remained: []string{"alice"},
				Removed:  []string{"bob"},
				Replaced: map[string]string{"carol": "dave"},
				Added:    []string{"earl"},
			},
			last: ranks{
				"alice": 0,
				"bob":   1,
				"carol": 2,
			},
			expRanks: ranks{
				"alice": 0,
				"earl":  1,
				"dave":  2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := getNewRanks(tc.last, tc.changes)
			assert.Equal(t, tc.expRanks, r)
		})
	}
}
