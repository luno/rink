package rink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRebalance(t *testing.T) {
	testCases := []struct {
		name string

		old     state
		members map[string]time.Time

		expState state
	}{
		{name: "scale down",
			old: state{"alice": 0, "bob": 1, "carol": 2},
			members: map[string]time.Time{
				"alice": time.Unix(0, 0),
				"bob":   time.Unix(0, 0),
			},
			expState: state{"alice": 0, "bob": 1},
		},
		{name: "scale down many",
			old: state{"alice": 0, "bob": 1, "carol": 2, "dave": 3},
			members: map[string]time.Time{
				"alice": time.Unix(0, 0),
				"dave":  time.Unix(0, 0),
			},
			expState: state{"alice": 0, "dave": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := rebalance(tc.old, tc.members)
			assert.Equal(t, tc.expState, s)
		})
	}
}

func TestMaybePromote(t *testing.T) {
	testCases := []struct {
		name string
		s    state
		mem  map[string]time.Time

		expState   state
		expUpdated bool
	}{
		{name: "no changes",
			s: state{"alice": 0, "bob": 1},
			mem: map[string]time.Time{
				"alice": time.Unix(100, 0),
				"bob":   time.Unix(100, 0),
			},
			expState:   map[string]int{"alice": 0, "bob": 1},
			expUpdated: false,
		},
		{name: "new node",
			s: state{"alice": 0},
			mem: map[string]time.Time{
				"alice": time.Unix(100, 0),
				"bob":   time.Unix(100, 0),
			},
			expState:   map[string]int{"alice": 0},
			expUpdated: false,
		},
		{name: "new node replaces old node",
			s: state{"alice": 0, "bob": 1},
			mem: map[string]time.Time{
				"alice": time.Unix(100, 0),
				"carol": time.Unix(100, 0),
			},
			expState:   map[string]int{"alice": 0, "carol": 1},
			expUpdated: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			upd := maybePromote(tc.s, tc.mem)
			assert.Equal(t, tc.expUpdated, upd)
			assert.Equal(t, tc.s, tc.expState)
		})
	}
}

func TestNextRebalance(t *testing.T) {
	testCases := []struct {
		name    string
		s       state
		members map[string]time.Time
		at      time.Time

		expDelay     time.Duration
		expRebalance bool
	}{
		{name: "rebalance new node",
			s: state{"alice": 0, "bob": 1},
			members: map[string]time.Time{
				"alice": time.Unix(80, 0),
				"bob":   time.Unix(80, 0),
				"carol": time.Unix(100, 0),
			},
			at:           time.Unix(99, 0),
			expDelay:     time.Second,
			expRebalance: true,
		},
		{name: "remove node",
			s: state{"alice": 0, "bob": 1},
			members: map[string]time.Time{
				"alice": time.Unix(80, 0),
			},
			at:           time.Unix(99, 0),
			expDelay:     0,
			expRebalance: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			delay, bal := nextRebalance(tc.s, tc.members, tc.at)
			assert.Equal(t, tc.expDelay, delay)
			assert.Equal(t, tc.expRebalance, bal)
		})
	}
}
