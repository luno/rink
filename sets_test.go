package rink

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntersect(t *testing.T) {
	s1 := map[int]bool{1: true, 2: false, 3: true}
	s2 := map[int]bool{2: true, 3: true, 4: false}

	i := Intersect(s1, s2)
	sort.Ints(i)
	assert.Equal(t, []int{2, 3}, i)
}

func TestDifference(t *testing.T) {
	s1 := map[string]int{"alice": 1, "bob": 12}
	s2 := map[string]bool{"bob": true, "carol": true, "dave": false}

	d := Difference(s1, s2)
	assert.Equal(t, []string{"alice"}, d)
}
