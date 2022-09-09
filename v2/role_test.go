package rink

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRole struct {
	S      string
	Locked bool
}

func (t testRole) Role() string             { return t.S }
func (t testRole) Context() context.Context { return context.Background() }
func (t *testRole) Lock() error             { t.Locked = true; return nil }
func (t *testRole) Unlock() error           { t.Locked = false; return nil }

type testDelegate struct {
	t    *testing.T
	role *testRole
}

func delegateForTesting(t *testing.T, r *testRole) *testDelegate {
	return &testDelegate{t: t, role: r}
}

func (t testDelegate) Create(role string) (RoleContext, error) {
	if t.role != nil {
		assert.Equal(t.t, t.role.S, role)
		return t.role, nil
	}
	panic("cannot create role")
}

func (t testDelegate) Assign(rank Rank, role string) bool {
	if !rank.HaveRank {
		return false
	}
	return t.role != nil && t.role.S == role
}

func TestRoles_GetRole(t *testing.T) {
	d := delegateForTesting(t, &testRole{S: "test"})

	r := NewRoles(d, RolesOptions{})
	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, ok := r.Get("test")

	assert.NotNil(t, ctx)
	assert.True(t, ok)
}

func TestRoles_AwaitRole(t *testing.T) {
	d := delegateForTesting(t, &testRole{S: "test"})
	r := NewRoles(d, RolesOptions{})

	done := make(chan struct{})
	go func() {
		ctx := r.AwaitRole("test")
		assert.NotNil(t, ctx)
		close(done)
	}()

	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})

	_, open := <-done
	assert.False(t, open)
}

func TestRoles_UpdateRankLosesOldRoles(t *testing.T) {
	role := &testRole{S: "test"}
	d := delegateForTesting(t, role)
	r := NewRoles(d, RolesOptions{})

	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})
	_, ok := r.Get("test")
	assert.True(t, ok)

	assert.True(t, role.Locked)
	r.updateRank(Rank{})
	assert.False(t, role.Locked)
}

func TestRoles_UnlockAllLosesOldRoles(t *testing.T) {
	role := &testRole{S: "test"}
	d := delegateForTesting(t, role)
	r := NewRoles(d, RolesOptions{})

	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})
	_, ok := r.Get("test")
	assert.True(t, ok)

	assert.True(t, role.Locked)
	r.unlockAll()
	assert.False(t, role.Locked)
}

func TestRoles_DontAssigned(t *testing.T) {
	d := delegateForTesting(t, nil)
	r := NewRoles(d, RolesOptions{})

	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})
	_, ok := r.Get("test")
	assert.False(t, ok)
}
