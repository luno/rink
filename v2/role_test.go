package rink

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

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
	t     *testing.T
	roles map[string]*testRole
}

func delegateForTesting(t *testing.T, roles ...*testRole) *testDelegate {
	rm := make(map[string]*testRole)
	for _, r := range roles {
		rm[r.Role()] = r
	}
	return &testDelegate{t: t, roles: rm}
}

func (t testDelegate) Create(role string) (RoleContext, error) {
	r, ok := t.roles[role]
	if !ok {
		panic("cannot create role")
	}
	return r, nil
}

func (t testDelegate) Assign(rank Rank, role string) bool {
	if !rank.HaveRank {
		return false
	}
	_, ok := t.roles[role]
	return ok
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
	d := delegateForTesting(t)
	r := NewRoles(d, RolesOptions{})

	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})
	_, ok := r.Get("test")
	assert.False(t, ok)
}

func TestRoles_MultipleAwait(t *testing.T) {
	d := delegateForTesting(t,
		&testRole{S: "role-0"}, &testRole{S: "role-1"},
		&testRole{S: "role-2"}, &testRole{S: "role-3"},
		&testRole{S: "role-4"},
	)
	r := NewRoles(d, RolesOptions{})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(role string) {
			defer wg.Done()
			r.AwaitRole(role)
		}("role-" + strconv.Itoa(i))
	}

	time.Sleep(time.Second)
	// r should get all the roles
	r.updateRank(Rank{MyRank: 0, HaveRank: true, Size: 1})

	// Will hang if we don't get roles
	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 10*time.Second, time.Millisecond)
}
