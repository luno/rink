package rink

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func assigner(t *testing.T, roles ...string) AssignRoleFunc {
	ass := make(map[string]bool)
	for _, r := range roles {
		ass[r] = true
	}
	return func(role string, roleCount int32) int32 {
		t.Log("asked to assign role", role)
		if ass[role] {
			return 0
		}
		return -1
	}
}

func RolesForTesting(t *testing.T, ro RolesOptions) (*Roles, func()) {
	if ro.Log == nil {
		ro.Log = log.Jettison{}
	}
	r := NewRoles(strconv.Itoa(rand.Int()), ro)

	cli := etcdForTesting(t)
	sess, err := concurrency.NewSession(cli)
	jtest.RequireNil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := r.assignRoles(ctx, sess)
		jtest.Assert(t, context.Canceled, err)
	}()

	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		wg.Wait()
	}

	t.Cleanup(stop)
	return r, stop
}

func TestRoles_AwaitRole(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})

	done := make(chan context.Context)
	go func() {
		ctx := r.AwaitRole("test")
		assert.NotNil(t, ctx)
		done <- ctx
	}()

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx := <-done
	jtest.AssertNil(t, ctx.Err())
}

func TestRoles_UpdateRankLosesOldRoles(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)

	r.updateRank(context.Background(), Rank{})

	<-ctx.Done()
}

func TestRoles_UpdateRankGainsRoles(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})

	r.updateRank(context.Background(), Rank{})
	time.AfterFunc(time.Second, func() {
		r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})
	})

	_, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)
}

func TestRoles_ResetLosesOldRoles(t *testing.T) {
	r, stop := RolesForTesting(t, RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)

	stop()
	jtest.Require(t, context.Canceled, ctx.Err())
}

func TestRoles_DontAssign(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{
		Assign: assigner(t),
	})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	_, err := r.AwaitRoleContext(ctx, "test")
	jtest.Assert(t, context.DeadlineExceeded, err)
}

func TestRoles_MultipleAwait(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(role string) {
			defer wg.Done()
			_, err := r.AwaitRoleContext(context.Background(), role)
			jtest.AssertNil(t, err)
		}("role-" + strconv.Itoa(i))
	}

	time.Sleep(time.Second)
	// r should get all the roles
	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	// Will hang if we don't get roles
	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, time.Second, 100*time.Millisecond)
}

func TestRoles_Get(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})
	r.updateRank(context.Background(), Rank{})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	_, ok = r.Get(context.Background(), "test")
	assert.True(t, ok)
}

func TestRoles_MutexAlreadyLocked(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{AwaitRetry: 100 * time.Millisecond})

	cli2 := etcdForTesting(t)
	sess2, err := concurrency.NewSession(cli2)
	jtest.RequireNil(t, err)
	mu := concurrency.NewMutex(sess2, path.Join(r.namespace, "roles", "test"))

	err = mu.Lock(context.Background())
	jtest.RequireNil(t, err)

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	go func() {
		time.Sleep(time.Second)
		err := mu.Unlock(context.Background())
		jtest.AssertNil(t, err)
	}()

	_, err = r.AwaitRoleContext(context.Background(), "test")
	jtest.AssertNil(t, err)

}

func TestRoles_AwaitCancel(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	t.Cleanup(cancel)

	_, err := r.AwaitRoleContext(ctx, "test")
	jtest.Assert(t, context.DeadlineExceeded, err)
}

func TestRoles_GetUnassigned(t *testing.T) {
	r, _ := RolesForTesting(t, RolesOptions{Assign: assigner(t)})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)
}

func TestRoles_NotifyCalledWhenNotAssigned(t *testing.T) {
	locks := make(map[string]bool)
	r, _ := RolesForTesting(t, RolesOptions{
		Assign: assigner(t),
		Notify: func(role string, locked bool) {
			locks[role] = locked
		},
	})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	assert.Equal(t, map[string]bool{"test": false}, locks)
}

func TestRoles_NotifyNotCalledTwice(t *testing.T) {
	locks := make(map[string]int)
	r, _ := RolesForTesting(t, RolesOptions{
		Assign: assigner(t),
		Notify: func(role string, locked bool) {
			locks[role]++
		},
	})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)
	_, ok = r.Get(context.Background(), "test")
	assert.False(t, ok)

	assert.Equal(t, map[string]int{"test": 1}, locks)
}

func TestRoles_NotifyCalledWhenAssigned(t *testing.T) {
	locks := make(map[string]bool)

	r, _ := RolesForTesting(t, RolesOptions{
		Notify: func(role string, locked bool) {
			locks[role] = locked
		},
	})
	r.updateRank(context.Background(), Rank{})

	_, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok = r.Get(context.Background(), "test")
	assert.True(t, ok)

	assert.Equal(t, map[string]bool{"test": true}, locks)
}

func TestRoles_NotifyCalledOnceWhenAssigned(t *testing.T) {
	locks := make(map[string]int)
	r, _ := RolesForTesting(t, RolesOptions{
		Notify: func(role string, locked bool) {
			locks[role]++
		},
	})
	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, ok := r.Get(context.Background(), "test")
	assert.True(t, ok)

	assert.Equal(t, map[string]int{"test": 1}, locks)
}

func TestRoles_MutexKeys(t *testing.T) {
	testCases := []struct {
		name string
		r    Roles
		role string

		expKey string
	}{
		{name: "empty", expKey: "roles"},
		{name: "namespace only",
			r:      Roles{namespace: "test"},
			expKey: "test/roles",
		},
		{name: "full key",
			r:      Roles{namespace: "hello"},
			role:   "world",
			expKey: "hello/roles/world",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.r.mutexKey(tc.role)
			assert.Equal(t, tc.expKey, key)
		})
	}

}
