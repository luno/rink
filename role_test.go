package rink

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func assigner(t testing.TB, roles ...string) AssignRoleFunc {
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

func randomName() string {
	return strconv.Itoa(rand.Int())
}

type testLogger struct {
	testing.TB
}

func (t testLogger) Debug(ctx context.Context, msg string, ol ...log.Option) {
	t.Log("DEBUG", msg)
	log.Debug(ctx, msg, ol...)
}

func (t testLogger) Info(ctx context.Context, msg string, ol ...log.Option) {
	t.Log("INFO", msg)
	log.Info(ctx, msg, ol...)
}

func (t testLogger) Error(ctx context.Context, err error, ol ...log.Option) {
	t.Log("ERROR", err)
	log.Error(ctx, err, ol...)
}

func RolesForTesting(t testing.TB, ns string, ro RolesOptions) (*Roles, func()) {
	if ro.Log == nil {
		ro.Log = testLogger{t}
	}
	r := NewRoles(ns, ro)

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
		jtest.RequireNil(t, sess.Close())
	}

	t.Cleanup(stop)
	return r, stop
}

func TestRoles_AwaitRole(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

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
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, _, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)

	r.updateRank(context.Background(), Rank{})

	<-ctx.Done()

	// Call update rank again to give the inner goroutine time to fully process the last updateRank
	// as the ctx here will be canceled before the mutex unlocks
	r.updateRank(context.Background(), Rank{})
}

func TestRoles_UpdateRankGainsRoles(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{})
	time.AfterFunc(time.Second, func() {
		r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})
	})

	_, _, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)
}

func TestRoles_ResetLosesOldRoles(t *testing.T) {
	r, stop := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, _, err := r.AwaitRoleContext(context.Background(), "test")
	jtest.RequireNil(t, err)

	stop()
	jtest.Require(t, context.Canceled, ctx.Err())
}

func TestRoles_DontAssign(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Assign: assigner(t),
	})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	_, _, err := r.AwaitRoleContext(ctx, "test")
	jtest.Assert(t, context.DeadlineExceeded, err)
}

func TestRoles_MultipleAwait(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(role string) {
			defer wg.Done()
			_, _, err := r.AwaitRoleContext(context.Background(), role)
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
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})
	r.updateRank(context.Background(), Rank{})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	_, _, ok = r.Get(context.Background(), "test")
	assert.True(t, ok)
}

func TestRoles_MutexAlreadyLocked(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{AwaitRetry: 100 * time.Millisecond})

	cli2 := etcdForTesting(t)
	sess2, err := concurrency.NewSession(cli2)
	jtest.RequireNil(t, err)
	mu := concurrency.NewMutex(sess2, path.Join(r.namespace, "roles", "test"))

	err = mu.Lock(context.Background())
	jtest.RequireNil(t, err)

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	go func() {
		time.Sleep(time.Second)
		err := mu.Unlock(context.Background())
		jtest.AssertNil(t, err)
	}()

	_, _, err = r.AwaitRoleContext(context.Background(), "test")
	jtest.AssertNil(t, err)
}

func TestRoles_AwaitCancel(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	t.Cleanup(cancel)

	_, _, err := r.AwaitRoleContext(ctx, "test")
	jtest.Assert(t, context.DeadlineExceeded, err)
}

func TestRoles_GetUnassigned(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{Assign: assigner(t)})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)
}

func TestRoles_NotifyCalledWhenNotAssigned(t *testing.T) {
	locks := make(map[string]bool)
	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Assign: assigner(t),
		Notify: func(role string, locked bool) {
			locks[role] = locked
		},
	})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	assert.Equal(t, map[string]bool{"test": false}, locks)
}

func TestRoles_NotifyNotCalledTwice(t *testing.T) {
	locks := make(map[string]int)
	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Assign: assigner(t),
		Notify: func(role string, locked bool) {
			locks[role]++
		},
	})

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)
	_, _, ok = r.Get(context.Background(), "test")
	assert.False(t, ok)

	assert.Equal(t, map[string]int{"test": 1}, locks)
}

func TestRoles_NotifyCalledWhenAssigned(t *testing.T) {
	locks := make(map[string]bool)

	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Notify: func(role string, locked bool) {
			locks[role] = locked
		},
	})
	r.updateRank(context.Background(), Rank{})

	_, _, ok := r.Get(context.Background(), "test")
	assert.False(t, ok)

	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok = r.Get(context.Background(), "test")
	assert.True(t, ok)

	assert.Equal(t, map[string]bool{"test": true}, locks)
}

func TestRoles_NotifyCalledOnceWhenAssigned(t *testing.T) {
	locks := make(map[string]int)
	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Notify: func(role string, locked bool) {
			locks[role]++
		},
	})
	r.updateRank(context.Background(), Rank{HaveRank: true, MyRank: 0, Size: 1})

	_, _, ok := r.Get(context.Background(), "test")
	assert.True(t, ok)

	assert.Equal(t, map[string]int{"test": 1}, locks)
}

func TestRoles_MutexKeys(t *testing.T) {
	testCases := []struct {
		name      string
		namespace string
		role      string
		expKey    string
	}{
		{name: "empty", expKey: "roles"},
		{
			name:      "namespace only",
			namespace: "test",
			expKey:    "test/roles",
		},
		{
			name:      "full key",
			namespace: "hello",
			role:      "world",
			expKey:    "hello/roles/world",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := mutexKey(tc.namespace, tc.role)
			assert.Equal(t, tc.expKey, key)
		})
	}
}

func TestRoles_ReturnsSameLegacyContext(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx1 := r.AwaitRole("leader")
	ctx2 := r.AwaitRole("leader")

	assert.Equal(t, ctx1, ctx2)
}

func TestRoles_LegacyCancel(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})
	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx1 := r.AwaitRole("leader")
	ctx2 := r.AwaitRole("leader")

	r.updateRank(context.Background(), Rank{})
	<-ctx1.Done()
	jtest.Assert(t, context.Canceled, ctx1.Err())
	jtest.Assert(t, context.Canceled, ctx2.Err())
}

func TestRoles_LegacyResumeAfterCancel(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})
	ctx1 := r.AwaitRole("leader")
	r.updateRank(context.Background(), Rank{})

	<-ctx1.Done()
	jtest.Assert(t, context.Canceled, ctx1.Err())
	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	ctx2 := r.AwaitRole("leader")
	jtest.AssertNil(t, ctx2.Err())
	jtest.Assert(t, context.Canceled, ctx1.Err())
}

func TestRoles_LegacyNotBlockedByOthers(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{
		Assign: assigner(t, "have"),
	})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	go func() {
		r.AwaitRole("have-not")
	}()

	assert.Eventually(t, func() bool {
		r.AwaitRole("have")
		return true
	}, time.Second, 100*time.Millisecond)
}

func TestRoles_MultipleWaitersOnEmpty(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	_, _, err := r.AwaitRoleContext(context.Background(), "")
	jtest.AssertNil(t, err)
	_, _, err2 := r.AwaitRoleContext(context.Background(), "")
	jtest.AssertNil(t, err2)
}

func TestRoles_ManyRolesReleasedOnClose(t *testing.T) {
	r, stop := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	var contexts []context.Context
	for i := 1; i <= 200; i++ {
		ctx, _, err := r.AwaitRoleContext(context.Background(), strconv.Itoa(i))
		jtest.RequireNil(t, err)
		contexts = append(contexts, ctx)
	}

	stop()
	for _, ctx := range contexts {
		jtest.Assert(t, context.Canceled, ctx.Err())
	}
}

func TestRoles_ManyRolesReleasedOnReRank(t *testing.T) {
	r, _ := RolesForTesting(t, randomName(), RolesOptions{})

	r.updateRank(context.Background(), Rank{MyRank: 0, HaveRank: true, Size: 1})

	var contexts []context.Context
	for i := 1; i <= 200; i++ {
		ctx, _, err := r.AwaitRoleContext(context.Background(), strconv.Itoa(i))
		jtest.RequireNil(t, err)
		contexts = append(contexts, ctx)
	}

	r.updateRank(context.Background(), Rank{})
	// A second time so that we wait for the processing goroutine to cancel all the contexts
	r.updateRank(context.Background(), Rank{})

	for _, ctx := range contexts {
		jtest.Assert(t, context.Canceled, ctx.Err())
	}
}

func TestRoles_RoleClashError(t *testing.T) {
	ctx := context.Background()
	cli := etcdForTesting(t)

	sess1, err := concurrency.NewSession(cli)
	jtest.RequireNil(t, err)
	sess2, err := concurrency.NewSession(cli)
	jtest.RequireNil(t, err)

	t.Cleanup(func() {
		jtest.RequireNil(t, sess1.Close())
	})
	t.Cleanup(func() {
		jtest.RequireNil(t, sess2.Close())
	})

	ns := randomName()
	_, err = createLock(ctx, sess1, ns, "clash", 0)
	jtest.RequireNil(t, err)

	_, err = createLock(ctx, sess2, ns, "clash", 0)
	jtest.Assert(t, concurrency.ErrLocked, err)

	var je *errors.JettisonError
	require.True(t, errors.As(err, &je))
	_, ok := je.GetKey("held_by_lease")
	require.True(t, ok)
}
