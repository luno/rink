package rink_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/luno/rink"
)

const (
	keyPrefix = "testrink"
	joinFast  = time.Millisecond
	joinSlow  = time.Hour
)

func TestJoinS1(t *testing.T) {
	c := newTestCluster()

	s, m := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m, 0, 1)

	assertHasRole(t, m, "any string")
	assertHasRole(t, m, "0")
	assertHasRole(t, m, time.Now().String())
	assertHasRole(t, m, "leader")

	require.NoError(t, s.Close())

	awaitInfo(t, m, 0, 0)

	assertNoRole(t, m, "any string")
	assertNoRole(t, m, "0")
	assertNoRole(t, m, time.Now().String())
	assertNoRole(t, m, "leader")
}

func TestJoinS2AfterS1(t *testing.T) {
	c := newTestCluster()

	s1, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	s2, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m2, 1, 2)

	require.NoError(t, s1.Close())
	awaitInfo(t, m1, 0, 0)
	awaitInfo(t, m2, 0, 1) // Rebalanced to rank 0

	require.NoError(t, s2.Close())
	awaitInfo(t, m1, 0, 0)
	awaitInfo(t, m2, 0, 0)
}

func TestImmediatePromote(t *testing.T) {
	c := newTestCluster()

	s1, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	s2, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m2, 1, 2)

	// m1 has "bestrole"
	assertHasRole(t, m1, "bestrole")
	ctx1 := m1.Await("bestrole")
	require.NoError(t, ctx1.Err())

	// m2 does not have "bestrole", but has "myrole"
	assertNoRole(t, m2, "bestrole")
	assertHasRole(t, m2, "myrole")
	ctx2 := m2.Await("myrole")
	require.NoError(t, ctx2.Err())

	s3, m3 := joinNew(t, c, "m3", joinSlow)
	awaitInfo(t, m3, -1, 2) // Assert it is waiting

	// Stopping 2, should immediately promote 3
	require.NoError(t, s2.Close())
	awaitInfo(t, m1, 0, 2)
	awaitInfo(t, m2, 0, 0)
	awaitInfo(t, m3, 1, 2)

	// m1 unaffected, context still open.
	require.NoError(t, ctx1.Err())
	// m2 stopped, context closed
	require.Error(t, ctx2.Err())

	// m3 now has "myrole"
	assertHasRole(t, m3, "myrole")
	ctx3 := m3.Await("myrole")
	require.NoError(t, ctx3.Err())

	// Stopping 3, will rebalance.
	require.NoError(t, s3.Close())
	awaitInfo(t, m1, 0, 1)
	awaitInfo(t, m2, 0, 0)
	awaitInfo(t, m3, 0, 0)

	// m1 not affected if secure.
	require.NoError(t, ctx1.Err())
	// m1 now has "myrole"
	assertHasRole(t, m1, "myrole")

	// Stop m1
	require.NoError(t, s1.Close())
	awaitInfo(t, m1, 0, 0)

	require.Error(t, ctx1.Err())
}

func TestPromoteToRank0(t *testing.T) {
	c := newTestCluster()

	_, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	_, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m1, 0, 2)
	_, m3 := joinNew(t, c, "m3", joinSlow)
	awaitInfo(t, m3, -1, 2) // Assert it is waiting

	// Stopping m1, should immediately promote m3
	require.NoError(t, m1.Close())
	awaitInfo(t, m3, 0, 2)
	awaitInfo(t, m2, 1, 2)
}

func TestConcurrentAwaits(t *testing.T) {
	c := newTestCluster()

	_, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	_, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m1, 0, 2)
	awaitInfo(t, m2, 1, 2)

	assertHasRole(t, m2, "leader")

	// Await leader on m1 should block
	ctxChan := make(chan context.Context, 2)
	await := func() {
		ctxChan <- m1.Await("leader")
	}
	go await()
	go await()
	require.Len(t, ctxChan, 0)

	// Closing m2 moves "leader" to m1.
	jtest.RequireNil(t, m2.Close())

	// Assert both goroutines got the same context and that it is active.
	ctx1 := <-ctxChan
	ctx2 := <-ctxChan
	require.Equal(t, ctx1, ctx2)
	require.Nil(t, ctx1.Err())
	require.Nil(t, ctx2.Err())

	// Adding m3 moves "leader" to m3.
	_, m3 := joinNew(t, c, "m3", joinFast)
	awaitInfo(t, m1, 0, 2)
	awaitInfo(t, m3, 1, 2)
	assertHasRole(t, m3, "leader")

	// Assert context is closed.
	require.Error(t, ctx1.Err())
	require.Error(t, ctx2.Err())
}

func TestAutoDelayedJoin(t *testing.T) {
	c := newTestCluster()
	_, m1 := joinNew(t, c, "m1", time.Millisecond*100) // 100 ms is just (sometimes) long enough to require internal timer.
	awaitInfo(t, m1, 0, 1)
}

func TestRoleRebalance(t *testing.T) {
	c := newTestCluster()

	_, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	assertRoles(t, m1, 0, 1, 2, 3, 4) // m1 has all roles

	_, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m1, 0, 2)
	awaitInfo(t, m2, 1, 2)      // m2 joins at tail, rank 1
	assertRoles(t, m1, 0, 3, 4) // m1 keeps 0,3,4
	assertRoles(t, m2, 1, 2)    // m2 gets 1,2

	_, m3 := joinNew(t, c, "m3", joinFast)
	awaitInfo(t, m1, 0, 3)
	awaitInfo(t, m2, 1, 3)
	awaitInfo(t, m3, 2, 3)         // m3 joins tail, rank 2
	assertRoles(t, m1, 0)          // m1 keeps 0
	assertRoles(t, m2)             // m2 keeps nothing
	assertRoles(t, m3, 1, 2, 3, 4) // m3 gets 1,2,3,4

	_, m4 := joinNew(t, c, "m4", joinFast)
	awaitInfo(t, m1, 0, 4)
	awaitInfo(t, m2, 1, 4)
	awaitInfo(t, m3, 2, 4)
	awaitInfo(t, m4, 3, 4)      // m4 joins tail, rank 3
	assertRoles(t, m1, 0)       // m1 keeps 0
	assertRoles(t, m2)          // m2 still has nothing
	assertRoles(t, m3, 1, 2, 4) // m3 keeps 1,2,4
	assertRoles(t, m4, 3)       // m4 gets 3

	_, m5 := joinNew(t, c, "m5", joinFast)
	awaitInfo(t, m1, 0, 5)
	awaitInfo(t, m2, 1, 5)
	awaitInfo(t, m3, 2, 5)
	awaitInfo(t, m4, 3, 5)
	awaitInfo(t, m5, 4, 5)      // m5 joins tail, rank 4
	assertRoles(t, m1, 0)       // m1 still keeps 0
	assertRoles(t, m2)          // m2 still has nothing
	assertRoles(t, m3, 1, 2, 4) // m3 keeps 1,2,4
	assertRoles(t, m4, 3)       // m4 keeps 3
	assertRoles(t, m5)          // m5 gets nothing

	require.NoError(t, m3.Close()) // Stop m3, will be replaced by tail member m5
	awaitInfo(t, m1, 0, 4)
	awaitInfo(t, m2, 1, 4)
	awaitInfo(t, m3, 0, 0)
	awaitInfo(t, m4, 3, 4)
	awaitInfo(t, m5, 2, 4)      // m5 jumps to rank 2 others unaffected
	assertRoles(t, m1, 0)       // m1 still keeps 0
	assertRoles(t, m2)          // m2 still has nothing
	assertRoles(t, m3)          // m3 is stopped, has nothing
	assertRoles(t, m4, 3)       // m4 keeps 3
	assertRoles(t, m5, 1, 2, 4) // m5 gets 1,2,4 from m3

	require.NoError(t, m4.Close()) // Stop 4 (tail member, so other ranks unaffected)
	awaitInfo(t, m1, 0, 3)
	awaitInfo(t, m2, 1, 3)
	awaitInfo(t, m3, 0, 0)
	awaitInfo(t, m4, 0, 0)
	awaitInfo(t, m5, 2, 3)         // m5 remains at 2
	assertRoles(t, m1, 0)          // m1 still keeps 0
	assertRoles(t, m2)             // m2 still has nothing
	assertRoles(t, m3)             // m3 is stopped, has nothing
	assertRoles(t, m4)             // m4 is stopped, has nothing
	assertRoles(t, m5, 1, 2, 3, 4) // m5 keeps 1,2,4 and gets 3 from m4
}

func TestModRoles(t *testing.T) {
	c := newTestCluster()

	_, m1 := joinNew(t, c, "m1", joinFast)
	awaitInfo(t, m1, 0, 1)
	assertModRoles(t, m1, 0, 1, 2, 3, 4) // m1 has all roles

	_, m2 := joinNew(t, c, "m2", joinFast)
	awaitInfo(t, m1, 0, 2)
	awaitInfo(t, m2, 1, 2)
	assertModRoles(t, m1, 0, 2, 4) // m1 keeps 0,2,4
	assertModRoles(t, m2, 1, 3)    // m2 gets 1, 3

	_, m3 := joinNew(t, c, "m3", joinFast)
	awaitInfo(t, m1, 0, 3)
	awaitInfo(t, m2, 1, 3)
	awaitInfo(t, m3, 2, 3)
	assertModRoles(t, m1, 0, 3) // m1 keeps 0, gets 3
	assertModRoles(t, m2, 1, 4) // m2 keeps 1, gets 4
	assertModRoles(t, m3, 2)    // m3 gets 2
}

func TestRapidRelease(t *testing.T) {
	c := newTestCluster()

	sessions := make(map[int]*concurrency.Session)
	schedulers := make(map[int]*rink.Scheduler)

	for i := 0; i < 20; i++ {
		s, r := joinNew(t, c, fmt.Sprintf("m%d", i), joinFast)
		sessions[i] = s
		schedulers[i] = r
		awaitInfo(t, r, i, i+1)
	}
	// Roles 0-19 assigned to m0-m19

	jtest.RequireNil(t, sessions[0].Close())
	for i := 5; i < 15; i++ {
		jtest.RequireNil(t, sessions[i].Close())
	}
	ranks := make(map[int]int)
	for i := 1; i < 20; i++ {
		if i >= 5 && i < 15 {
			continue
		}
		r := awaitRank(t, schedulers[i], 9)
		ranks[r] = i
	}
	assert.Len(t, ranks, 9)
	for i := 0; i < len(ranks); i++ {
		_, ok := ranks[i]
		assert.True(t, ok)
	}
}

// newTestCluster returns a unique cluster name to avoid etcd key clashes with previous aborted tests.
func newTestCluster() string {
	return fmt.Sprintf("%s_%d", keyPrefix, time.Now().UnixNano())
}

func joinNew(t *testing.T, cluster string, name string, delay time.Duration) (*concurrency.Session, *rink.Scheduler) {
	s := setupSession(t)

	l := new(nolog)

	ctx := log.ContextWith(context.Background(), j.KS("name", name))

	r := rink.New(s, cluster,
		rink.WithRebalanceDelay(delay),
		rink.WithName(name),
		rink.WithLogger(l),
		rink.WithContext(ctx))

	t.Cleanup(func() {
		require.NoError(t, r.Close())

		for _, e := range l.Get() {
			require.False(t, e.isErr, "Got error log: %v", e.err)
		}
	})

	return s, r
}

func assertRoles(t *testing.T, s *rink.Scheduler, roleIndexes ...int) {
	t.Helper()
	innerAssertRoles(t, s, false, roleIndexes...)
}

func assertModRoles(t *testing.T, s *rink.Scheduler, roleIndexes ...int) {
	t.Helper()
	innerAssertRoles(t, s, true, roleIndexes...)
}

func innerAssertRoles(t *testing.T, s *rink.Scheduler, mod bool, roleIndexes ...int) {

	t.Helper()

	all := []string{"0", "1", "2", "3", "4"}

	expect := make(map[string]bool)
	for _, role := range all {
		expect[role] = false
	}
	for _, idx := range roleIndexes {
		role := all[idx]
		expect[role] = true
	}

	actual := make(map[string]bool)
	for i, role := range all {
		r := role
		if mod {
			r = rink.ModRole(i)
		}
		_, ok := s.Get(r)
		actual[role] = ok
	}

	require.EqualValues(t, expect, actual)
}

func assertNoRole(t *testing.T, r *rink.Scheduler, role string) {
	t.Helper()
	ctx, ok := r.Get(role)
	require.False(t, ok)
	require.Nil(t, ctx)
}

func assertHasRole(t *testing.T, r *rink.Scheduler, role string) {
	t.Helper()
	ctx, ok := r.Get(role)
	require.True(t, ok, "Doesn't have role %s", role)
	require.NoError(t, ctx.Err())
}

func awaitInfo(t *testing.T, r *rink.Scheduler, rank, size int) {
	t.Helper()

	futureSize := new(future) // For error logging
	futureRank := new(future)
	require.Eventually(t, func() bool {
		actualRank, actualSize := r.Info()
		*futureSize, *futureRank = future(actualSize), future(actualRank)
		return actualSize == size && actualRank == rank
	}, time.Second*2, time.Millisecond*10,
		"expect size %v vs actual size %s; expected rank %v vs actual rank %s",
		size, futureSize, rank, futureRank)
}

// awaitRank returns the rank of `r` once the cluster has size members
func awaitRank(t *testing.T, r *rink.Scheduler, size int) int {
	t.Helper()

	futureSize := new(future) // For error logging
	require.Eventually(t, func() bool {
		_, actualSize := r.Info()
		*futureSize = future(actualSize)
		return actualSize == size
	}, time.Second*2, time.Millisecond*10,
		"expect size %v vs actual size %s",
		size, futureSize)
	rank, s := r.Info()
	assert.Equal(t, size, s)
	return rank
}

type future int

func (f *future) String() string {
	if f == nil {
		return ""
	}
	return strconv.Itoa(int(*f))
}

type entry struct {
	isErr, isInfo, isDebug bool
	msg                    string
	err                    error
}

type nolog struct {
	mu      sync.Mutex
	entries []entry
}

func (l *nolog) Debug(ctx context.Context, msg string, opts ...jettison.Option) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Info(ctx, msg, opts...)
	l.entries = append(l.entries, entry{isDebug: true, msg: msg})
}

func (l *nolog) Info(ctx context.Context, msg string, opts ...jettison.Option) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Info(ctx, msg, opts...)
	l.entries = append(l.entries, entry{isInfo: true, msg: msg})
}

func (l *nolog) Error(ctx context.Context, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Error(ctx, err)
	l.entries = append(l.entries, entry{isErr: true, err: err})
}
func (l *nolog) Get() []entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries
}
