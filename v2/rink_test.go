package rink

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
)

func TestRink(t *testing.T) {
	cli := etcdForTesting(t)

	roles := make(map[string]bool)
	s := New(cli, "testing",
		WithClusterOptions(ClusterOptions{MemberName: "testing-pod-1"}),
		WithRolesOptions(RolesOptions{RoleNotify: func(role string, locked bool) {
			roles[role] = locked
		}}),
	)
	t.Cleanup(s.Shutdown)
	ctx := s.Roles.AwaitRole("test")
	jtest.RequireNil(t, ctx.Err())

	assert.Equal(t, map[string]bool{"test": true}, roles)
}

func TestRink_DoesntAssign(t *testing.T) {
	cli := etcdForTesting(t)

	s := New(cli, "testing",
		WithClusterOptions(ClusterOptions{MemberName: "testing-pod-1"}),
		WithAssignRoleFunc(func(role string, rankCount int32) int32 {
			return 100
		}),
	)
	t.Cleanup(s.Shutdown)

	_, ok := s.Roles.Get("test")
	assert.False(t, ok)
}

func TestRink_CancelsOnShutdown(t *testing.T) {
	cli := etcdForTesting(t)

	roles := make(map[string]bool)
	s := New(cli, "testing",
		WithClusterOptions(ClusterOptions{MemberName: "testing-pod-1"}),
		WithRolesOptions(RolesOptions{RoleNotify: func(role string, locked bool) {
			roles[role] = locked
		}}),
	)
	ctx := s.Roles.AwaitRole("test")
	assert.Equal(t, map[string]bool{"test": true}, roles)
	s.Shutdown()
	jtest.Assert(t, context.Canceled, ctx.Err())
	assert.Equal(t, map[string]bool{"test": false}, roles)
}

func TestRink_HandlesSessionClosure(t *testing.T) {
	cli := etcdForTesting(t)
	s := New(cli, "testing",
		WithClusterOptions(ClusterOptions{MemberName: "testing-pod-1"}),
	)
	t.Cleanup(s.Shutdown)

	ctx1 := s.Roles.AwaitRole("test")
	s.currentSession.Orphan()

	<-ctx1.Done()
	jtest.Require(t, context.Canceled, ctx1.Err())

	ctx2 := s.Roles.AwaitRole("test")
	jtest.RequireNil(t, ctx2.Err())
}

type logCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func makeLogger() *logCounter {
	return &logCounter{counts: make(map[string]int)}
}

func (l *logCounter) Debug(ctx context.Context, msg string, ol ...jettison.Option) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ol = append(ol, log.WithLevel(log.LevelDebug))
	log.Info(ctx, msg, ol...)
	l.counts["debug"] += 1
}
func (l *logCounter) Info(ctx context.Context, msg string, ol ...jettison.Option) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Info(ctx, msg, ol...)
	l.counts["info"] += 1
}

func (l *logCounter) Error(ctx context.Context, err error, ol ...jettison.Option) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Error(ctx, err, ol...)
	l.counts["error"] += 1
}

func (l *logCounter) Sum() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts["debug"] + l.counts["info"] + l.counts["error"]
}

func TestRinkLogs(t *testing.T) {
	logger := makeLogger()

	cli := etcdForTesting(t)
	s := New(cli, "testing", WithLogger(logger))

	ctx := s.Roles.AwaitRole("test")

	s.Shutdown()
	<-ctx.Done()

	assert.Greater(t, logger.Sum(), 0)
}

func TestRink_RecoversETCD(t *testing.T) {
	t.Skip("requires manual intervention")
	logger := makeLogger()
	cli := etcdForTesting(t)

	s := New(cli, "testing", WithLogger(logger))
	t.Cleanup(s.Shutdown)

	ctx := s.Roles.AwaitRole("test")

	t.Log("Please shut down the etcd server.")

	<-ctx.Done()
	time.Sleep(time.Second)

	t.Log("Please start the etcd server.")

	s.Roles.AwaitRole("test2")
}

func TestBuildOptions(t *testing.T) {
	l1 := noopLogger{}
	l2 := noopLogger{}
	l3 := noopLogger{}

	testCases := []struct {
		name string
		opts []Option

		expOptions options
	}{
		{name: "logger defaulted to cluster and roles",
			opts: []Option{WithLogger(l1)},
			expOptions: options{
				Log:            l1,
				ClusterOptions: ClusterOptions{Log: l1},
				RolesOptions:   RolesOptions{Log: l1},
			},
		},
		{name: "can specify separate logger for cluster and roles",
			opts: []Option{
				WithLogger(l1),
				WithClusterOptions(ClusterOptions{Log: l2}),
				WithRolesOptions(RolesOptions{Log: l3}),
			},
			expOptions: options{
				Log:            l1,
				ClusterOptions: ClusterOptions{Log: l2},
				RolesOptions:   RolesOptions{Log: l3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := buildOptions(tc.opts)

			assert.Equal(t, tc.expOptions.Log, o.Log)
			assert.Equal(t, tc.expOptions.RolesOptions.Log, o.RolesOptions.Log)
			assert.Equal(t, tc.expOptions.ClusterOptions.Log, o.ClusterOptions.Log)
			assert.Equal(t, tc.expOptions.ClusterOptions.MemberName, o.ClusterOptions.MemberName)
			assert.Equal(t, tc.expOptions.ClusterOptions.NewMemberWait, o.ClusterOptions.NewMemberWait)
		})
	}

}
