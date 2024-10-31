package rink

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func etcdEndpoints() []string {
	if v, ok := os.LookupEnv("TESTING_ETCD_ENDPOINTS"); ok {
		return strings.Split(v, ",")
	}
	return []string{"http://localhost:2380", "http://localhost:2379"}
}

type rankChangeEvent struct {
	Member string
	Time   time.Time
	State  Rank
}

type testCluster struct {
	clusterName string
	ctx         context.Context
	cancel      context.CancelFunc

	cli *clientv3.Client
	eg  *errgroup.Group

	rankLog chan rankChangeEvent
}

func etcdForTesting(t testing.TB) *clientv3.Client {
	config := clientv3.Config{
		Endpoints:            etcdEndpoints(),
		DialKeepAliveTime:    time.Second,
		DialKeepAliveTimeout: time.Second,
		DialTimeout:          time.Second,
		DialOptions:          []grpc.DialOption{grpc.WithBlock()},
		Logger:               zap.NewNop(),
	}
	cli, err := clientv3.New(config)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("etcd server not accessible")
	}
	jtest.RequireNil(t, err)
	t.Cleanup(func() {
		_ = cli.Close()
	})
	return cli
}

func newTestCluster(t testing.TB) *testCluster {
	cli := etcdForTesting(t)
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	c := &testCluster{
		clusterName: strconv.Itoa(rand.Int()),
		ctx:         ctx,
		cancel:      cancel,
		cli:         cli,
		eg:          g,
		rankLog:     make(chan rankChangeEvent, 100),
	}
	t.Cleanup(func() {
		_ = c.Close()
	})
	return c
}

func (c *testCluster) Close() error {
	c.cancel()
	return c.eg.Wait()
}

func (c *testCluster) CollectEvents(count int, timeout time.Duration) []rankChangeEvent {
	deadline := time.After(timeout)

	var ret []rankChangeEvent
	for len(ret) < count {
		select {
		case ev := <-c.rankLog:
			ret = append(ret, ev)
		case <-deadline:
			return ret
		}
	}
	return ret
}

func (c *testCluster) GoMember(name string) context.CancelFunc {
	ctx, cancel := context.WithCancel(c.ctx)
	ctx = log.ContextWith(ctx, j.KV("member", name))
	wait := make(chan struct{})

	c.eg.Go(func() error {
		defer close(wait)
		log.Info(ctx, "new member")
		sess, err := concurrency.NewSession(c.cli)
		if err != nil {
			return err
		}
		defer func() {
			_ = sess.Close()
		}()

		var haveHadRank bool
		rankHandler := func(ctx context.Context, rank Rank) {
			select {
			case c.rankLog <- rankChangeEvent{Member: name, Time: time.Now(), State: rank}:
			default:
				panic("overflow rank log buffer")
			}
			if !rank.HaveRank {
				if haveHadRank {
					log.Info(ctx, "leaving")
				} else {
					log.Info(ctx, "waiting to join")
				}
			} else {
				haveHadRank = true
				log.Info(ctx, "i'm ranked", j.MKV{"rank": rank.MyRank, "of": rank.Size})
			}
		}
		err = Run(ctx, sess, c.clusterName, rankHandler, ClusterOptions{
			MemberName:    name,
			NewMemberWait: 1 * time.Second,
			Log:           log.Jettison{},
		})
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			return nil
		}
		return err
	})
	return func() {
		cancel()
		<-wait
	}
}

func convertEventsToLookup(t *testing.T, events []rankChangeEvent) map[Rank]rankChangeEvent {
	ret := make(map[Rank]rankChangeEvent)
	for _, ev := range events {
		_, exist := ret[ev.State]
		require.False(t, exist, "duplicate event for state %+v", ev.State)

		ret[ev.State] = ev
	}
	return ret
}

func assertEvents(t *testing.T, c *testCluster, expEvents ...rankChangeEvent) {
	evs := c.CollectEvents(len(expEvents), 500*time.Millisecond)
	require.Len(t, evs, len(expEvents))

	events := convertEventsToLookup(t, evs)

	for _, expEv := range expEvents {
		ev, exists := events[expEv.State]
		require.True(t, exists,
			"no event for state %+v. got events %+v",
			expEv.State, events,
		)

		assert.Equal(t, expEv.Member, ev.Member)
		assert.Equal(t, expEv.State, ev.State)

		assert.WithinDuration(t, expEv.Time, ev.Time, 500*time.Millisecond)
	}
}

func TestETCDCloses(t *testing.T) {
	c := newTestCluster(t)
	stop := c.GoMember("test-member")
	time.Sleep(time.Second)
	stop()
	jtest.RequireNil(t, c.Close())
}

func TestValidateOptions(t *testing.T) {
	rand.Seed(0)
	testCases := []struct {
		name        string
		clusterName string
		in          ClusterOptions
		expOut      ClusterOptions
		expError    bool
	}{
		{
			name:     "empty cluster errors",
			expError: true,
		},
		{
			name:        "default",
			clusterName: "test",
			expOut: ClusterOptions{
				MemberName:      "78fc2ffac2fd9401",
				NewMemberWait:   time.Minute,
				electionKey:     "test/election",
				memberKey:       "test/members/78fc2ffac2fd9401",
				memberKeyPrefix: "test/members/",
			},
		},
		{
			name:        "member name with dashes",
			clusterName: "deploy",
			in: ClusterOptions{
				MemberName:    "pod-1",
				NewMemberWait: time.Second,
			},
			expOut: ClusterOptions{
				MemberName:      "pod-1",
				NewMemberWait:   time.Second,
				electionKey:     "deploy/election",
				memberKey:       "deploy/members/pod-1",
				memberKeyPrefix: "deploy/members/",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.in
			err := validateOptions(tc.clusterName, &out)
			if tc.expError {
				require.Error(t, err)
				return
			} else {
				jtest.RequireNil(t, err)
			}
			// Ignore logging and notify
			out.Log = nil
			out.NotifyRank = nil
			out.NotifyLeader = nil
			assert.Equal(t, tc.expOut, out)
		})
	}
}

func TestServiceDeployment(t *testing.T) {
	c := newTestCluster(t)
	start := time.Now()

	var deployOne []context.CancelFunc

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("deploy-1-member-%d", i)
		deployOne = append(deployOne, c.GoMember(name))
		// Allow time for last member to join
		time.Sleep(2 * time.Second)
	}

	expLog := []rankChangeEvent{
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: false, Size: 0}, Time: start},
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: true, Size: 1}, Time: start},
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: true, Size: 2}, Time: start.Add(3 * time.Second)},
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: true, Size: 3}, Time: start.Add(5 * time.Second)},
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: true, Size: 4}, Time: start.Add(7 * time.Second)},
		{Member: "deploy-1-member-0", State: Rank{MyRank: 0, HaveRank: true, Size: 5}, Time: start.Add(9 * time.Second)},

		{Member: "deploy-1-member-1", State: Rank{MyRank: 0, HaveRank: false, Size: 1}, Time: start.Add(2 * time.Second)},
		{Member: "deploy-1-member-1", State: Rank{MyRank: 1, HaveRank: true, Size: 2}, Time: start.Add(3 * time.Second)},
		{Member: "deploy-1-member-1", State: Rank{MyRank: 1, HaveRank: true, Size: 3}, Time: start.Add(5 * time.Second)},
		{Member: "deploy-1-member-1", State: Rank{MyRank: 1, HaveRank: true, Size: 4}, Time: start.Add(7 * time.Second)},
		{Member: "deploy-1-member-1", State: Rank{MyRank: 1, HaveRank: true, Size: 5}, Time: start.Add(9 * time.Second)},

		{Member: "deploy-1-member-2", State: Rank{MyRank: 0, HaveRank: false, Size: 2}, Time: start.Add(4 * time.Second)},
		{Member: "deploy-1-member-2", State: Rank{MyRank: 2, HaveRank: true, Size: 3}, Time: start.Add(5 * time.Second)},
		{Member: "deploy-1-member-2", State: Rank{MyRank: 2, HaveRank: true, Size: 4}, Time: start.Add(7 * time.Second)},
		{Member: "deploy-1-member-2", State: Rank{MyRank: 2, HaveRank: true, Size: 5}, Time: start.Add(9 * time.Second)},

		{Member: "deploy-1-member-3", State: Rank{MyRank: 0, HaveRank: false, Size: 3}, Time: start.Add(6 * time.Second)},
		{Member: "deploy-1-member-3", State: Rank{MyRank: 3, HaveRank: true, Size: 4}, Time: start.Add(7 * time.Second)},
		{Member: "deploy-1-member-3", State: Rank{MyRank: 3, HaveRank: true, Size: 5}, Time: start.Add(9 * time.Second)},

		{Member: "deploy-1-member-4", State: Rank{MyRank: 0, HaveRank: false, Size: 4}, Time: start.Add(8 * time.Second)},
		{Member: "deploy-1-member-4", State: Rank{MyRank: 4, HaveRank: true, Size: 5}, Time: start.Add(9 * time.Second)},
	}

	assertEvents(t, c, expLog...)

	for i, stopOne := range deployOne {
		redeploy := time.Now()

		name := fmt.Sprintf("deploy-2-member-%d", i)
		replaces := fmt.Sprintf("deploy-1-member-%d", i)

		c.GoMember(name)
		// Wait until the new "pod" is almost ready for a role
		assertEvents(t, c,
			rankChangeEvent{Member: name, State: Rank{MyRank: 0, HaveRank: false, Size: 5}, Time: redeploy},
		)

		termOld := redeploy.Add(500 * time.Millisecond)
		time.Sleep(time.Until(termOld))
		stopOne()

		expReDeploy := []rankChangeEvent{
			{Member: name, State: Rank{MyRank: int32(i), HaveRank: true, Size: 5}, Time: termOld},
			{Member: replaces, State: Rank{}, Time: termOld},
		}
		assertEvents(t, c, expReDeploy...)
	}
}

func TestSessionDuplicate(t *testing.T) {
	cli := etcdForTesting(t)

	name := "test-cluster"
	opt := ClusterOptions{MemberName: "pod-1"}
	rankNoop := func(context.Context, Rank) {}

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		sess, err := concurrency.NewSession(cli)
		jtest.RequireNil(t, err)
		defer func() {
			_ = sess.Close()
		}()
		jtest.Require(t, context.Canceled, Run(ctx1, sess, name, rankNoop, opt))
	}()

	time.Sleep(time.Second)

	ctx2 := context.Background()
	sess2, err := concurrency.NewSession(cli)
	jtest.RequireNil(t, err)
	t.Cleanup(func() {
		_ = sess2.Close()
	})
	err = Run(ctx2, sess2, name, rankNoop, opt)
	jtest.Assert(t, ErrMemberAlreadyExists, err)

	cancel1()
	<-done1

	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		sess, err := concurrency.NewSession(cli)
		jtest.RequireNil(t, err)
		defer func() {
			_ = sess.Close()
		}()
		jtest.Require(t, context.Canceled, Run(ctx2, sess, name, rankNoop, opt))
	}()

	time.Sleep(time.Second)
	cancel2()
	<-done2
}
