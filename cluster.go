package rink

import (
	"bytes"
	"context"
	"encoding/gob"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const campaignPrefix = "campaign/"

// state is the state of the cluster, a map of member sessions by rank.
type state map[string]int

// cluster maintains membership of the ranked ring.
type cluster struct {
	callback func(state)

	sess           *concurrency.Session
	clusterPrefix  string
	name           string
	rebalanceDelay time.Duration
	ctx            context.Context
	logger         logger

	mu    sync.Mutex
	state state // Immutable
}

// cloneState returns a cloned state that is safe to mutated.
func (c *cluster) cloneState() state {
	c.mu.Lock()
	defer c.mu.Unlock()

	clone := make(state)
	for k, v := range c.state {
		clone[k] = v
	}

	return clone
}

func (c *cluster) updateState(state state) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.callback(state)

	c.state = state
}

// startCluster joins the member to the cluster and starts cluster goroutines.
func startCluster(c *cluster) {
	// Create member key
	reliably(c.ctx, c.logger, "member key", func() error {
		return createMemberKey(c)
	})

	elec := concurrency.NewElection(c.sess, path.Join(c.clusterPrefix, "election"))

	campaignCh := make(chan struct{}, 1)
	go leadForever(c, elec, campaignCh)
	go observeForever(c, elec, campaignCh)
}

func observeForever(c *cluster, elec *concurrency.Election, ch chan<- struct{}) {
	for {
		observeOnce(c, elec, ch)
		if c.ctx.Err() != nil {
			// NoReturnErr: Just exit forever once context done.
			return
		}

		// We lost connection, so might become out of sync, but since we lock roles it is ok.
		c.logger.Error(c.ctx, errors.New("observe election failed"))
		time.Sleep(time.Second * 5) // Backoff 5 seconds, then try again.
	}
}

// observeOnce watches the election append-only log of proclamations.
// It returns when the context is closed or the underlying watcher
// is otherwise disrupted (network issues).
func observeOnce(c *cluster, elec *concurrency.Election, ch chan<- struct{}) {

	// Note: Election.Observe returns a subset of previous proclamations.
	// So members are eventually consistent and will first cycle through previous/old states.
	// But that is ok since scheduler locks roles to avoid any overlapping.

	for res := range elec.Observe(c.ctx) {
		v := res.Kvs[0].Value
		c.logger.Debug(c.ctx, "observed election proposal", j.KS("val", string(v)))

		if strings.HasPrefix(string(v), campaignPrefix) {
			// Someone is the new leader
			if strings.HasSuffix(string(v), c.name) {
				// We are leader, notify leadForever
				ch <- struct{}{}
			}
			continue
		}

		var s state
		err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(&s)
		if err != nil {
			// NoReturnErr: Just log.
			c.logger.Error(c.ctx, errors.Wrap(err, "decode state"))
			continue
		}

		c.updateState(s)
	}
}

func leadForever(c *cluster, elec *concurrency.Election, ch <-chan struct{}) {
	for {
		leadOnce(c, elec, ch)
		if c.ctx.Err() != nil {
			// NoReturnErr: Just exit forever once context done.
			return
		}

		c.logger.Debug(c.ctx, "rink cluster leadership lost")

		time.Sleep(time.Second * 5) // Backoff 5 seconds, then try again.
	}
}

func leadOnce(c *cluster, elec *concurrency.Election, ch <-chan struct{}) {
	gauge := leaderGauge.WithLabelValues(c.clusterPrefix)
	gauge.Set(0)
	defer gauge.Set(0)

	ctx := c.ctx
	c.logger.Debug(ctx, "campaigning for rink cluster leadership")

	// Campaign for leader (this blocks until we are leader)
	reliably(ctx, c.logger, "campaign", func() error {
		return elec.Campaign(ctx, campaignPrefix+c.name)
	})

	if ctx.Err() != nil {
		// NoReturnErr: Just exit once context done.
		return
	}

	// We are leader
	c.logger.Debug(ctx, "rink cluster leadership gained")
	gauge.Set(1)

	// Monitor members and notify on join/leave
	notifyCh := make(chan struct{}, 1)
	go reliably(ctx, c.logger, "watch members", func() error {
		if elec.Key() == "" {
			// We are not leader anymore
			return nil
		}

		cl := c.sess.Client()
		wch := cl.Watch(ctx, path.Join(c.clusterPrefix, "members"), clientv3.WithPrefix())
		for res := range wch {
			if res.Err() != nil {
				return res.Err()
			}

			for _, event := range res.Events {
				if event.IsModify() {
					// Ignore modifications (heart beats)
					continue
				}

				// Member joined or left
				msg := "join"
				if !event.IsCreate() {
					msg = "left"
				}
				c.logger.Debug(ctx, "rink cluster changed", j.KV("event", msg))

				select {
				case notifyCh <- struct{}{}:
				default:
					c.logger.Debug(c.ctx, "rink cluster notify channel full")
				}

				break
			}
		}

		return errors.New("watch channel closed")
	})

	timer := time.NewTimer(time.Second)

	// Wait for observeForever to sync latest state or cancel or timeout (1s)
	select {
	case <-ch:
	case <-timer.C:
	case <-ctx.Done():
		return
	}

	// While leader, react to changes in members, rebalance timers and context cancel

	// Trigger immediately on first loop.
	reset(timer, time.Nanosecond)

	handleErr := func(err error) {
		if ctx.Err() != nil {
			// NoReturnErr: Assume the error is related to context closed.
			return
		}
		c.logger.Error(ctx, err)
		reset(timer, time.Second*5)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-notifyCh:
		}

		if elec.Key() == "" {
			// We are not leader anymore
			return
		}

		members, err := listMembers(c)
		if err != nil {
			// NoReturnErr: handle error and loop again.
			handleErr(err)
			continue
		}

		s := c.cloneState()

		updated := maybePromote(s, members)

		next, ok := nextRebalance(s, members)
		if ok && next == 0 {
			// Rebalance now
			s = rebalance(s, members)
			updated = true
		} else if ok {
			// Rebalance in future
			reset(timer, next)
		}

		if !updated {
			continue
		}

		var buf bytes.Buffer
		err = gob.NewEncoder(&buf).Encode(s)
		if err != nil {
			// NoReturnErr: handle error and loop again.
			handleErr(err)
			continue
		}

		err = elec.Proclaim(ctx, buf.String())
		if err != nil {
			// NoReturnErr: handle error and loop again.
			handleErr(err)
			continue
		}

		c.updateState(s)
	}
}

func reset(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// maybePromote returns true if it mutated the state by "promoting" any
// waiting members to the rank of any left members.
func maybePromote(state state, members map[string]time.Time) bool {
	var waiting []string
	for member := range members {
		if _, ok := state[member]; !ok {
			waiting = append(waiting, member)
		}
	}

	if len(waiting) == 0 {
		// No waiting members
		return false
	}

	// Get keys so we can mutate the state.
	var stateKeys []string
	for member := range state {
		stateKeys = append(stateKeys, member)
	}

	var promoted bool
	for _, member := range stateKeys {
		if _, ok := members[member]; ok {
			// Member still active has a rank
			continue
		}

		// Member left, promote first waiting member
		promote := waiting[0]
		waiting = waiting[1:]

		state[promote] = state[member]
		delete(state, member)
		promoted = true

		if len(waiting) == 0 {
			// No more waiting members
			break
		}
	}

	return promoted
}

// rebalance returns a new state with a rank assigned to each member.
// If tries to assign a previous rank.
func rebalance(old state, members map[string]time.Time) state {

	// Assign old ranks if possible
	result := make(state)
	reverse := make(map[int]bool)
	for member, rank := range old {
		if rank >= len(members) {
			// Old rank too big
			continue
		}

		_, ok := members[member]
		if !ok {
			// Member left
			continue
		}

		result[member] = rank
		reverse[rank] = true
	}

	// Assign remaining ranks.
	var next int
	for member := range members {
		_, ok := result[member]
		if ok {
			continue
		}
		for reverse[next] {
			next++
		}
		result[member] = next
		reverse[next] = true
	}

	return result
}

// reliably executes the function while it returns an error (infinite retry).
// It will return when the function successfully completed (no error returned),
// or when the context is cancelled.
func reliably(ctx context.Context, logger logger, label string, fn func() error) {
	for {
		err := fn()
		if ctx.Err() != nil {
			// NoReturnErr: Context closed, assume error is due to that, just return.
			return
		} else if err != nil {
			// NoReturnErr: Log sleep and try again.
			logger.Error(ctx, errors.Wrap(err, "reliably", j.KS("label", label)))
			time.Sleep(time.Second * 5)
			continue
		}

		return
	}
}

func createMemberKey(c *cluster) error {
	sess := c.sess
	cl := sess.Client()

	key := path.Join(c.clusterPrefix, "members", c.name)
	val := time.Now().Add(c.rebalanceDelay).Unix()

	_, err := cl.Put(c.ctx, key, strconv.FormatInt(val, 10), clientv3.WithLease(sess.Lease()))
	if err != nil {
		return err
	}

	return nil
}

func listMembers(c *cluster) (map[string]time.Time, error) {
	prefix := path.Join(c.clusterPrefix, "members")
	res, err := c.sess.Client().Get(c.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	members := make(map[string]time.Time)
	for _, kv := range res.Kvs {
		var t time.Time
		if len(kv.Value) > 0 {
			unix, err := strconv.ParseInt(string(kv.Value), 10, 64)
			if err != nil {
				// NoReturnErr: Log and use zero value.
				c.logger.Error(nil, errors.New("invalid unix timestamp member key"))
			} else {
				t = time.Unix(unix, 0)
			}
		}
		name := strings.TrimPrefix(string(kv.Key), prefix)
		name = strings.TrimLeft(name, "/")
		members[name] = t
	}

	return members, nil
}

// nextRebalance returns true and a optional delay of when the next rebalance should occur.
// It returns false if no rebalance is needed.
func nextRebalance(state state, members map[string]time.Time) (time.Duration, bool) {
	for member := range state {
		if _, ok := members[member]; ok {
			// Member still active
			continue
		}

		// Member left, rebalance now.
		return 0, true
	}

	var next time.Duration
	for member, rebalanceAt := range members {
		if _, ok := state[member]; ok {
			// Member has a rank
			continue
		}

		if rebalanceAt.IsZero() {
			// Zero rebalanceAt means it doesn't trigger a rebalance
			continue
		}

		delta := rebalanceAt.Sub(time.Now())

		if delta <= 0 {
			// Rebalance now
			return 0, true
		}
		if next == 0 || next > delta {
			next = delta
		}
	}

	return next, next > 0
}

func getInfo(state state, name string) (rank, size int) {
	if len(state) == 0 {
		return 0, 0
	}

	rank, ok := state[name]
	if !ok {
		rank = -1
	}

	return rank, len(state)
}
