package rink

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-jump"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const modprefix = "modrole:"

// ModRole returns a role that will be mapped to a member by modulo operation
// instead of consistent hashing.
//
// This is useful is even distribution of roles is important.
func ModRole(role int) string {
	return fmt.Sprintf("%s%d", modprefix, role)
}

// New returns a new scheduler linked to the session and cluster prefix. It also starts the underlying cluster.
// Closing the session releases all etcd resources and results in asynchronous release of all scheduler golang resources.
// Note that Scheduler.Close is a synchronous alternative.
func New(sess *concurrency.Session, clusterPrefix string, opts ...Option) *Scheduler {
	o := defaultOptions(sess.Lease())
	for _, opt := range opts {
		opt(o)
	}

	ctx, cancel := context.WithCancel(o.ctx)

	c := &cluster{
		ctx:            ctx,
		sess:           sess,
		clusterPrefix:  clusterPrefix,
		name:           o.name,
		rebalanceDelay: o.rebalanceDelay,
		logger:         o.logger,
	}

	s := &Scheduler{
		ctx:           ctx,
		cancel:        cancel,
		sess:          sess,
		clusterPrefix: clusterPrefix,
		name:          o.name,
		hasher:        o.hasher,
		logger:        o.logger,
		cond:          sync.NewCond(new(sync.Mutex)),
		roles:         make(map[string]*roleCtx),
	}

	// Link the scheduler to the cluster.
	c.callback = s.updateState

	// Link root context/lifecycle to that of session.
	go func() {
		<-sess.Done()
		s.logger.Debug(s.ctx, "rink session closed")
		s.cancel()             // Close root context
		s.updateState(state{}) // Clear state
	}()

	// Start the cluster.
	go startCluster(c)

	return s
}

// Scheduler maps arbitrary roles to members of the cluster by consistent
// hashing to the mth of n member. It also maintains
// additional etcd mutex locks for each role this instance assumes.
// This ensures that overlapping roles is not possible.
type Scheduler struct {
	ctx           context.Context
	cancel        context.CancelFunc
	clusterPrefix string
	name          string
	sess          *concurrency.Session
	hasher        hasher
	logger        logger

	cond  *sync.Cond
	state state

	roles map[string]*roleCtx
}

// Close closes the etcd session which releases all etcd resources or returns an error. It the session is closed, it
// also synchronously releases all golang resources.
func (s *Scheduler) Close() error {
	select {
	case <-s.sess.Done():
		// Session already closed.
	default:
		// Most important, first close the session removing all etcd keys.
		err := s.sess.Close()
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// NoReturnErr: Session already closed.
		} else if err != nil {
			return err
		}
	}

	s.cancel()             // Close root context
	s.updateState(state{}) // Clear state

	return nil
}

// Info returns the current scheduler rank (m) and cluster size (n).
// A rank of -1 indicates this scheduler has joined the cluster but is waiting for a rank.
// A size of 0 indicates this scheduler has not joined a cluster yet or has been stopped and left the cluster.
func (s *Scheduler) Info() (rank int, size int) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	return getInfo(s.state, s.name)
}

// Await blocks until this scheduler can assign the role and returns a role context.
// The context will be closed when the role is assigned to another member of the cluster.
func (s *Scheduler) Await(role string) context.Context {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for !hasRole(s.state, s.hasher, s.name, role) {
		// Wait while we do not have the role.
		//
		// Note: Wait unlocks s.cond.L and locks it again before
		// returning. So multiple goroutines can wait at the
		// same time but only one will continue at a time.
		s.cond.Wait()
	}

	// We have the role (and s.conf.L is locked)

	return s.getOrCreateUnsafe(role)
}

// Get returns true and a role context if this scheduler can assume the role now.
// The context will be closed when the role is assigned to another member of the cluster.
// It returns false and a nil context if this scheduler cannot assume the role now.
func (s *Scheduler) Get(role string) (context.Context, bool) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if !hasRole(s.state, s.hasher, s.name, role) {
		return nil, false
	}

	return s.getOrCreateUnsafe(role), true
}

// getOrCreateUnsafe returns an existing or new role context for the provided role.
// If new, an additional etcd mutex is also locked for additional protection against
// overlapping roles. It is unsafe since it assumes the s.cond.L lock is held.
func (s *Scheduler) getOrCreateUnsafe(role string) context.Context {
	if roleCtx, ok := s.roles[role]; ok {
		return roleCtx.ctx // Just return existing role context.
	}

	// FIXME(corver): Creating a new role context for each role will result
	//  in resource leak if roles are unbounded. Maybe expose
	//  `Release` method in the API.

	roleCtx := newRoleCtx(s, role)
	s.roles[role] = roleCtx

	// Lock additional etcd role mutex.
	roleCtx.Lock()

	return roleCtx.ctx
}

func (s *Scheduler) updateState(next state) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if len(next) > 0 && s.ctx.Err() != nil {
		// Ignore racey cluster updates after close.
		return
	}

	// Cancel roles we do not have anymore.
	var toCancel []*roleCtx
	for role, roleCtx := range s.roles {
		if hasRole(next, s.hasher, s.name, role) {
			// We still have this role!
			continue
		}
		toCancel = append(toCancel, roleCtx)
	}

	for _, roleCtx := range toCancel {
		roleCtx.Release()
		delete(s.roles, roleCtx.role)
	}

	s.state = next
	s.cond.Broadcast()

	// Metrics and logging
	rank, has := s.state[s.name]
	if len(s.state) > 0 && !has {
		rank = -1 // Use -1 to indicate waiting to join.
	}

	s.logger.Debug(s.ctx, "rink state updated", j.MKV{
		"size": len(s.state), "rank": rank})
	rankGauge.WithLabelValues(s.clusterPrefix).Set(float64(rank))
}

func newRoleCtx(r *Scheduler, role string) *roleCtx {
	ctx, cancel := context.WithCancel(r.ctx)
	ctx = log.ContextWith(ctx, j.KS("rink_role", role))

	mutex := concurrency.NewMutex(r.sess, path.Join(r.clusterPrefix, "roles", role))
	gauge := roleGauge.WithLabelValues(r.clusterPrefix, role)

	return &roleCtx{
		ctx:    ctx,
		cancel: cancel,
		role:   role,
		logger: r.logger,
		mutex:  mutex,
		gauge:  gauge,
	}
}

type roleCtx struct {
	mutex  *concurrency.Mutex
	role   string
	ctx    context.Context
	cancel context.CancelFunc
	logger logger
	gauge  prometheus.Gauge
}

func (r *roleCtx) Lock() {
	reliably(r.ctx, r.logger, "lock role", func() error {
		return r.mutex.Lock(r.ctx)
	})

	r.logger.Info(r.ctx, "rink role locked")
	r.gauge.Set(1)
}

// Release releases the role by canceling the role context and unlocking the role mutex.
func (r *roleCtx) Release() {
	defer r.logger.Debug(r.ctx, "rink role released")
	defer r.gauge.Set(0)

	r.cancel()

	// Try a quick synchronous mutex unlock (with fresh context).
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()

	err := r.mutex.Unlock(ctx)
	if err == nil {
		// It worked.
		return
	}

	// Do not block, but keep on trying to unlock.
	go func() {
		reliably(r.ctx, r.logger, "unlock role", func() error {
			return r.mutex.Unlock(context.Background()) // Unlock is idempotent.
		})
	}()
}

// hasRole returns true if the member (name) has the role.
func hasRole(state state, hasher hasher, name, role string) bool {
	rank, ok := state[name]
	if !ok {
		return false
	}

	if i, ok := maybeModRole(role); ok {
		return rank == i%len(state)
	}

	// Convert role string to uint64 hash key
	h := hasher()
	_, _ = h.Write([]byte(role))
	b := h.Sum(nil)
	key := binary.BigEndian.Uint64(b[len(b)-8:])

	// Get the role rank (hash the key to a bucket)
	rolerank := jump.Hash(key, len(state))

	return rank == int(rolerank)%len(state)
}

// maybeModRole returns true and the integer to use
// if this role should be mapped with modulo operation instead of
// consistent hashing.
func maybeModRole(role string) (int, bool) {
	if !strings.HasPrefix(role, modprefix) {
		return 0, false
	}

	i, err := strconv.Atoi(strings.TrimPrefix(role, modprefix))
	if err != nil {
		// NoReturnErr: Assume not a mod role
		return 0, false
	}

	return i, true
}
