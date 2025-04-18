package rink

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/errgroup"
)

type (
	AssignRoleFunc func(role string, roleCount int32) int32
	RoleNotify     func(role string, locked bool)
)

type RolesOptions struct {
	// Log is used for logging messages and errors on role management
	Log log.Interface

	// AwaitRetry is used when we get an error while trying to create
	// a lock. We will wait this amount of time to try again.
	AwaitRetry time.Duration

	// LockTimeout is how long we will wait when attempting to get a role lock.
	// TODO(adam): Replace timeout with an asynchronous blocking call to Lock
	LockTimeout time.Duration

	// Assign is used for passing a custom implementation that maps
	// roles onto ranks. The function should return a rank in the range [0, roleCount).
	// Alternatively, if the role cannot be mapped, it can return -1.
	// It is critical that the assign role func always maps a given role+roleCount
	// to the same rank.
	Assign AssignRoleFunc

	// Notify is a callback that will be called when roles are locked or unlocked.
	Notify RoleNotify
}

// roleLockReq is the client side request to attach a context to a role lock
type roleLockReq struct {
	// The role we need
	Role string
	// The channel to send the response on
	Receive chan roleLockResp
}

func newRequest(role string) roleLockReq {
	return roleLockReq{
		Role: role,
		// Construct a channel for assignRoles to deliver on
		// we can re-use this channel after it has been drained,
		// so we must make sure that we receive from this channel
		// every time we deliver it to r.lockers
		// We use a buffered channel so that if the client goes
		// away before assignRoles responds, then we don't block
		Receive: make(chan roleLockResp, 1),
	}
}

// roleLockResp is returned by assign roles
type roleLockResp struct {
	// Locked is whether the role is successfully locked
	// If true, the context is attached to the role, and we can return the context
	Locked bool

	// Err is returned when there was an issue with locking the role
	// Most commonly this is when another lease has the role locked
	Err error

	// WaitForChange is a channel that will be closed when the rank changes
	// this will be populated when the role is not currently assigned
	WaitForChange <-chan struct{}

	// OnRoleUnlock
	OnRoleUnlock <-chan struct{}
}

// lockedContext holds a mutex and all the context cancellation functions that need
// to be called when the mutex is Unlocked
type lockedContext struct {
	role string
	mu   *concurrency.Mutex
	sig  *Signal
}

func (l lockedContext) Cancel() {
	l.sig.Broadcast()
}

func cancelOnClose(ctx context.Context, cancel context.CancelFunc, onClose <-chan struct{}) {
	select {
	case <-ctx.Done():
	case <-onClose:
		cancel()
	}
}

// Roles is an abstraction of the two sides of the role allocation API.
// On the one side we have clients that want to get contexts for roles.
// On the other side we have the rink cluster which provides the cluster rank and etcd session.
// The rink side calls into assignRoles which is responsible for locking and unlocking roles.
// The client side calls into AwaitRole, AwaitRoleContext, and Get and uses the internal channel to request roles.
type Roles struct {
	namespace string
	options   RolesOptions

	lockers    chan roleLockReq
	rankChange chan Rank

	legacyRoles sync.Map
}

func NewRoles(namespace string, opts RolesOptions) *Roles {
	if opts.Log == nil {
		opts.Log = noopLogger{}
	}
	if opts.AwaitRetry == 0 {
		opts.AwaitRetry = 10 * time.Second
	}
	if opts.LockTimeout == 0 {
		opts.LockTimeout = time.Second
	}
	if opts.Assign == nil {
		opts.Assign = ConsistentHashRole
	}
	if opts.Notify == nil {
		opts.Notify = func(string, bool) {}
	}
	return &Roles{
		namespace:  namespace,
		options:    opts,
		lockers:    make(chan roleLockReq),
		rankChange: make(chan Rank),
	}
}

func (r *Roles) assign(rank Rank, role string) bool {
	if !rank.HaveRank {
		return false
	}
	roleRank := r.options.Assign(role, rank.Size)
	if roleRank < 0 {
		return false
	}
	return rank.MyRank == roleRank
}

func (r *Roles) updateRank(ctx context.Context, rank Rank) {
	select {
	case r.rankChange <- rank:
	case <-ctx.Done():
	}
}

func mutexKey(namespace, role string) string {
	return path.Join(namespace, "roles", role)
}

func createLock(ctx context.Context,
	sess *concurrency.Session,
	namespace string, role string,
	lockTimeout time.Duration,
) (lockedContext, error) {
	key := mutexKey(namespace, role)
	mu := concurrency.NewMutex(sess, key)

	var err error
	if lockTimeout > 0 {
		lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
		defer cancel()
		err = mu.Lock(lockCtx)
	} else {
		err = mu.TryLock(ctx)
	}
	if errors.IsAny(err, context.DeadlineExceeded, concurrency.ErrLocked) {
		err = errors.Wrap(err, "another process has locked the role")
		// NoReturnErr: Will return it, just try to embellish it a bit first
		resp, getErr := sess.Client().Get(ctx, key, clientv3.WithFirstCreate()...)
		if getErr != nil {
			// NoReturnErr: Will return the original error
			log.Error(ctx, errors.Wrap(getErr, "getting lock details"))
		} else if len(resp.Kvs) == 0 {
			err = errors.Wrap(err, "", j.KV("held_by_lease", "null"))
		} else {
			err = errors.Wrap(err, "", j.KV("held_by_lease", resp.Kvs[0].Lease))
		}
	}
	if err != nil {
		return lockedContext{}, errors.Wrap(err, "")
	}
	return lockedContext{role: role, mu: mu, sig: NewSignal()}, nil
}

func (r *Roles) getOrCreateLock(ctx context.Context,
	sess *concurrency.Session,
	role string, locks map[string]lockedContext,
) (lockedContext, error) {
	l, ok := locks[role]
	if ok {
		return l, nil
	}
	l, err := createLock(ctx, sess, r.namespace, role, r.options.LockTimeout)
	if err != nil {
		return lockedContext{}, err
	}
	r.options.Notify(role, true)
	locks[role] = l
	return l, nil
}

func (r *Roles) assignRoles(ctx context.Context, sess *concurrency.Session) error {
	r.options.Log.Debug(ctx, "started assigning roles")
	defer r.options.Log.Debug(ctx, "stopped assigning roles")

	nextReRank := NewSignal()
	// Trigger the signal so that waiters queue up on r.lockers.
	defer nextReRank.Broadcast()

	locks := make(map[string]lockedContext)
	// Track not assigned roles for notifying
	notified := make(map[string]bool)

	defer func() {
		// Cancel all the contexts here but don't need to unlock the mutexes
		// because they will be expired by etcd when we close the session
		for _, lock := range locks {
			lock.Cancel()
			r.options.Notify(lock.role, false)
		}
	}()

	r.options.Log.Debug(ctx, "waiting for first rank")

	var rank Rank

	// Wait until we have the first rank change.
	// The cluster will always provide one when it's initialised,
	// so we won't block waiting here for long.
	select {
	case rank = <-r.rankChange:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.options.Log.Debug(ctx, "got first rank",
		j.MKV{"rank": rank.MyRank, "size": rank.Size, "have": rank.HaveRank},
	)

	for {
		select {
		// Requests from clients for new role locks
		// Check if assigned, create a lock if needed and return the response to the client
		case req := <-r.lockers:
			var ret roleLockResp
			if r.assign(rank, req.Role) {
				l, err := r.getOrCreateLock(ctx, sess, req.Role, locks)
				if err != nil {
					// NoReturnErr: Hand it back to the client to handle
					ret.Err = err
				} else {
					ret.Locked = true
					ret.OnRoleUnlock = l.sig.Wait()
				}
			} else {
				// Return a channel for the client to get the next rank change
				ret.WaitForChange = nextReRank.Wait()
			}

			if !ret.Locked && !notified[req.Role] {
				r.options.Notify(req.Role, false)
				notified[req.Role] = true
			}

			r.options.Log.Debug(ctx, "handled role request", j.MKV{
				"role":   req.Role,
				"locked": ret.Locked,
				"err":    ret.Err != nil,
			})
			req.Receive <- ret

		// Notifications from the cluster of a Rank change
		// Unallocated any unassigned and Broadcast to any waiters
		case rank = <-r.rankChange:
			r.options.Log.Debug(ctx, "received new rank",
				j.MKV{"rank": rank.MyRank, "size": rank.Size, "have": rank.HaveRank},
			)

			var toUnlock []lockedContext
			for _, lock := range locks {
				if !r.assign(rank, lock.role) {
					lock.Cancel()
					r.options.Notify(lock.role, false)
					delete(locks, lock.role)
					toUnlock = append(toUnlock, lock)
				}
			}
			if err := r.unlockMutexes(ctx, toUnlock); err != nil {
				// Return the error so that the session can be cancelled
				return err
			}
			// Tell any waiting goroutines that they should try again
			// to see if they're assigned now
			nextReRank.Broadcast()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// unlockMutexes will unlock each mutex individually.
// This should be used when your session will continue to be alive.
// If there's an error unlocking then we give up because we assume that
// we'll need to close the session which will unlock all the mutexes anyway.
func (r *Roles) unlockMutexes(ctx context.Context, locks []lockedContext) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, l := range locks {
		l := l
		eg.Go(func() error {
			if err := l.mu.Unlock(ctx); err != nil {
				return errors.Wrap(err, "unable to unlock role", j.KV("role", l.role))
			}
			r.options.Log.Debug(ctx, "unlocked role", j.KV("role", l.role))
			return nil
		})
	}
	return eg.Wait()
}

type legacyRole struct {
	mu  sync.Mutex
	ctx context.Context
}

// Deprecated: Use AwaitRoleContext
func (r *Roles) AwaitRole(role string) context.Context {
	a, _ := r.legacyRoles.LoadOrStore(role, &legacyRole{})
	lr := a.(*legacyRole)

	lr.mu.Lock()
	defer lr.mu.Unlock()

	if lr.ctx != nil && lr.ctx.Err() == nil {
		return lr.ctx
	}

	ctx, _, _ := r.AwaitRoleContext(context.Background(), role)
	lr.ctx = ctx
	return ctx
}

// AwaitRoleContext will wait for a role to be assigned and return a new context
// Or if ctx is cancelled before then, it will return an error.
// If we're assigned the role, we attach the context to the role and return the context.
// If we're not assigned the role, we wait until the Rank changes
// If there's an error locking the role, we will wait for RolesOptions.AwaitRetry
func (r *Roles) AwaitRoleContext(ctx context.Context, role string) (context.Context, context.CancelFunc, error) {
	ctx = log.ContextWith(ctx, j.KV("role", role))
	req := newRequest(role)
	// Whenever we can read from deliver we will send req to the lockers channel
	deliver := Immediate()
	// Wait will be provided by assignRoles, it will close when there is a change in rank
	var wait <-chan struct{}

	for {
		select {
		case <-deliver:
			r.options.Log.Debug(ctx, "client requesting role")
			select {
			case r.lockers <- req:
				// Nil the deliver channel, won't re-deliver until failure
				deliver = nil
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		case resp := <-req.Receive:
			if resp.Locked {
				ctx, cancel := context.WithCancel(ctx)
				go cancelOnClose(ctx, cancel, resp.OnRoleUnlock)
				return ctx, cancel, nil
			}
			// If the base context has been cancelled we can continue the loop
			// and the ctx.Done will drop us out
			if resp.Err != nil && !errors.Is(resp.Err, context.Canceled) {
				r.options.Log.Error(ctx, resp.Err)
				deliver = time.After(r.options.AwaitRetry)
			}
			wait = resp.WaitForChange
		case <-wait:
			r.options.Log.Debug(ctx, "client got rank change, retrying", j.KV("role", req.Role))
			wait = nil
			deliver = Immediate()
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// Get will check Roles to see if we have the role and return.
// Crucial difference to AwaitRoleContext is that it won't wait for rank changes until we DO have the role.
func (r *Roles) Get(ctx context.Context, role string) (context.Context, context.CancelFunc, bool) {
	req := newRequest(role)
	r.options.Log.Debug(ctx, "client requesting role", j.KV("role", req.Role))

	select {
	case r.lockers <- req:
	case <-ctx.Done():
		return nil, nil, false
	}

	select {
	case resp := <-req.Receive:
		if resp.Err != nil {
			// NoReturnErr: Log the error and return false
			r.options.Log.Error(ctx, resp.Err)
			return nil, nil, false
		}
		if !resp.Locked {
			return nil, nil, false
		}
		ctx, cancel := context.WithCancel(ctx)
		go cancelOnClose(ctx, cancel, resp.OnRoleUnlock)
		return ctx, cancel, true
	case <-ctx.Done():
		return nil, nil, false
	}
}
