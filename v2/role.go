package rink

import (
	"context"
	"path"
	"sync"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var ErrAlreadyLocked = errors.New("", j.C("ERR_383b12a0162931fe"))

type RoleContext interface {
	Role() string
	Context() context.Context
	Lock() error
	Unlock() error
}

// LockedContext keeps a Context in sync with an etcd Mutex
type LockedContext struct {
	sess  *concurrency.Session
	mutex *concurrency.Mutex
	role  string

	ctx     context.Context
	lockCtx context.Context
	cancel  context.CancelFunc
}

func NewLockedContext(
	ctx context.Context,
	sess *concurrency.Session,
	namespace, role string,
) *LockedContext {
	return &LockedContext{
		sess:  sess,
		mutex: concurrency.NewMutex(sess, path.Join(namespace, "roles", role)),
		role:  role,
		ctx:   log.ContextWith(ctx, j.KS("rink_role", role)),
	}
}

func (c *LockedContext) Role() string {
	return c.role
}

func (c *LockedContext) Context() context.Context {
	return c.lockCtx
}

func (c *LockedContext) Lock() error {
	if c.lockCtx != nil && c.lockCtx.Err() == nil {
		return errors.Wrap(ErrAlreadyLocked, "")
	}
	err := c.mutex.TryLock(c.ctx)
	if errors.Is(err, concurrency.ErrLocked) {
		return errors.Wrap(ErrAlreadyLocked, err.Error())
	} else if err != nil {
		return errors.Wrap(err, "try lock")
	}
	ctx, cancel := context.WithCancel(c.ctx)
	ctx = log.ContextWith(ctx, j.KV("role", c.role))
	c.lockCtx, c.cancel = ctx, cancel
	return nil
}

func (c *LockedContext) Unlock() error {
	c.cancel()
	return c.mutex.Unlock(c.sess.Client().Ctx())
}

type RolesOptions struct {
	// RoleNotify will be called when the locked status of any role is changed
	RoleNotify func(role string, locked bool)

	// Log is used for logging messages and errors on role management
	Log log.Interface
}

type Roles struct {
	options  RolesOptions
	delegate RoleDelegate

	cond  *sync.Cond
	rank  Rank
	roles map[string]RoleContext
}

type RoleDelegate interface {
	Create(role string) (RoleContext, error)
	Assign(rank Rank, role string) bool
}

func NewRoles(delegate RoleDelegate, opts RolesOptions) *Roles {
	if opts.RoleNotify == nil {
		opts.RoleNotify = func(string, bool) {}
	}
	if opts.Log == nil {
		opts.Log = noopLogger{}
	}
	return &Roles{
		options:  opts,
		delegate: delegate,
		cond:     sync.NewCond(new(sync.Mutex)),
		roles:    make(map[string]RoleContext),
	}
}

func (r *Roles) unlockAll() {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	for _, rc := range r.roles {
		r.options.Log.Debug(rc.Context(), "role released due to shutdown")
		_ = rc.Unlock()
		r.options.RoleNotify(rc.Role(), false)
	}
	r.roles = make(map[string]RoleContext)
}

func (r *Roles) updateRank(rank Rank) {
	defer r.cond.Broadcast()

	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	var toRelease []RoleContext
	for _, rc := range r.roles {
		if r.delegate.Assign(rank, rc.Role()) {
			continue
		}
		toRelease = append(toRelease, rc)
	}
	for _, rc := range toRelease {
		r.options.Log.Debug(rc.Context(), "role released due to rank change")
		_ = rc.Unlock()
		r.options.RoleNotify(rc.Role(), false)
		delete(r.roles, rc.Role())
	}
	r.rank = rank
}

func (r *Roles) AwaitRole(role string) context.Context {
	for {
		ctx, err := r.awaitRoleOnce(role)
		if err != nil {
			r.options.Log.Error(context.TODO(), err, j.KV("role", role))
			continue
		}
		return ctx
	}
}

func (r *Roles) awaitRoleOnce(role string) (context.Context, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	for !r.delegate.Assign(r.rank, role) {
		r.cond.Wait()
	}

	return r.getOrCreateRoleUnsafe(role)
}

func (r *Roles) Get(role string) (context.Context, bool) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	if !r.delegate.Assign(r.rank, role) {
		return nil, false
	}

	ctx, err := r.getOrCreateRoleUnsafe(role)
	if err != nil {
		r.options.Log.Error(context.TODO(), err, j.KV("role", role))
		return nil, false
	}
	return ctx, true
}

func (r *Roles) getOrCreateRoleUnsafe(role string) (context.Context, error) {
	rc, ok := r.roles[role]
	if ok {
		return rc.Context(), nil
	}
	rc, err := r.delegate.Create(role)
	if err != nil {
		return nil, err
	}
	if err := rc.Lock(); err != nil {
		return nil, err
	}
	ctx := rc.Context()
	r.options.Log.Debug(ctx, "role acquired")
	r.options.RoleNotify(rc.Role(), true)
	r.roles[role] = rc
	return rc.Context(), nil
}
