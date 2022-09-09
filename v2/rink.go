package rink

import (
	"context"
	"sync"
	"time"

	"github.com/luno/jettison"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/errgroup"
)

type Logger interface {
	Debug(ctx context.Context, s string, ol ...jettison.Option)
	Info(ctx context.Context, s string, ol ...jettison.Option)
	Error(ctx context.Context, err error, ol ...jettison.Option)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, ...jettison.Option) {}
func (noopLogger) Info(context.Context, string, ...jettison.Option)  {}
func (noopLogger) Error(context.Context, error, ...jettison.Option)  {}

type options struct {
	Log Logger

	AssignRoleFunc func(role string, roleCount int32) int32

	ClusterOptions ClusterOptions

	RolesOptions RolesOptions
}

type Option func(*options)

type AssignRoleFunc func(role string, roleCount int32) int32

// WithAssignRoleFunc is used for passing a custom implementation that maps
// roles onto ranks. The function should return a rank in the range [0, roleCount).
// Alternatively, if the role cannot be mapped, it can return -1.
// It is important that the assign role func always maps a given role+roleCount
// to the same rank.
func WithAssignRoleFunc(f func(role string, roleCount int32) int32) Option {
	return func(o *options) {
		o.AssignRoleFunc = f
	}
}

// WithClusterOptions passes through specific options to the Cluster.
// See ClusterOptions for more details.
func WithClusterOptions(opts ClusterOptions) Option {
	return func(o *options) {
		o.ClusterOptions = opts
	}
}

// WithRolesOptions passes through options to the Roles interface.
// See RolesOptions for more details.
func WithRolesOptions(opts RolesOptions) Option {
	return func(o *options) {
		o.RolesOptions = opts
	}
}

// WithLogger sets a logger to be used for logging in Rink.
// It is also used for the Roles and Cluster logging if not specified in
// ClusterOptions or RolesOptions
func WithLogger(l Logger) Option {
	return func(o *options) {
		o.Log = l
	}
}

func buildOptions(opts []Option) options {
	o := options{
		AssignRoleFunc: ConsistentHashRole,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.RolesOptions.Log == nil {
		o.RolesOptions.Log = o.Log
	}
	if o.ClusterOptions.Log == nil {
		o.ClusterOptions.Log = o.Log
	}
	if o.Log == nil {
		o.Log = noopLogger{}
	}
	return o
}

// Rink manages roles for members of a cluster.
// Each role is assigned to a single member of the cluster at any one time.
//
// Should the connection to the etcd client fail, roles will be revoked until
// it can be re-established.
//
// The Roles' struct is used to manage individual roles.
type Rink struct {
	cli      *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{}

	name    string
	options options

	Roles *Roles

	sessionMutex   sync.Mutex
	currentSession *concurrency.Session
}

// New starts Rink and runs the Cluster.
// Call Shutdown to stop and clean up rink roles.
// Use Roles to access individual roles.
func New(cli *clientv3.Client, name string, opts ...Option) *Rink {

	ctx, cancel := context.WithCancel(context.Background())
	s := &Rink{
		cli:      cli,
		ctx:      ctx,
		cancel:   cancel,
		finished: make(chan struct{}),

		name:    name,
		options: buildOptions(opts),
	}
	s.Roles = NewRoles(rinkDelegate{s}, s.options.RolesOptions)

	go s.run(ctx)
	return s
}

// Shutdown will synchronously release all locked roles, shutdown the cluster, and
// revoke the current Session.
func (s *Rink) Shutdown() {
	s.options.Log.Debug(s.ctx, "shutting down rink")
	s.cancel()
	<-s.finished
}

func (s *Rink) run(ctx context.Context) {
	s.options.Log.Debug(ctx, "running rink")
	defer s.options.Log.Debug(ctx, "stopped rink")

	defer close(s.finished)
	for ctx.Err() == nil {
		err := s.runOnce(ctx)
		if err != nil && !errors.IsAny(err, context.Canceled) {
			s.options.Log.Error(ctx, errors.Wrap(err, "running rink"))
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
	}
}

func (s *Rink) newSession() (*concurrency.Session, error) {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()
	if s.currentSession != nil {
		panic("existing session when creating new one")
	}

	sess, err := concurrency.NewSession(s.cli)
	if err != nil {
		return nil, errors.Wrap(err, "new etcd session")
	}
	s.currentSession = sess

	return s.currentSession, nil
}

func (s *Rink) clearSession() {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()

	if s.currentSession == nil {
		panic("session already cleared")
	}
	err := s.currentSession.Close()
	if err != nil {
		s.options.Log.Error(s.ctx, errors.Wrap(err, "close session"))
	}
	s.currentSession = nil
}

func (s *Rink) runOnce(ctx context.Context) error {
	s.options.Log.Debug(ctx, "creating new session")
	sess, err := s.newSession()
	if err != nil {
		return err
	}
	s.options.Log.Debug(ctx, "created session", j.KV("etcd_lease", sess.Lease()))

	defer s.clearSession()
	defer s.Roles.unlockAll()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return Run(ctx, sess, s.name, s.Roles.updateRank, s.options.ClusterOptions)
	})

	eg.Go(func() error {
		return watchSession(ctx, sess)
	})

	return eg.Wait()
}

// rinkDelegate is a hidden type to satisfy RoleDelegate
// without exposing methods on Rink
type rinkDelegate struct {
	*Rink
}

func (s rinkDelegate) Create(role string) (RoleContext, error) {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()
	if s.currentSession == nil {
		return nil, errors.New("session finished")
	}
	return NewLockedContext(s.ctx, s.currentSession, s.name, role), nil
}

func (s rinkDelegate) Assign(rank Rank, role string) bool {
	if !rank.HaveRank {
		return false
	}
	roleRank := s.options.AssignRoleFunc(role, rank.Size)
	if roleRank < 0 {
		return false
	}
	return rank.MyRank == roleRank
}

func watchSession(ctx context.Context, sess *concurrency.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sess.Done():
		return errors.New("etcd lease expired")
	}
}
