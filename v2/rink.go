package rink

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/errgroup"
)

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, ...log.Option) {}
func (noopLogger) Info(context.Context, string, ...log.Option)  {}
func (noopLogger) Error(context.Context, error, ...log.Option)  {}

type options struct {
	Log log.Interface

	ClusterOptions ClusterOptions

	RolesOptions RolesOptions
}

type Option func(*options)

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
func WithLogger(l log.Interface) Option {
	return func(o *options) {
		o.Log = l
	}
}

func buildOptions(opts []Option) options {
	var o options
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
	s.Roles = NewRoles(name, s.options.RolesOptions)

	go s.run(ctx)
	return s
}

// Shutdown will synchronously release all locked roles, shutdown the cluster, and
// revoke the current Session.
func (s *Rink) Shutdown(ctx context.Context) {
	s.cancel()
	s.options.Log.Debug(s.ctx, "shutting down rink")
	select {
	case <-s.finished:
	case <-ctx.Done():
	}
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

func (s *Rink) runOnce(ctx context.Context) error {
	s.options.Log.Debug(ctx, "creating new session")
	sess, err := concurrency.NewSession(s.cli)
	if err != nil {
		return err
	}
	s.options.Log.Debug(ctx, "created session", j.KV("etcd_lease", sess.Lease()))
	defer s.options.Log.Debug(ctx, "ended session", j.KV("etcd_lease", sess.Lease()))

	defer func() {
		err := sess.Close()
		if err != nil {
			// NoReturnErr: Nowhere to return it
			s.options.Log.Error(ctx, errors.Wrap(err, "session close"))
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	ctx = clientv3.WithRequireLeader(ctx)

	eg.Go(func() error {
		return Run(ctx, sess, s.name, s.Roles.updateRank, s.options.ClusterOptions)
	})

	eg.Go(func() error {
		return watchSession(ctx, sess)
	})

	eg.Go(func() error {
		return watchForExpiredKeys(ctx, s.cli, s.name)
	})

	eg.Go(func() error {
		return s.Roles.assignRoles(ctx, sess)
	})

	return eg.Wait()
}

func watchSession(ctx context.Context, sess *concurrency.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sess.Done():
		return errors.New("etcd lease expired")
	}
}
