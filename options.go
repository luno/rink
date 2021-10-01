package rink

import (
	"context"
	"hash"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/luno/jettison"
	"github.com/luno/jettison/log"
	"go.etcd.io/etcd/client/v3"
)

const defaultRebalanceDelay = time.Second * 60

var defaultHasher = fnv.New64a

type hasher func() hash.Hash64

type Option func(*options)

// WithName provides an option to override the default
// session name of the LeaseID.
func WithName(name string) Option {
	return func(o *options) {
		o.name = name
	}
}

// WithRebalanceDelay returns an option to override the default
// rebalance delay of 60 secs. Rebalance delay specifies how long
// this member can wait on startup before a rank must be assigned to it.
// A rank may be assigned to it earlier if another member leaves.
func WithRebalanceDelay(d time.Duration) Option {
	return func(o *options) {
		o.rebalanceDelay = d
	}
}

// WithHash returns an option to override the default
// 64-bit FNV-1a hash used to convert roles into keys
// for the consistent hash function.
func WithHash(hasher hasher) Option {
	return func(o *options) {
		o.hasher = hasher
	}
}

// WithLogger returns an option to override the default
// jettison logger.
func WithLogger(logger logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// WithContext returns an option to override the default
// background context.
func WithContext(ctx context.Context) Option {
	return func(o *options) {
		o.ctx = ctx
	}
}

type options struct {
	name           string
	rebalanceDelay time.Duration
	hasher         hasher
	logger         logger
	ctx            context.Context
}

func defaultOptions(leaseID clientv3.LeaseID) *options {
	return &options{
		name:           strconv.FormatInt(int64(leaseID), 10),
		rebalanceDelay: defaultRebalanceDelay,
		hasher:         defaultHasher,
		logger:         jlogger{},
		ctx:            context.Background(),
	}
}

type logger interface {
	Debug(context.Context, string, ...jettison.Option)
	Info(context.Context, string, ...jettison.Option)
	Error(context.Context, error)
}

type jlogger struct{}

func (jlogger) Debug(ctx context.Context, msg string, opts ...jettison.Option) {
	opts = append(opts, log.WithLevel(log.LevelDebug))
	log.Info(ctx, msg, opts...)
}

func (jlogger) Info(ctx context.Context, msg string, opts ...jettison.Option) {
	log.Info(ctx, msg, opts...)
}

func (jlogger) Error(ctx context.Context, err error) {
	log.Error(ctx, err)
}
