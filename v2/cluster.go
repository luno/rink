package rink

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/errgroup"
)

var ErrMemberAlreadyExists = errors.New("member key already exists", j.C("ERR_48c09c42047da4bf"))

type ClusterOptions struct {
	// MemberName is the name of this member of the cluster, every member
	// of the cluster must have a unique name. If not provided, we
	// will use a random identifier for the lifetime of this cluster.
	MemberName string
	// NewMemberWait is the amount of time to wait to add new roles
	// for new members. The reason we don't add new members immediately
	// is that we would prefer to replace leaving members.
	NewMemberWait time.Duration

	// NotifyLeader is called when we either become leader or lose leadership
	NotifyLeader func(cluster string, member string, leader bool)
	// NotifyRank is called when our rank is updated
	NotifyRank func(cluster string, member string, rank Rank)

	// Log will log out messages and errors on cluster activity
	Log log.Interface

	// electionKey will contain the elected cluster state as
	// an encoded blob
	electionKey string

	// memberKey is set to signal membership in the cluster
	// it is associated with a lease so that if the client
	// dies, the key is removed. The value should be the
	// (unix) time at which this member joined the cluster so
	// that re-balancing can be calculated.
	memberKey string

	// memberKeyPrefix is used to search etcd for all the
	// members of the cluster
	memberKeyPrefix string
}

func validateOptions(name string, o *ClusterOptions) error {
	if name == "" {
		return errors.New("invalid cluster name")
	}
	if o.MemberName == "" {
		o.MemberName = fmt.Sprintf("%x", rand.Int63())
	}
	if o.NewMemberWait == 0 {
		o.NewMemberWait = time.Minute
	}
	if o.Log == nil {
		o.Log = noopLogger{}
	}
	if o.NotifyLeader == nil {
		o.NotifyLeader = func(string, string, bool) {}
	}
	if o.NotifyRank == nil {
		o.NotifyRank = func(string, string, Rank) {}
	}
	o.electionKey = path.Join(name, "election")
	o.memberKey = path.Join(name, "members", o.MemberName)
	o.memberKeyPrefix = path.Join(name, "members") + "/"
	return nil
}

// Run runs a distributed cluster
// ClusterName is the name of the cluster which you will
// be a part of. There should be many instances which share
// a ClusterName. For instance there might be 5 pods called
// "user-service" and they might be in a cluster called "users".
func Run(ctx context.Context,
	sess *concurrency.Session,
	name string,
	rankHandler func(context.Context, Rank),
	o ClusterOptions,
) error {
	if err := validateOptions(name, &o); err != nil {
		return err
	}
	if err := putMemberKey(ctx, sess, o.memberKey); err != nil {
		return err
	}
	o.Log.Debug(ctx, "joining cluster", j.KV("member", o.memberKey))
	c := &cluster{
		RankHandler: rankHandler,
		Options:     o,
		Election:    concurrency.NewElection(sess, o.electionKey),
		Session:     sess,
	}
	return c.runCluster(ctx)
}

func putMemberKey(ctx context.Context, sess *concurrency.Session, key string) error {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	// If the key doesn't exist, we'll put it with our lease
	put := clientv3.OpPut(key, ts, clientv3.WithLease(sess.Lease()))
	// If it does exist, let's get it, so we can see who owns it
	get := clientv3.OpGet(key)
	resp, err := sess.Client().Txn(ctx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		return errors.Wrap(err, "put member key")
	}
	if !resp.Succeeded {
		owner := resp.Responses[0].GetResponseRange().Kvs[0].Lease
		return errors.Wrap(ErrMemberAlreadyExists, "", j.MKV{
			"owner_lease": owner,
			"member_key":  key,
			"my_lease":    sess.Lease(),
		})
	}
	return nil
}

type ranks map[string]int32

type cluster struct {
	RankHandler func(context.Context, Rank)
	Name        string
	Options     ClusterOptions
	Election    *concurrency.Election
	Session     *concurrency.Session

	ranks        ranks
	rankRevision int64
	rankMutex    sync.Mutex
}

func (c *cluster) getRanks() ranks {
	c.rankMutex.Lock()
	defer c.rankMutex.Unlock()
	return c.ranks
}

type Rank struct {
	MyRank   int32
	HaveRank bool
	Size     int32
}

func findMe(r ranks, me string) Rank {
	size := int32(len(r))
	myRank, hasRank := r[me]
	return Rank{myRank, hasRank, size}
}

func (c *cluster) setRanks(resp *clientv3.GetResponse) (bool, error) {
	c.rankMutex.Lock()
	defer c.rankMutex.Unlock()
	if resp.Header.Revision <= c.rankRevision {
		return false, nil
	}
	val := resp.Kvs[0].Value
	if string(val) == "" {
		return false, nil
	}
	var r ranks
	err := gob.NewDecoder(bytes.NewBuffer(val)).Decode(&r)
	if err != nil {
		return false, errors.Wrap(err, "rank state decode")
	}
	c.ranks = r
	return true, nil
}

func (c *cluster) runCluster(ctx context.Context) error {
	// Check the current state
	resp, err := c.Election.Leader(ctx)
	if errors.Is(err, concurrency.ErrElectionNoLeader) {
		// NoReturnErr: No leader so don't need to initialise state
	} else if err != nil {
		return err
	} else {
		_, err := c.setRanks(resp)
		if err != nil {
			return err
		}
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.maybeLeadElection(ctx)
	})
	g.Go(func() error {
		return c.observeElection(ctx)
	})
	return g.Wait()
}

func (c *cluster) changedRank(ctx context.Context, r Rank) {
	c.RankHandler(ctx, r)
	c.Options.NotifyRank(c.Name, c.Options.MemberName, r)
}

func (c *cluster) observeElection(ctx context.Context) error {
	c.Options.Log.Debug(ctx, "observing etcd election")
	defer c.Options.Log.Debug(ctx, "stopped observing etcd election")

	rank := findMe(c.getRanks(), c.Options.MemberName)
	c.changedRank(ctx, rank)
	defer c.changedRank(ctx, Rank{})

	for r := range c.Election.Observe(ctx) {
		set, err := c.setRanks(&r)
		if err != nil {
			return err
		}
		if set {
			newRank := findMe(c.getRanks(), c.Options.MemberName)
			if newRank == rank {
				continue
			}
			rank = newRank
			c.changedRank(ctx, rank)
		}
	}
	return ctx.Err()
}

func (c *cluster) maybeLeadElection(ctx context.Context) error {
	c.Options.Log.Debug(ctx, "maybe leading etcd election")
	defer c.Options.Log.Debug(ctx, "stopped maybe leading etcd election")

	for ctx.Err() == nil {
		if err := c.Election.Campaign(ctx, ""); err != nil {
			return errors.Wrap(err, "election campaign")
		}
		err := c.leadElection(ctx)
		if err != nil {
			return errors.Wrap(err, "lead election")
		}
	}
	return ctx.Err()
}

func (c *cluster) leadElection(ctx context.Context) error {
	c.Options.Log.Debug(ctx, "leading election")
	c.Options.NotifyLeader(c.Name, c.Options.MemberName, true)
	defer c.Options.Log.Debug(ctx, "stopped leading election")
	defer c.Options.NotifyLeader(c.Name, c.Options.MemberName, false)

	defer func() {
		err := c.Election.Resign(ctx)
		if err != nil {
			// NoReturnErr: Log
			c.Options.Log.Error(ctx, errors.Wrap(err, "resign"))
		}
	}()

	r := c.getRanks()

	watchChan := c.Session.Client().Watch(ctx, c.Options.memberKeyPrefix, clientv3.WithPrefix())
	refresh := time.NewTimer(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Session.Done():
			return nil
		case resp := <-watchChan:
			if resp.Err() != nil {
				return resp.Err()
			}
			if !anyCreateOrDelete(resp.Events) {
				continue
			}
			c.Options.Log.Debug(ctx, "received cluster member changes")
		case <-refresh.C:
			refresh.Reset(time.Minute)
		}

		mem, err := listMembers(ctx, c.Session.Client(), c.Options.memberKeyPrefix)
		if err != nil {
			return err
		}
		changes := getMemberChanges(mem, r, time.Now(), c.Options.NewMemberWait)

		if len(changes.Waiting) > 0 {
			nextMember := changes.Waiting[0]
			balanceAt := mem[nextMember].Add(c.Options.NewMemberWait)
			c.Options.Log.Debug(ctx, "next balance", j.KV("at", balanceAt))

			if !refresh.Stop() {
				<-refresh.C
			}
			refresh.Reset(time.Until(balanceAt))
		}

		if len(changes.Remained) == len(r) && len(changes.Added) == 0 {
			continue
		}

		r = getNewRanks(r, changes)
		c.Options.Log.Debug(ctx, "publishing state", j.KV("state", fmt.Sprintf("%+v", r)))

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(r); err != nil {
			return err
		}

		err = c.Election.Proclaim(ctx, buf.String())
		if errors.Is(err, concurrency.ErrElectionNoLeader) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func listMembers(ctx context.Context, client *clientv3.Client, prefix string) (map[string]time.Time, error) {
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "etcd get members")
	}

	ret := make(map[string]time.Time, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		ts, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "invalid member value", j.MKV{
				"key":   string(kv.Key),
				"value": string(kv.Value),
			})
		}
		mem := strings.TrimPrefix(string(kv.Key), prefix)
		ret[mem] = time.UnixMilli(ts)
	}
	return ret, nil
}

func anyCreateOrDelete(events []*clientv3.Event) bool {
	for _, ev := range events {
		if !ev.IsModify() {
			return true
		}
	}
	return false
}
