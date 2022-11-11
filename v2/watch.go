package rink

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const etcdScanPage = 1000
const monitorInterval = time.Minute

func knownLeases(ctx context.Context, cli *clientv3.Client) (map[clientv3.LeaseID]time.Time, error) {
	leases, err := cli.Lease.Leases(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "leases")
	}
	ret := make(map[clientv3.LeaseID]time.Time)
	for _, l := range leases.Leases {
		ttlResp, err := cli.Lease.TimeToLive(ctx, l.ID)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		var expiresAt time.Time
		if ttlResp.TTL >= 0 {
			expiresAt = now.Add(time.Duration(ttlResp.TTL) * time.Second)
		} else if ttlResp.TTL == -1 {
			log.Info(ctx, "found expired lease", j.KV("lease_id", l.ID))
		}
		ret[l.ID] = expiresAt
	}
	return ret, nil
}

func scanForExpiredKeys(ctx context.Context, cli *clientv3.Client,
	prefix string,
	leases map[clientv3.LeaseID]time.Time,
) (map[string]clientv3.LeaseID, error) {
	var rev int64
	expired := make(map[string]clientv3.LeaseID)
	for {
		resp, err := cli.Get(ctx, prefix,
			clientv3.WithMinCreateRev(rev),
			clientv3.WithPrefix(),
			clientv3.WithLimit(etcdScanPage),
			clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		)
		if err != nil {
			return nil, errors.Wrap(err, "get")
		}

		for _, kv := range resp.Kvs {
			rev = kv.CreateRevision + 1
			if kv.Lease == 0 {
				continue
			}
			if leases[clientv3.LeaseID(kv.Lease)].IsZero() {
				expired[string(kv.Key)] = clientv3.LeaseID(kv.Lease)
			}
		}

		if !resp.More {
			break
		}
	}
	return expired, nil
}

type keyHistory struct {
	lastExpired map[string]clientv3.LeaseID
}

func (h *keyHistory) checkForExpiredKeys(ctx context.Context, cli *clientv3.Client, prefix string) error {
	leases, err := knownLeases(ctx, cli)
	if err != nil {
		return err
	}
	expired, err := scanForExpiredKeys(ctx, cli, prefix, leases)
	if err != nil {
		return err
	}
	// Check if keys were expired last time (with the same lease)
	// to avoid issues with temporary consistency
	for key, lease := range expired {
		if lease == h.lastExpired[key] {
			log.Info(ctx, "found an expired key", j.MKV{
				"key": key, "lease_id": lease,
			})
		}
	}
	h.lastExpired = expired
	return nil
}

func watchForExpiredKeys(ctx context.Context, cli *clientv3.Client, prefix string) error {
	ti := time.NewTicker(monitorInterval)
	defer ti.Stop()

	var history keyHistory

	for {
		select {
		case <-ti.C:
			err := history.checkForExpiredKeys(ctx, cli, prefix)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error(ctx, errors.Wrap(err, "checking for expired keys"))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
