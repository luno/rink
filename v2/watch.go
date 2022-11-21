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
const monitorInterval = 5 * time.Minute

func knownLeases(ctx context.Context, cli *clientv3.Client) (map[clientv3.LeaseID]int64, error) {
	leases, err := cli.Lease.Leases(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "leases")
	}
	ret := make(map[clientv3.LeaseID]int64)
	for _, l := range leases.Leases {
		ttlResp, err := cli.Lease.TimeToLive(ctx, l.ID)
		if err != nil {
			return nil, err
		}
		ret[l.ID] = ttlResp.TTL
		if ttlResp.TTL == -1 {
			log.Info(ctx, "found expired lease", j.KV("lease_id", l.ID))
		}
	}
	log.Info(ctx, "found leases", j.KV("lease_count", len(ret)))
	return ret, nil
}

func scanForExpiredKeys(ctx context.Context, cli *clientv3.Client,
	prefix string,
	leases map[clientv3.LeaseID]int64,
) (map[string]clientv3.LeaseID, error) {
	var rev int64
	expired := make(map[string]clientv3.LeaseID)
	var scanned int
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
		scanned += len(resp.Kvs)
		for _, kv := range resp.Kvs {
			rev = kv.CreateRevision + 1
			keyTTL := leases[clientv3.LeaseID(kv.Lease)]
			if keyTTL <= 0 {
				expired[string(kv.Key)] = clientv3.LeaseID(kv.Lease)
			}
		}

		if !resp.More {
			break
		}
	}
	log.Info(ctx, "scanned keys", j.KV("count", scanned))
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
			err := errors.New("found an expired key", j.MKV{
				"key": key, "lease_id": lease,
			})
			log.Error(ctx, err)
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
