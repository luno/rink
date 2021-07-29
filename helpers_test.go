package rink_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"google.golang.org/grpc"
)

// This was copied from go.etcd.io/etcd/clientv3/concurrency/main_test.go

var endpoints []string

func setupSession(t *testing.T) *concurrency.Session {
	cl, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},

		// See https://github.com/etcd-io/etcd/issues/9877
		// Dialing hangs without error if the v3 endpoint is unavailable.
		DialTimeout: 1 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if errors.Is(err, context.DeadlineExceeded) {
		t.Skip("Couldn't connect to local etcd v3 instance, skipping...")
	}
	require.NoError(t, err)

	t.Cleanup(func() {
		err := cl.Close()
		require.NoError(t, err)
	})

	s, err := concurrency.NewSession(cl)
	require.NoError(t, err)
	t.Cleanup(func() {
		select {
		case <-s.Done():
			return
		default:
		}

		err = s.Close()
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// Session closed during test.
			return
		}
		require.NoError(t, err)
	})

	return s
}

func rando(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestUnlockErrors(t *testing.T) {
	s := setupSession(t)
	m := concurrency.NewMutex(s, rando("test"))

	err := m.Lock(context.TODO())
	require.NoError(t, err)

	err = m.Unlock(context.TODO())
	require.NoError(t, err)

	// Unlock is idempotent
	err = m.Unlock(context.TODO())
	require.NoError(t, err)

	err = m.Lock(context.TODO())
	require.NoError(t, err)

	require.NoError(t, s.Close())

	// Unlock is idempotent even if underlying session closed.
	err = m.Unlock(context.TODO())
	require.NoError(t, err)
}

func TestCloseErrors(t *testing.T) {
	s := setupSession(t)

	err := s.Close()
	require.NoError(t, err)

	// Duplicate close returns ErrLeaseNotFound
	err = s.Close()
	jtest.Require(t, rpctypes.ErrLeaseNotFound, err)
}
