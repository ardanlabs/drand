package beacon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/chain/boltdb"
	"github.com/drand/drand/test/testlogger"
)

func TestStoreCallback(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	l := testlogger.New(t)
	bbstore, err := boltdb.NewBoltStore(l, dir, nil)
	require.NoError(t, err)
	cb := NewCallbackStore(ctx, bbstore)
	id1 := "superid"
	doneCh := make(chan bool, 1)
	cb.AddCallback(id1, func(b *chain.Beacon) {
		doneCh <- true
	})

	err = cb.Put(ctx, &chain.Beacon{
		Round: 1,
	})
	require.NoError(t, err)
	require.True(t, checkOne(doneCh))
	cb.AddCallback(id1, func(*chain.Beacon) {})
	err = cb.Put(ctx, &chain.Beacon{
		Round: 1,
	})
	require.NoError(t, err)
	require.False(t, checkOne(doneCh))

	cb.RemoveCallback(id1)
	require.False(t, checkOne(doneCh))
}

func checkOne(ch chan bool) bool {
	select {
	case <-ch:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}
