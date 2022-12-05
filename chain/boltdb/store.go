package boltdb

import (
	"context"
	"io"
	"path"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/drand/drand/chain"
	chainerrors "github.com/drand/drand/chain/errors"
	"github.com/drand/drand/log"
)

// BoltStore implements the Store interface using the kv storage boltdb (native
// golang implementation). Internally, Beacons are stored as JSON-encoded in the
// db file.
//
//nolint:gocritic// We do want to have a mutex here
type BoltStore struct {
	sync.Mutex
	db *bolt.DB

	log log.Logger

	requiresPrevious bool
}

var beaconBucket = []byte("beacons")

// BoltFileName is the name of the file boltdb writes to
const BoltFileName = "drand.db"

// BoltStoreOpenPerm is the permission we will use to read bolt store file from disk
const BoltStoreOpenPerm = 0660

// NewBoltStore returns a Store implementation using the boltdb storage engine.
func NewBoltStore(ctx context.Context, l log.Logger, folder string, opts *bolt.Options) (*BoltStore, error) {
	dbPath := path.Join(folder, BoltFileName)
	db, err := bolt.Open(dbPath, BoltStoreOpenPerm, opts)
	if err != nil {
		return nil, err
	}
	// create the bucket already
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(beaconBucket)
		return err
	})

	return &BoltStore{
		log: l,
		db:  db,

		requiresPrevious: chain.PreviousRequiredFromContext(ctx),
	}, err
}

// Len performs a big scan over the bucket and is _very_ slow - use sparingly!
func (b *BoltStore) Len(context.Context) (int, error) {
	var length = 0
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		// this `.Stats()` call is the particularly expensive one!
		length = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		b.log.Warnw("", "boltdb", "error getting length", "err", err)
	}
	return length, err
}

func (b *BoltStore) Close(context.Context) error {
	err := b.db.Close()
	if err != nil {
		b.log.Errorw("", "boltdb", "close", "err", err)
	}
	return err
}

// Put implements the Store interface. WARNING: It does NOT verify that this
// beacon is not already saved in the database or not and will overwrite it.
func (b *BoltStore) Put(_ context.Context, beacon *chain.Beacon) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		key := chain.RoundToBytes(beacon.Round)
		err := bucket.Put(key, beacon.Signature)
		if err != nil {
			b.log.Errorw("storing beacon", "round", beacon.Round, "err", err)
		}
		return err
	})
}

// Last returns the last beacon signature saved into the db
func (b *BoltStore) Last(context.Context) (*chain.Beacon, error) {
	beacon := &chain.Beacon{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		cursor := bucket.Cursor()
		b, err := b.getCursorBeacon(bucket, cursor.Last)
		if err != nil {
			return err
		}

		*beacon = *b
		return nil
	})
	return beacon, err
}

// Get returns the beacon saved at this round
func (b *BoltStore) Get(_ context.Context, round uint64) (*chain.Beacon, error) {
	beacon := &chain.Beacon{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		b, err := b.getBeacon(bucket, round, true)
		if err != nil {
			return err
		}

		*beacon = *b
		return nil
	})
	return beacon, err
}

func (b *BoltStore) Del(_ context.Context, round uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		return bucket.Delete(chain.RoundToBytes(round))
	})
}

func (b *BoltStore) Cursor(ctx context.Context, fn func(context.Context, chain.Cursor) error) error {
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(beaconBucket)
		c := bucket.Cursor()
		return fn(ctx, &boltCursor{Cursor: c, store: b})
	})
	if err != nil {
		b.log.Errorw("", "boltdb", "error getting cursor", "err", err)
	}
	return err
}

// SaveTo saves the bolt database to an alternate file.
func (b *BoltStore) SaveTo(_ context.Context, w io.Writer) error {
	return b.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

type boltCursor struct {
	*bolt.Cursor
	store *BoltStore
}

func (c *boltCursor) First(context.Context) (*chain.Beacon, error) {
	return c.store.getCursorBeacon(c.Bucket(), c.Cursor.First)
}

func (c *boltCursor) Next(context.Context) (*chain.Beacon, error) {
	return c.store.getCursorBeacon(c.Bucket(), c.Cursor.Next)
}

func (c *boltCursor) Seek(_ context.Context, round uint64) (*chain.Beacon, error) {
	_, v := c.Cursor.Seek(chain.RoundToBytes(round))
	if v == nil {
		return nil, chainerrors.ErrNoBeaconStored
	}

	b := chain.Beacon{
		Round:     round,
		Signature: v,
	}

	if c.store.requiresPrevious &&
		b.Round > 0 {
		prevBeacon, err := c.store.getBeacon(c.Bucket(), b.Round-1, false)
		if err != nil {
			c.store.log.Errorw("missing previous beacon from database", "round", b.Round-1, "err", err)
			return nil, chainerrors.ErrNoBeaconStored
		}
		b.PreviousSig = prevBeacon.Signature
	}

	return &b, nil
}

func (c *boltCursor) Last(context.Context) (*chain.Beacon, error) {
	return c.store.getCursorBeacon(c.Bucket(), c.Cursor.Last)
}

type beaconCursorGetter func() (key []byte, value []byte)

func (b *BoltStore) getBeacon(bucket *bolt.Bucket, round uint64, canFetchPrevious bool) (*chain.Beacon, error) {
	sig := bucket.Get(chain.RoundToBytes(round))
	if sig == nil {
		return nil, chainerrors.ErrNoBeaconStored
	}

	beacon := chain.Beacon{
		Round:     round,
		Signature: sig,
	}

	if canFetchPrevious &&
		b.requiresPrevious &&
		beacon.Round > 0 {
		prevSig := bucket.Get(chain.RoundToBytes(round - 1))
		if prevSig == nil {
			b.log.Errorw("missing previous beacon from database", "round", beacon.Round-1)
			return nil, chainerrors.ErrNoBeaconStored
		}
		beacon.PreviousSig = prevSig
	}

	return &beacon, nil
}

func (b *BoltStore) getCursorBeacon(bucket *bolt.Bucket, get beaconCursorGetter) (*chain.Beacon, error) {
	key, sig := get()
	if sig == nil {
		return nil, chainerrors.ErrNoBeaconStored
	}

	beacon := chain.Beacon{
		Round:     chain.BytesToRound(key),
		Signature: sig,
	}

	if b.requiresPrevious &&
		beacon.Round > 0 {
		prevBeacon, err := b.getBeacon(bucket, beacon.Round-1, false)
		if err != nil {
			return nil, err
		}
		if prevBeacon == nil {
			b.log.Errorw("missing previous beacon from database", "round", beacon.Round-1)
			return nil, chainerrors.ErrNoBeaconStored
		}
		beacon.PreviousSig = prevBeacon.Signature
	}

	return &beacon, nil
}
