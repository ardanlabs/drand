package boltdb

import (
	"context"
	"io"
	"path"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/chain/errors"
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
}

var beaconBucket = []byte("beacons")

// BoltFileName is the name of the file boltdb writes to
const BoltFileName = "drand.db"

// BoltStoreOpenPerm is the permission we will use to read bolt store file from disk
const BoltStoreOpenPerm = 0660

// NewBoltStore returns a Store implementation using the boltdb storage engine.
func NewBoltStore(l log.Logger, folder string, opts *bolt.Options) (*BoltStore, error) {
	if opts == nil {
		opts = bolt.DefaultOptions
	}
	// We need to check the timeout here.
	// If Bolt cannot open the file, it will stay in a loop until it can or the timeout is reached.
	// When the timeout is set to 0, it will wait infinitely.
	// That's why we want to set a reasonable timeout.
	// If the timeout of 2 seconds is too small, then the storage layer will likely perform poorly
	// for all other operations too.

	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second
	}

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
	}, err
}

// Len performs a big scan over the bucket and is _very_ slow - use sparingly!
func (b *BoltStore) Len(ctx context.Context) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var length = 0
	err := b.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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

func (b *BoltStore) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := b.db.Close()
	if err != nil {
		b.log.Errorw("", "boltdb", "close", "err", err)
	}
	return err
}

// Put implements the Store interface. WARNING: It does NOT verify that this
// beacon is not already saved in the database or not and will overwrite it.
func (b *BoltStore) Put(ctx context.Context, beacon *chain.Beacon) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := b.db.Update(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bucket := tx.Bucket(beaconBucket)
		key := chain.RoundToBytes(beacon.Round)
		buff, err := beacon.Marshal()
		if err != nil {
			return err
		}
		return bucket.Put(key, buff)
	})
	return err
}

// Last returns the last beacon signature saved into the db
func (b *BoltStore) Last(ctx context.Context) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	beacon := &chain.Beacon{}
	err := b.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bucket := tx.Bucket(beaconBucket)
		cursor := bucket.Cursor()
		_, v := cursor.Last()
		if v == nil {
			return errors.ErrNoBeaconStored
		}
		return beacon.Unmarshal(v)
	})
	return beacon, err
}

// Get returns the beacon saved at this round
func (b *BoltStore) Get(ctx context.Context, round uint64) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	beacon := &chain.Beacon{}
	err := b.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bucket := tx.Bucket(beaconBucket)
		v := bucket.Get(chain.RoundToBytes(round))
		if v == nil {
			return errors.ErrNoBeaconStored
		}
		return beacon.Unmarshal(v)
	})
	return beacon, err
}

func (b *BoltStore) Del(ctx context.Context, round uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bucket := tx.Bucket(beaconBucket)
		return bucket.Delete(chain.RoundToBytes(round))
	})
}

func (b *BoltStore) Cursor(ctx context.Context, fn func(context.Context, chain.Cursor) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := b.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bucket := tx.Bucket(beaconBucket)
		c := bucket.Cursor()
		return fn(ctx, &boltCursor{Cursor: c})
	})
	if err != nil {
		log.DefaultLogger().Warnw("", "boltdb", "error getting cursor", "err", err)
	}
	return err
}

// SaveTo saves the bolt database to an alternate file.
func (b *BoltStore) SaveTo(ctx context.Context, w io.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return b.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := tx.WriteTo(w)
		return err
	})
}

type boltCursor struct {
	*bolt.Cursor
}

func (c *boltCursor) First(ctx context.Context) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	k, v := c.Cursor.First()
	if k == nil {
		return nil, errors.ErrNoBeaconStored
	}
	b := &chain.Beacon{}
	err := b.Unmarshal(v)
	return b, err
}

func (c *boltCursor) Next(ctx context.Context) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	k, v := c.Cursor.Next()
	if k == nil {
		return nil, errors.ErrNoBeaconStored
	}
	b := &chain.Beacon{}
	err := b.Unmarshal(v)
	return b, err
}

func (c *boltCursor) Seek(ctx context.Context, round uint64) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	k, v := c.Cursor.Seek(chain.RoundToBytes(round))
	if k == nil {
		return nil, errors.ErrNoBeaconStored
	}
	b := &chain.Beacon{}
	err := b.Unmarshal(v)
	return b, err
}

func (c *boltCursor) Last(ctx context.Context) (*chain.Beacon, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	k, v := c.Cursor.Last()
	if k == nil {
		return nil, errors.ErrNoBeaconStored
	}
	b := &chain.Beacon{}
	err := b.Unmarshal(v)
	return b, err
}
