package migration

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	json "github.com/nikkolasg/hexjson"
	"go.etcd.io/bbolt"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/chain/postgresdb/database"
	"github.com/drand/drand/chain/postgresdb/pgdb"
	"github.com/drand/drand/chain/postgresdb/schema"
	"github.com/drand/drand/log"
)

type (
	beacon struct {
		PreviousSig []byte `json:",omitempty"`
		Round       uint64
		Signature   []byte
	}

	migrator struct {
		ctx       context.Context
		ctxCancel context.CancelFunc

		startedAt time.Time
		logger    log.Logger

		pgDSN            string
		bufferSize       int
		beaconName       string
		sourceBeaconPath string

		// existingDB   *bbolt.DB
		existingRows int

		distChan    chan beacon
		errChan     chan error
		destination chain.StorageType
	}
)

var (
	// ErrMigrationNotNeeded ...
	ErrMigrationNotNeeded = fmt.Errorf("migration not needed")

	bucketName = []byte("beacons")
)

//nolint:lll // This function uses the right amount of params
func Migrate(ctx context.Context, logger log.Logger, sourceBeaconPath, beaconName string, destination chain.StorageType, pgDSN string, bufferSize int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	startedAt := time.Now()

	if err := shouldMigrate(ctx, logger, sourceBeaconPath, beaconName, destination, pgDSN); err != nil {
		logger.Warnw("decided storage format migration is not needed", "err", err)
		if errors.Is(err, ErrMigrationNotNeeded) {
			return err
		}

		return nil
	}

	switch {
	case bufferSize < 0:
		logger.Infow("buffer size not specified, defaulting to 10000")
		bufferSize = 10_000
	case bufferSize == 0:
		var err error
		bufferSize, err = automaticBufferSize(logger, sourceBeaconPath)
		if err != nil {
			return err
		}
	case bufferSize <= 10_000:
		logger.Warnw("buffer size seems a bit too small. The migration process might be slow", "bufferSize", bufferSize)
	case bufferSize > 10_000_000:
		//nolint:lll // This line has the right amount of chars
		logger.Warnw("buffer size seems a bit too large. Make sure your system can allocate enough system memory for this", "bufferSize", bufferSize)
	}

	m := migrator{
		logger:    logger,
		startedAt: startedAt,

		pgDSN:            pgDSN,
		bufferSize:       bufferSize,
		beaconName:       beaconName,
		sourceBeaconPath: sourceBeaconPath,
		destination:      destination,

		ctx:       ctx,
		ctxCancel: cancel,

		distChan: make(chan beacon, bufferSize),

		errChan: make(chan error),
	}

	return m.doMigrate(ctx)
}

func automaticBufferSize(logger log.Logger, sourceBeaconPath string) (int, error) {
	logger.Warnw("buffer size set to 0. Running automatic buffer inference. Make sure you have enough system memory for this")
	existingDB, err := bbolt.Open(sourceBeaconPath, 0600, nil)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = existingDB.Close()
	}()

	var bufferSize = 0

	err = existingDB.View(func(tx *bbolt.Tx) error {
		bufferSize = tx.Bucket(bucketName).Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}

	logger.Infow("buffer size automatically inferred from existing DB", "bufferSize", bufferSize)

	return bufferSize, nil
}

//nolint:lll // This function has the right amount of
func shouldMigrate(ctx context.Context, logger log.Logger, sourceBeaconPath, beaconName string, destination chain.StorageType, pgDSN string) error {
	//nolint:exhaustive // We want to explicitly ignore the chain.MemDB backend since there's nothing to migrate there.
	switch destination {
	case chain.BoltDB:
		return shouldMigrateBolt(sourceBeaconPath)
	case chain.PostgreSQL:
		return shouldMigratePostgres(ctx, logger, beaconName, pgDSN)
	default:
		return fmt.Errorf("unknown destination type %q for migration package", destination)
	}
}

func shouldMigrateBolt(sourceBeaconPath string) error {
	existingDB, err := bbolt.Open(sourceBeaconPath, 0600, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = existingDB.Close()
	}()

	return existingDB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		_, value := bucket.Cursor().First()
		b := beacon{}
		err := json.Unmarshal(value, &b)
		if err != nil {
			return ErrMigrationNotNeeded
		}

		return nil
	})
}

func pgConn(ctx context.Context, logger log.Logger, beaconName, pgDSN string) (*pgdb.Store, func(), error) {
	pgConf, err := database.ConfigFromDSN(pgDSN)
	if err != nil {
		return nil, nil, err
	}

	conn, err := database.Open(ctx, pgConf)
	if err != nil {
		return nil, nil, err
	}

	if err := schema.Migrate(ctx, conn); err != nil {
		err := fmt.Errorf("migrating error: %w", err)
		return nil, nil, err
	}

	store, err := pgdb.NewStore(ctx, logger, conn, beaconName)
	if err != nil {
		return nil, nil, err
	}

	cancel := func() {
		_ = store.Close(ctx)
		_ = conn.Close()
	}

	return store, cancel, err
}

func shouldMigratePostgres(ctx context.Context, logger log.Logger, beaconName, pgDSN string) error {
	store, cancel, err := pgConn(ctx, logger, beaconName, pgDSN)
	if err != nil {
		return err
	}
	defer cancel()

	storeLen, err := store.Len(ctx)
	if err != nil {
		return err
	}

	if storeLen != 0 {
		logger.Warnw("Postgres contains beacon rounds. Skipping migration", "rounds count", storeLen)
		return ErrMigrationNotNeeded
	}

	return nil
}

func (m *migrator) doMigrate(ctx context.Context) error {
	defer func() {
		finishedIn := time.Since(m.startedAt).String()
		m.logger.Infow(
			"Finished processing beacons",
			"beaconName", m.beaconName,
			"finishedIn", finishedIn,
		)
	}()

	//nolint:exhaustive // We want to explicitly ignore the chain.MemDB backend since there's nothing to migrate there.
	switch m.destination {
	case chain.BoltDB:
		go m.migrateBolt()
	case chain.PostgreSQL:
		go m.migratePostgres(ctx)
	}

	// Spin up the reader
	go m.reader()

	return m.wait()
}

//nolint:funlen // This function has the right length
func (m *migrator) migratePostgres(ctx context.Context) {
	rows := 0
	defer func() {
		if m.existingRows != rows {
			err := fmt.Errorf("not all rounds migrated successfully expected: %d actual: %d", m.existingRows, rows)
			m.errChan <- err
			return
		}
	}()
	defer func() {
		finishedIn := time.Since(m.startedAt).String()
		m.logger.Infow("finished saving data in postgres", "rows", rows, "finishedIn", finishedIn)
	}()
	defer m.ctxCancel()
	defer func() {
		if m.existingRows != rows {
			m.logger.Errorw("not all rounds migrated successfully", "expected", m.existingRows, "actually", rows)
			return
		}
	}()

	store, cancel, err := pgConn(ctx, m.logger, m.beaconName, m.pgDSN)
	if err != nil {
		m.errChan <- err
		return
	}
	defer cancel()

	buffSize := 0

	m.logger.Infow("dropping the FK from the table")
	err = store.DropFK(ctx)
	if err != nil {
		m.errChan <- err
		return
	}
	defer func() {
		m.logger.Infow("adding FK back to the table")
		err := store.AddFK(ctx)
		if err != nil {
			m.errChan <- err
			return
		}
	}()

	// Make sure we can still commit to the database
	// avoiding the error:
	//   pq: got 9229389 parameters but PostgreSQL only supports 65535 parameters
	pgBuffSize := m.bufferSize
	if pgBuffSize > 30_000 {
		pgBuffSize = 30_000
		m.logger.Warnw("buffer size automatically reconfigured for Postgres only", "bufferSize", pgBuffSize)
	}

	bs := make([]chain.Beacon, pgBuffSize)

	for val := range m.distChan {
		rows++
		b := chain.Beacon(val)

		bs[buffSize] = b
		buffSize++

		if buffSize == pgBuffSize {
			m.logger.Debugw("writing buffer contents to DB", "rows", rows)
			err := store.BatchPut(ctx, bs)
			if err != nil {
				m.logger.Errorw("while writing buffer contents to DB", "err", err)
				m.errChan <- err
				return
			}

			bs = make([]chain.Beacon, pgBuffSize)
			buffSize = 0
		}
	}

	if buffSize > 0 {
		m.logger.Infow("writing buffer contents to DB", "rows", buffSize)
		err := store.BatchPut(ctx, bs[:buffSize])
		if err != nil {
			m.logger.Errorw("while writing buffer contents to DB", "err", err)
			m.errChan <- err
			return
		}
	}
}

func (m *migrator) swapMigratedFile(newBeaconPath string, rows int) {
	if m.existingRows != rows {
		err := fmt.Errorf("not all rounds migrated successfully expected: %d actual: %d", m.existingRows, rows)
		m.errChan <- err
		return
	}

	m.logger.Infow("migrating BoltDB file")
	err := os.Rename(m.sourceBeaconPath, fmt.Sprintf("%s.old", m.sourceBeaconPath))
	if err != nil {
		m.errChan <- err
		return
	}
	err = os.Rename(newBeaconPath, m.sourceBeaconPath)
	if err != nil {
		m.errChan <- err
		return
	}
}

func (m *migrator) migrateBolt() {
	newBeaconPath := m.sourceBeaconPath + "-migrated"
	rows := 0
	defer func() {
		finishedIn := time.Since(m.startedAt).String()
		m.logger.Infow("finished saving data in boltdb", "rows", rows, "finishedIn", finishedIn)
	}()
	defer m.ctxCancel()

	// Swap migrated file with the old one.
	defer func() {
		m.swapMigratedFile(newBeaconPath, rows)
	}()

	db, err := bbolt.Open(newBeaconPath, 0600, nil)
	if err != nil {
		m.errChan <- err
		return
	}
	defer func() {
		_ = db.Close()
	}()

	db.MaxBatchSize = m.bufferSize

	err = db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		// We know this will be an append-only workload, it's safe to do it this way.
		bucket.FillPercent = 1.0

		newKey := make([]byte, 8)
		for val := range m.distChan {
			rows++

			binary.BigEndian.PutUint64(newKey, val.Round)
			err := bucket.Put(newKey, val.Signature)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		m.errChan <- err
	}
}

func (m *migrator) reader() {
	defer func() {
		finishedIn := time.Since(m.startedAt).String()
		m.logger.Infow(
			"finished reading existing beacon database",
			"beaconName", m.beaconName,
			"rows", m.existingRows,
			"finishedIn", finishedIn,
		)
	}()

	defer close(m.distChan)

	existingDB, err := bbolt.Open(m.sourceBeaconPath, 0600, nil)
	if err != nil {
		m.errChan <- err
		return
	}

	defer func() {
		err := existingDB.Close()
		if err != nil {
			m.errChan <- err
			return
		}
	}()

	err = existingDB.View(func(tx *bbolt.Tx) error {
		existingBucket := tx.Bucket(bucketName)
		return existingBucket.ForEach(func(k, v []byte) error {
			m.existingRows++

			b := beacon{}
			err := json.Unmarshal(v, &b)
			if err != nil {
				return err
			}

			m.distChan <- b

			return nil
		})
	})

	if err != nil {
		m.errChan <- err
	}
}

func (m *migrator) wait() error {
	select {
	case <-m.ctx.Done():
		return nil
	case err := <-m.errChan:
		defer close(m.errChan)
		return err
	}
}
