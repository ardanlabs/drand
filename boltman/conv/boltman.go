package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/chain/migration"
	"github.com/drand/drand/log"
)

func main() {
	// const basePath = "/tmp/ramdisk/"
	const basePath = "/home/florin/projects/drand/drand/"

	hostPort := `localhost:5432`
	if customHostPort := os.Getenv("florin_drand_pg_hostport"); customHostPort != "" {
		hostPort = customHostPort
	}
	var pgDSN = fmt.Sprintf(`postgres://drand:drand@%s/drand?sslmode=disable&connect_timeout=5`, hostPort)

	logger := log.NewLogger(nil, log.LogDebug)

	migrationTarget := chain.BoltDB
	// migrationTarget := chain.PostgreSQL

	// const baseFileName = "b9s-db"
	// const baseFileName = "def-bkp"
	// const baseFileName = "unch"
	const baseFileName = "drand-testnet"

	sourcePath := fmt.Sprintf("%s%s.bkp", basePath, baseFileName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := migration.Migrate(ctx, logger, sourcePath, baseFileName, migrationTarget, pgDSN, 10000)
	if !errors.Is(err, migration.ErrMigrationNotNeeded) {
		panic(err)
	}
	logger.Infow("finished migration process")
}
