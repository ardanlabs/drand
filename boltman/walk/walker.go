package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

// beacon holds the randomness as well as the info to verify it.
type beacon struct {
	// Round is the round number this beacon is tied to
	Round uint64
	// Signature is the BLS deterministic signature over Round || PreviousRand
	Signature []byte
	// PreviousSig is the previous signature generated
	PreviousSig []byte `json:",omitempty"`
}

func main() {
	const basePath = "/home/florin/projects/drand/drand/"
	// const baseFileName = "b9s-db"
	// const baseFileName = "def-bkp"
	const baseFileName = "drand-testnet"
	// const baseFileName = "drand-testnet-new-trimmed"

	bucketName := []byte("beacons")
	rows := 0
	started := time.Now()
	defer func() {
		finishedIn := time.Since(started)
		//nolint:forbidigo // I want to print to stdout
		fmt.Printf("\n\nFinished processing %s containing %d records in %s\n\n", baseFileName, rows, finishedIn)
	}()

	existingDB, err := bbolt.Open(basePath+baseFileName+".bkp", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = existingDB.Close()
	}()

	err = existingDB.View(func(tx *bbolt.Tx) error {
		existingBucket := tx.Bucket(bucketName)

		var prevSig []byte
		var prevBeacon beacon
		rounds := make(map[string]uint64)
		err := existingBucket.ForEach(func(k, v []byte) error {
			rows++
			/*var b beacon
			err := json.Unmarshal(v, &b)
			if err != nil {
				return err
			}*/

			b := beacon{
				PreviousSig: prevSig,
				Round:       binary.BigEndian.Uint64(k),
				Signature:   v,
			}

			if b.Round-1 != prevBeacon.Round {
				log.Printf("previous beacon round %d is farther behind than current round %d\n", prevBeacon.Round, b.Round)
			}
			if previousRound, exists := rounds[string(b.Signature)]; exists {
				log.Printf("duplicate round signature found previous round: %d current round: %d!\n", previousRound, b.Round)
			}
			rounds[string(b.Signature)] = b.Round

			prevSig = v
			prevBeacon = b

			return nil
		})

		if err != nil {
			log.Fatal(err)
		}

		_ = prevSig
		_ = prevBeacon
		log.Printf("last beacon round %d\n", prevBeacon.Round)
		return nil
	})

	statsTime := func() {
		length := 0
		startedAt := time.Now()
		defer func() {
			finishedIn := time.Since(startedAt)
			log.Printf("db length %d - time processing: %s\n", length, finishedIn)
		}()

		err := existingDB.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			length = bucket.Stats().KeyN
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	statsTime()

	if err != nil {
		log.Fatal(err)
	}
}
