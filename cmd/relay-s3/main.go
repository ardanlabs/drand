package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	client2 "github.com/drand/drand/client"
	"github.com/drand/drand/common"
	"github.com/drand/drand/common/client"
	"github.com/drand/drand/common/log"
	"github.com/drand/drand/internal/lib"
	json "github.com/nikkolasg/hexjson"
	"github.com/urfave/cli/v2"
)

// Automatically set through -ldflags
// Example: go install -ldflags "-X main.buildDate=$(date -u +%d/%m/%Y@%H:%M:%S) -X main.gitCommit=$(git rev-parse HEAD)"
var (
	gitCommit = "none"
	buildDate = "unknown"
)

var (
	bucketFlag = &cli.StringFlag{
		Name:     "bucket",
		Usage:    "Name of the AWS bucket to upload to",
		Required: true,
		EnvVars:  []string{"DRAND_S3RELAY_BUCKET"},
	}
	regionFlag = &cli.StringFlag{
		Name:    "region",
		Usage:   "Name of the AWS region to use (optional)",
		EnvVars: []string{"DRAND_S3RELAY_REGION"},
	}
)

func main() {
	version := common.GetAppVersion()

	app := &cli.App{
		Name:     "drand-relay-s3",
		Version:  version.String(),
		Usage:    "AWS S3 relay for randomness beacon",
		Commands: []*cli.Command{runCmd, syncCmd},
	}

	// See https://cli.urfave.org/v2/examples/bash-completions/#enabling for how to turn on.
	app.EnableBashCompletion = true

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("drand AWS S3 relay %s (date %v, commit %v)\n", version, buildDate, gitCommit)
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(1)
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "start a drand AWS S3 relay process",
	Flags: append(lib.ClientFlags, bucketFlag, regionFlag),

	Action: func(cctx *cli.Context) error {
		sess, err := session.NewSession(&aws.Config{Region: aws.String(cctx.String(regionFlag.Name))})
		if err != nil {
			return fmt.Errorf("creating aws session: %w", err)
		}

		if _, err := sess.Config.Credentials.Get(); err != nil {
			return fmt.Errorf("checking credentials: %w", err)
		}

		lg := log.New(nil, log.DefaultLevel, false)
		cctx.Context = log.ToContext(cctx.Context, lg)
		c, err := lib.Create(cctx, false)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		upr := s3manager.NewUploader(sess)
		watch(context.Background(), lg, c, upr, cctx.String(bucketFlag.Name))
		return nil
	},
}

func watch(ctx context.Context, l log.Logger, c client2.Watcher, upr *s3manager.Uploader, buc string) {
	for {
		ch := c.Watch(ctx)
	INNER:
		for {
			select {
			case res, ok := <-ch:
				if !ok {
					l.Warnw("", "relay_s3", "watch channel closed")
					t := time.NewTimer(time.Second)
					select {
					case <-t.C:
						break INNER
					case <-ctx.Done():
						return
					}
				}
				l.Infow("", "relay_s3", "got randomness", "round", res.Round())
				go func(res client.Result) {
					url, err := uploadRandomness(ctx, upr, buc, res)
					if err != nil {
						l.Errorw("", "relay_s3", "failed to upload randomness", "err", err)
						return
					}
					l.Infow("", "relay_s3", "uploaded randomness", "round", res.Round(), "location", url)
				}(res)
			case <-ctx.Done():
				return
			}
		}
	}
}

func uploadRandomness(ctx context.Context, upr *s3manager.Uploader, buc string, res client.Result) (string, error) {
	rd, ok := res.(*client2.RandomData)
	if !ok {
		return "", fmt.Errorf("unexpected underlying result type")
	}
	data, err := json.Marshal(rd)
	if err != nil {
		return "", fmt.Errorf("failed to marshal randomness: %w", err)
	}
	r, err := upr.UploadWithContext(ctx, &s3manager.UploadInput{
		ACL:          aws.String("public-read"),
		Bucket:       aws.String(buc),
		Key:          aws.String(fmt.Sprintf("public/%v", res.Round())),
		Body:         bytes.NewBuffer(data),
		ContentType:  aws.String("application/json"),
		CacheControl: aws.String("public, max-age=604800, immutable"),
	})
	if err != nil {
		return "", err
	}
	return r.Location, nil
}

var syncCmd = &cli.Command{
	Name:  "sync",
	Usage: "sync the AWS S3 bucket with the randomness chain",
	Flags: append(
		lib.ClientFlags,
		bucketFlag,
		regionFlag,
		&cli.Uint64Flag{
			Name:  "begin",
			Usage: "Begin syncing from this round number to the latest round.",
			Value: 1,
		},
	),

	Action: func(cctx *cli.Context) error {
		sess, err := session.NewSession(&aws.Config{Region: aws.String(cctx.String(regionFlag.Name))})
		if err != nil {
			return fmt.Errorf("creating aws session: %w", err)
		}

		if _, err := sess.Config.Credentials.Get(); err != nil {
			return fmt.Errorf("checking credentials: %w", err)
		}

		lg := log.New(nil, log.DefaultLevel, false)
		cctx.Context = log.ToContext(cctx.Context, lg)
		c, err := lib.Create(cctx, false)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		buc := cctx.String(bucketFlag.Name)
		upr := s3manager.NewUploader(sess)
		ctx := context.Background()

		for rnd := cctx.Uint64("begin"); rnd <= c.RoundAt(time.Now()); rnd++ {
			// TODO: check if bucket already has this round
			r, err := c.Get(ctx, rnd)
			if err != nil {
				lg.Errorw("", "relay_s3_sync", "failed to get randomness", "round", rnd, "err", err)
				continue
			}
			url, err := uploadRandomness(ctx, upr, buc, r)
			if err != nil {
				lg.Errorw("", "relay_s3_sync", "failed to upload randomness", "err", err)
				continue
			}
			lg.Infow("", "relay_s3_sync", "uploaded randomness", "round", r.Round(), "location", url)
		}

		return nil
	},
}
