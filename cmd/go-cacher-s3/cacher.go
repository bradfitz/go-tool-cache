// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

var userCacheDir, _ = os.UserCacheDir()
var defaultLocalCacheDir = filepath.Join(userCacheDir, "go-cacher")

var (
	flagVerbose                = flag.Int("v", 0, "logging verbosity; 0=error, 1=warn, 2=info, 3=debug")
	flagCacheKey               = flag.String("cache-key", defaultCacheKey, "cache key")
	flagLocalCacheDir          = flag.String("local-cache-dir", defaultLocalCacheDir, "local cache directory")
	flagSkipZeroByteRemotePuts = flag.Bool("skip-zero-byte-remote-puts", false, "skip zero-byte remote puts")
	bucket                     string
)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("usage: %s <bucket>", os.Args[0])
	}
	bucket = flag.Args()[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logLevel := slog.Level(*flagVerbose*-4 + 8)
	slog.Info(fmt.Sprintf("Log level: %d", logLevel))

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	log.Printf("starting cache")
	var clientLogMode aws.ClientLogMode
	if logLevel >= slog.LevelDebug {
		clientLogMode = aws.LogRetries | aws.LogRequest
	}
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(clientLogMode))
	if err != nil {
		log.Fatal("S3 cache disabled; failed to load AWS config: ", err)
	}
	s3Cacher := cachers.NewS3Cache(s3.NewFromConfig(awsConfig), bucket, *flagCacheKey)
	s3Cacher.SkipZeroBytePuts = *flagSkipZeroByteRemotePuts
	proc := cacheproc.NewCacheProc(
		cachers.NewCombinedCache(
			cachers.NewSimpleDiskCache(*flagLocalCacheDir),
			s3Cacher,
		),
	)
	if err := proc.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
