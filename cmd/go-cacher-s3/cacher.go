// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

var (
	flagVerbose  = flag.Bool("verbose", false, "be verbose")
	flagCacheKey = flag.String("cache-key", defaultCacheKey, "cache key")
	bucket       string
)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("usage: %s <bucket>", os.Args[0])
	}
	bucket = flag.Args()[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("starting cache")
	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("S3 cache disabled; failed to load AWS config: ", err)
	}
	proc := cacheproc.NewCacheProc(
		cachers.NewCombinedCache(&cachers.NoopLocalCache{},
			cachers.NewS3Cache(s3.NewFromConfig(awsConfig), bucket, *flagCacheKey, *flagVerbose),
			*flagVerbose,
		),
	)
	if err := proc.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
