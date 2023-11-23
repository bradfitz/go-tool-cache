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
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

var (
	serverBase = flag.String("cache-server", "", "optional cache server HTTP prefix (scheme and authority only); should be low latency. empty means to not use one.")
	verbose    = flag.Bool("verbose", false, "be verbose")
)

func getAwsConfigFromEnv() (*aws.Config, error) {
	// read from env
	awsRegion, awsRegionOk := os.LookupEnv("GOCACHE_AWS_REGION")
	if !awsRegionOk {
		return nil, nil
	}
	accessKey, accessKeyOk := os.LookupEnv("GOCACHE_AWS_ACCESS_KEY")
	secretAccessKey, secretKeyOk := os.LookupEnv("GOCACHE_AWS_SECRET_ACCESS_KEY")
	if accessKeyOk && secretKeyOk {
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(awsRegion),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretAccessKey,
				},
			}))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	credsProfile, credsProfileOk := os.LookupEnv("GOCACHE_CREDS_PROFILE")
	if credsProfileOk {
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion), config.WithSharedConfigProfile(credsProfile))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	return nil, nil
}

func maybeS3Cache() (cachers.RemoteCache, error) {
	awsConfig, err := getAwsConfigFromEnv()
	if err != nil {
		return nil, err
	}
	bucket, ok := os.LookupEnv("GOCACHE_S3_BUCKET")
	if !ok || awsConfig == nil {
		// We need at least name of bucket and valid aws config
		return nil, nil
	}
	cacheKey := os.Getenv("GOCACHE_CACHE_KEY")
	if cacheKey == "" {
		cacheKey = defaultCacheKey
	}
	s3Client := s3.NewFromConfig(*awsConfig)
	s3Cache := cachers.NewS3Cache(s3Client, bucket, cacheKey, *verbose)
	return s3Cache, nil
}

func getFinalCacher(local cachers.LocalCache, remote cachers.RemoteCache, verbose bool) cachers.LocalCache {
	if remote != nil {
		return cachers.NewCombinedCache(local, remote, verbose)
	}
	if verbose {
		return cachers.NewLocalCacheStates(local)
	}
	return local
}

func main() {
	flag.Parse()
	dir := os.Getenv("GOCACHE_DISK_DIR")
	if dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		dir = d
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	var localCache cachers.LocalCache = cachers.NewSimpleDiskCache(*verbose, dir)
	s3Cache, err := maybeS3Cache()
	if err != nil {
		log.Fatal(err)
	}
	proc := cacheproc.NewCacheProc(getFinalCacher(localCache, s3Cache, *verbose))
	if err := proc.Run(); err != nil {
		log.Fatal(err)
	}
}
