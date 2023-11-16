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

func maybeS3Cache(dc *cachers.DiskCache) (*cachers.S3Cache, error) {
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

	s3Cache := cachers.NewS3Cache(bucket, awsConfig, cacheKey, dc, *verbose)
	return s3Cache, nil
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
	log.Printf("cache dir %v ...", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}

	dc := &cachers.DiskCache{Dir: dir}

	var p *cacheproc.Process
	p = &cacheproc.Process{
		Close: func() error {
			if *verbose {
				log.Printf("cacher: closing; %d gets (%d hits, %d misses, %d errors); %d puts (%d errors)",
					p.Gets.Load(), p.GetHits.Load(), p.GetMisses.Load(), p.GetErrors.Load(), p.Puts.Load(), p.PutErrors.Load())
			}
			return nil
		},
		Get: dc.Get,
		Put: dc.Put,
	}

	if *serverBase != "" {
		hc := &cachers.HTTPClient{
			BaseURL: *serverBase,
			Disk:    dc,
			Verbose: *verbose,
		}
		p.Get = hc.Get
		p.Put = hc.Put
	}

	s3Cache, err := maybeS3Cache(dc)
	if err != nil {
		log.Fatal(err)
	}
	if s3Cache != nil {
		p.Get = s3Cache.Get
		p.Put = s3Cache.Put
	}

	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
