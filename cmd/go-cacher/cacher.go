// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const (
	defaultPrefix = "go-cacher"
)

// All the following env variable names are optional
const (
	// path to local disk directory. defaults to os.UserCacheDir()/go-cacher
	envVarDiskCacheDir = "GOCACHE_DISK_DIR"

	// S3 cache
	envVarS3CacheRegion        = "GOCACHE_AWS_REGION"
	envVarS3CacheURL           = "GOCACHE_AWS_URL"
	envVarS3AwsAccessKey       = "GOCACHE_AWS_ACCESS_KEY"
	envVarS3AwsSecretAccessKey = "GOCACHE_AWS_SECRET_ACCESS_KEY"
	envVarS3AwsSessionToken    = "GOCACHE_AWS_SESSION_TOKEN"
	envVarS3AwsCredsProfile    = "GOCACHE_AWS_CREDS_PROFILE"
	envVarS3BucketName         = "GOCACHE_S3_BUCKET"
	envVarS3Prefix             = "GOCACHE_S3_PREFIX"

	// HTTP cache - optional cache server HTTP prefix (scheme and authority only);
	envVarHttpCacheServerBase = "GOCACHE_HTTP_SERVER_BASE"
)

var (
	verbose = flag.Bool("verbose", false, "be verbose")
)

type Env interface {
	Get(key string) string
}

type osEnv struct{}

func (osEnv) Get(key string) string {
	return os.Getenv(key)
}

func getAwsConfigFromEnv(ctx context.Context, env Env) (*aws.Config, error) {
	// read from env
	awsRegion := env.Get(envVarS3CacheRegion)
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}
	accessKey := env.Get(envVarS3AwsAccessKey)
	secretAccessKey := env.Get(envVarS3AwsSecretAccessKey)
	sessionToken := env.Get(envVarS3AwsSessionToken)
	if accessKey != "" && secretAccessKey != "" || sessionToken != "" {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(awsRegion),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretAccessKey,
					SessionToken:    sessionToken,
				},
			}))
		return &cfg, err
	}
	credsProfile := env.Get(envVarS3AwsCredsProfile)
	if credsProfile != "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion), config.WithSharedConfigProfile(credsProfile))
		return &cfg, err
	}
	return nil, errors.New("no s3 credentials found")
}

func maybeS3Cache(ctx context.Context, env Env) (cachers.RemoteCache, error) {
	bucket := env.Get(envVarS3BucketName)
	if bucket == "" {
		// We need at least name of bucket.
		return nil, nil
	}

	awsConfig, err := getAwsConfigFromEnv(ctx, env)
	if err != nil {
		return nil, err
	}
	prefix := strings.Trim(env.Get(envVarS3Prefix), "/")
	if prefix == "" {
		prefix = defaultPrefix
	}

	s3Client := s3.NewFromConfig(*awsConfig, func(o *s3.Options) {
		if u := env.Get(envVarS3CacheURL); u != "" {
			// Custom URL, use path style.
			o.UsePathStyle = true
			o.BaseEndpoint = &u
		}
	},
	)
	s3Cache := cachers.NewS3Cache(s3Client, bucket, prefix, *verbose)
	return s3Cache, nil
}

func getCache(ctx context.Context, env Env, verbose bool) cachers.LocalCache {
	dir := getDir(env)
	var local cachers.LocalCache = cachers.NewSimpleDiskCache(verbose, dir)

	remote, err := maybeS3Cache(ctx, env)
	if err != nil {
		log.Fatal(err)
	}
	if remote == nil {
		remote, err = maybeHttpCache(env)
		if err != nil {
			log.Fatal(err)
		}
	}

	if remote != nil {
		return cachers.NewCombinedCache(local, remote, verbose)
	}
	if verbose {
		return cachers.NewLocalCacheStates(local)
	}
	return local
}

func maybeHttpCache(env Env) (cachers.RemoteCache, error) {
	serverBase := env.Get(envVarHttpCacheServerBase)
	if serverBase == "" {
		return nil, nil
	}
	return cachers.NewHttpCache(serverBase, *verbose), nil
}

func getDir(env Env) string {
	dir := env.Get(envVarDiskCacheDir)
	if dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		dir = d
	}
	return dir
}

func main() {
	flag.Parse()
	env := &osEnv{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cache := getCache(ctx, env, *verbose)
	proc := cacheproc.NewCacheProc(cache)
	if err := proc.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
