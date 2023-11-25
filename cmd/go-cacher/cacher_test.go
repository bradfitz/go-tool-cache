package main

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mapEnv struct {
	m map[string]string
}

var _ Env = &mapEnv{}

func (m *mapEnv) Get(key string) string {
	return m.m[key]
}

func TestMaybeS3Cache(t *testing.T) {
	fullMap := map[string]string{
		envVarS3BucketName:         "bucket",
		envVarS3CacheRegion:        "region",
		envVarS3AwsAccessKey:       "accessKey",
		envVarS3AwsSecretAccessKey: "secretAccessKey",
	}
	for k := range fullMap {
		missingMap := map[string]string{}
		maps.Copy(missingMap, fullMap)
		delete(missingMap, k)
		t.Run("should return nil if "+k+" is missing", func(t *testing.T) {
			env := &mapEnv{m: missingMap}
			client, err := maybeS3Cache(context.TODO(), env)
			assert.NoError(t, err)
			assert.Nil(t, client)
		})
	}

	t.Run("should return cache if all expected env vars exists", func(t *testing.T) {
		env := &mapEnv{
			m: fullMap,
		}
		client, err := maybeS3Cache(context.TODO(), env)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}

func TestMaybeHttpCache(t *testing.T) {
	t.Run("should return nil if "+envVarHttpCacheServerBase+" is missing", func(t *testing.T) {
		env := &mapEnv{m: map[string]string{}}
		client, err := maybeHttpCache(env)
		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	t.Run("should return cache if "+envVarHttpCacheServerBase+" env vars exists", func(t *testing.T) {
		env := &mapEnv{
			m: map[string]string{
				envVarHttpCacheServerBase: "http://localhost:8080",
			},
		}
		client, err := maybeHttpCache(env)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}
