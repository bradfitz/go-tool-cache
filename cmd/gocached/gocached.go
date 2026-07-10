// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// The gocached command is a small example binary showing how to use
// the gocached library package.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bradfitz/go-tool-cache/gocached"
	"github.com/bradfitz/parentdeath"
	_ "modernc.org/sqlite"
)

var (
	dir         = flag.String("cache-dir", "", "cache directory, if empty defaults to <UserCacheDir>/gocached")
	sqliteDir   = flag.String("sqlite-dir", "", "directory for the SQLite metadata database; if empty, defaults to the cache directory")
	hotDir      = flag.String("hot-dir", "", "if non-empty, enable storage tiering with this directory as a fast tier (e.g. local NVMe) holding a bounded copy of recently used blobs; the cache directory remains the source of truth")
	hotCapacity = flag.Int("hot-capacity-gb", 600, "maximum size of the hot tier directory in GiB; only used with --hot-dir")
	verbose     = flag.Bool("verbose", false, "be verbose")
	listen      = flag.String("listen", ":31364", "listen address for the build-facing HTTP server")
	debugListen = flag.String("debug-listen", "", "if non-empty, listen address for the debug HTTP server (pprof, metrics, etc)")

	maxSize = flag.Int("max-size-gb", 50, "maximum size of the cache in GiB; 0 means no limit")
	maxAge  = flag.Int("max-age-days", 60, "maximum age of objects in the cache in days; 0 means no limit")

	shardPrefixLen = flag.Int("shard-prefix-len", 2, "number of SHA256 hex characters per usage-stats shard key (valid 1..4); total shards = 16^n. Defaults to 2 (256 shards). Increase if a single shard's stats scan becomes too slow on a large DB")

	jwtIssuer = flag.String("jwt-issuer", "", "the issuer to trust JWTs from; if set, all requests will require auth, and must set at least one -jwt-claim")
	// See example GitHub token claims for what can be available:
	// https://docs.github.com/en/actions/concepts/security/openid-connect
	jwtClaims       = make(jwtClaimValue)
	globalJWTClaims = make(jwtClaimValue)
)

type jwtClaimValue map[string]string

func (v jwtClaimValue) String() string {
	return fmt.Sprintf("%v", map[string]string(v))
}

func (v jwtClaimValue) Set(s string) error {
	claim, value, ok := strings.Cut(s, "=")
	if !ok || claim == "" || value == "" {
		return fmt.Errorf("bad claim %q, want x=y", s)
	}
	v[claim] = value
	return nil
}

func main() {
	flag.Var(&jwtClaims, "jwt-claim", "a claim in the form x=y that any JWT presented must have to start a session; may be specified more than once")
	flag.Var(&globalJWTClaims, "global-jwt-claim", "an additional claim in the form x=y that a JWT must have to allow writing to the cache's global namespace; may be specified more than once")
	flag.Parse()

	parentdeath.Monitor(func() {
		log.Printf("gocached: parent process died, exiting")
		os.Exit(0)
	})

	opts := []gocached.ServerOption{
		gocached.WithDir(*dir),
		gocached.WithSQLiteDir(*sqliteDir),
		gocached.WithVerbose(*verbose),
		gocached.WithMaxSize(int64(*maxSize) << 30),
		gocached.WithMaxAge(time.Duration(*maxAge) * 24 * time.Hour),
		gocached.WithShardPrefixLen(*shardPrefixLen),
	}

	if *hotDir != "" {
		if *hotCapacity <= 0 {
			log.Fatal("--hot-capacity-gb must be positive when --hot-dir is set")
		}
		opts = append(opts,
			gocached.WithHotDir(*hotDir),
			gocached.WithHotCapacity(int64(*hotCapacity)<<30),
		)
	}

	if *jwtIssuer != "" {
		if len(jwtClaims) == 0 {
			log.Fatal("must specify --jwt-claim at least once when --jwt-issuer is set")
		}

		opts = append(opts,
			gocached.WithJWTAuth(*jwtIssuer),
			gocached.WithNamespaceMapping(func(claims map[string]any) (gocached.Namespace, error) {
				var ns gocached.Namespace
				for k, want := range jwtClaims {
					if got := claims[k]; got != want {
						return "", fmt.Errorf("claim %q = %v, want %v", k, got, want)
					}
					if ns != "" {
						ns += ","
					}
					ns += gocached.Namespace(fmt.Sprintf("%s=%s", k, want))
				}
				for k, want := range globalJWTClaims {
					if got := claims[k]; got != want {
						return ns, nil
					}
				}
				return gocached.GlobalNamespace, nil
			}),
		)
	}

	srv, err := gocached.NewServer(opts...)
	if err != nil {
		log.Fatalf("starting gocached: %v", err)
	}

	if *debugListen != "" {
		debugLn, err := net.Listen("tcp", *debugListen)
		if err != nil {
			log.Fatalf("debug listen: %v", err)
		}
		go func() {
			log.Fatal(http.Serve(debugLn, http.HandlerFunc(srv.ServeHTTPDebug)))
		}()
	}

	log.Printf("gocached: listening on %s ...", *listen)
	log.Fatal(http.ListenAndServe(*listen, srv))
}
