// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// A small wrapper around the [gocached] library package to run it as a
// standalone binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bradfitz/go-tool-cache/gocached"
	_ "modernc.org/sqlite"
)

var (
	dir         = flag.String("cache-dir", "", "cache directory, if empty defaults to <UserCacheDir>/gocached")
	verbose     = flag.Bool("verbose", false, "be verbose")
	listen      = flag.String("listen", ":31364", "listen address for the build-facing HTTP server")
	debugListen = flag.String("debug-listen", "", "if non-empty, listen address for the debug HTTP server (pprof, metrics, etc)")

	maxSize = flag.Int("max-size-gb", 50, "maximum size of the cache in GiB; 0 means no limit")
	maxAge  = flag.Int("max-age-days", 60, "maximum age of objects in the cache in days; 0 means no limit")

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
	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "gocached")
		log.Printf("Defaulting to cache dir %v ...", d)
		*dir = d
	}
	if err := os.MkdirAll(*dir, 0750); err != nil {
		log.Fatal(err)
	}

	srv := &gocached.Server{
		Dir:     *dir,
		Logf:    log.Printf,
		Verbose: *verbose,
		MaxSize: int64(*maxSize) << 30,
		MaxAge:  time.Duration(*maxAge) * 24 * time.Hour,
	}

	if *jwtIssuer != "" {
		if len(jwtClaims) == 0 {
			log.Fatal("must specify --jwt-claim at least once when --jwt-issuer is set")
		}
		srv.JWTIssuer = *jwtIssuer
		srv.JWTClaims = jwtClaims

		globalClaims := map[string]string{}
		maps.Copy(globalClaims, jwtClaims)
		maps.Copy(globalClaims, globalJWTClaims)
		srv.GlobalJWTClaims = globalClaims
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

	if err := srv.Start(context.Background()); err != nil {
		log.Fatalf("starting gocached: %v", err)
	}

	log.Printf("gocached: listening on %s ...", *listen)
	log.Fatal(http.ListenAndServe(*listen, srv))
}
