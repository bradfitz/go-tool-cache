// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/bradfitz/go-tool-cache/abs"
	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

var (
	dir        = flag.String("cache-dir", "", "cache directory; empty means automatic")
	serverBase = flag.String("cache-server", "", "optional cache server HTTP prefix (scheme and authority only); should be low latency. empty means to not use one.")
	verbose    = flag.Bool("verbose", false, "be verbose")
	remote = flag.String("remote", "", "remote to use. Defaults to disabled. Valid values are: azure")

	absAccountName = flag.String("abs-account-name", "", "Azure Blob Storage account name")
	absAccountKey  = flag.String("abs-account-key", "", "Azure Blob Storage account key")
	absEndpoint    = flag.String("abs-endpoint", "", "Azure Blob Storage endpoint")
	absContainer   = flag.String("abs-container", "", "Azure Blob Storage container")
)

func main() {
	flag.Parse()
	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		log.Printf("Defaulting to cache dir %v ...", d)
		*dir = d
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}

	var cache cachers.Cache

	dc := &cachers.DiskCache{Dir: *dir, Verbose: *verbose}

	switch *remote {
		case "azure":
			cache = &cachers.WithUpstream{
				Upstream: &abs.CacheUpstream{
					AccountName: *absAccountName,
					AccountKey:  *absAccountKey,
					Endpoint:    *absEndpoint,
					Container:   *absContainer,
				},
				Local: dc,
			}
		default:
			cache = dc
	}

	var p *cacheproc.Process
	p = &cacheproc.Process{
		Close: func() error {
			if *verbose {
				log.Printf("cacher: closing; %d gets (%d hits, %d misses, %d errors); %d puts (%d errors)",
					p.Gets.Load(), p.GetHits.Load(), p.GetMisses.Load(), p.GetErrors.Load(), p.Puts.Load(), p.PutErrors.Load())
			}
			return nil
		},
		Get: cache.Get,
		Put: cache.Put,
	}

	if *serverBase != "" {
		hc := &cachers.WithUpstream{
			Upstream: &cachers.HTTPRemote{
				BaseURL: *serverBase,
				Verbose: *verbose,
			},
			Local: dc,
		}
		p.Get = hc.Get
		p.Put = hc.Put
	}

	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
