// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"time"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

var (
	dir        = flag.String("cache-dir", "", "cache directory; empty means automatic")
	serverBase = flag.String("cache-server", "", "optional cache server HTTP prefix (scheme and authority only); should be low latency. empty means to not use one.")
	verbose    = flag.Bool("verbose", false, "be verbose")
	gwPort     = flag.Int("gateway-addr-port", 0, "if non-zero, try to use an HTTP server on this port on our machine's gateway IP. If that fails, use local disk instead.")
)

func main() {
	flag.Parse()
	if *verbose {
		log.Printf("go-cacher: verbose mode enabled")
	}
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

	dc := &cachers.DiskCache{Dir: *dir}

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

	if *gwPort != 0 {
		if gw, ok := getGatewayIP(); ok {
			probe := net.JoinHostPort(gw, fmt.Sprint(*gwPort))
			log.Printf("go-cacher: probing gateway IP %v", gw)
			var d net.Dialer
			d.Timeout = time.Second / 2
			c, err := d.Dial("tcp", probe)
			if err != nil {
				log.Printf("go-cacher: failed to probe %v: %v", probe, err)
			} else {
				c.Close()
				*serverBase = "http://" + probe
			}
		} else {
			log.Printf("go-cacher: failed to get gateway IP; using local disk cache instead")
		}
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

	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func getGatewayIP() (ip string, ok bool) {
	if runtime.GOOS != "darwin" {
		return
	}
	out, err := exec.Command("route", "-n", "get", "default").CombinedOutput()
	if err != nil {
		log.Printf("getGatewayIP: %v, %s", err, out)
		return "", false
	}
	rx := regexp.MustCompile(`(?m)^\s*gateway: (\S+)`)
	if m := rx.FindSubmatch(out); len(m) == 2 {
		return string(m[1]), true
	}
	return "", false
}
