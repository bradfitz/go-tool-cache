// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher-server is an HTTP server daemon that go-cacher can hit.
/*

Protocol:

GET /action/<actionID-hex>
{"outputID":"$outputID-hex","size":1234}

GET /output/<outputID-hex>
200 of those bytes with Content-Length or 404

PUT /<actionID>/<outputID>
Content-Length: 1234
<bytes>

*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bradfitz/go-tool-cache/cachers"
)

var (
	dir     = flag.String("cache-dir", "", "cache directory")
	verbose = flag.Bool("verbose", false, "be verbose")
	listen  = flag.String("listen", ":31364", "listen address")
	latency = flag.Duration("inject-latency", 0, "the additional latency to add to all requests (for testing)")
)

func main() {
	flag.Parse()
	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher-server")
		log.Printf("Defaulting to cache dir %v ...", d)
		*dir = d
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}

	dc := cachers.NewSimpleDiskCache(*verbose, *dir)

	srv := &server{
		cache:   dc,
		verbose: *verbose,
		latency: *latency,
	}

	log.Fatal(http.ListenAndServe(*listen, srv))
}

type server struct {
	cache   *cachers.SimpleDiskCache // TODO: add interface for things other than disk cache? when needed.
	verbose bool
	latency time.Duration
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	if s.verbose {
		log.Printf("%s %s", r.Method, r.RequestURI)
	}
	if r.Method == "PUT" {
		s.handlePut(w, r)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/action/"):
		s.handleGetAction(w, r)
	case strings.HasPrefix(r.URL.Path, "/output/"):
		s.handleGetOutput(w, r)
	case r.URL.Path == "/":
		_, _ = io.WriteString(w, "hi")
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func getHexSuffix(r *http.Request, prefix string) (hexSuffix string, ok bool) {
	hexSuffix, _ = strings.CutPrefix(r.RequestURI, prefix)
	if !validHex(hexSuffix) {
		return "", false
	}
	return hexSuffix, true
}

func validHex(x string) bool {
	if len(x) < 4 || len(x) > 1000 || len(x)%2 == 1 {
		return false
	}
	for i := range x {
		b := x[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return false
	}
	return true
}

func (s *server) handleGetAction(w http.ResponseWriter, r *http.Request) {
	actionID, ok := getHexSuffix(r, "/action/")
	if !ok {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	outputID, diskPath, err := s.cache.Get(ctx, actionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if outputID == "" {
		http.Error(w, "not found ()", http.StatusNotFound)
		return
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found (post-stat)", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&cachers.ActionValue{
		OutputID: outputID,
		Size:     fi.Size(),
	})
}

func (s *server) handleGetOutput(w http.ResponseWriter, r *http.Request) {
	outputID, ok := getHexSuffix(r, "/output/")
	if !ok {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeFile(w, r, OutputFilename(*dir, outputID))
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != "PUT" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	actionID, outputID, ok := strings.Cut(r.RequestURI[len("/"):], "/")
	if !ok || !validHex(actionID) || !validHex(outputID) {
		http.Error(w, "bad URI", http.StatusBadRequest)
		return
	}
	if r.ContentLength == -1 {
		http.Error(w, "missing Content-Length", http.StatusBadRequest)
		return
	}
	_, err := s.cache.Put(ctx, actionID, outputID, r.ContentLength, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func OutputFilename(dir, outputID string) string {
	if len(outputID) < 4 || len(outputID) > 1000 {
		return ""
	}
	for i := range outputID {
		b := outputID[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return ""
	}
	return filepath.Join(dir, fmt.Sprintf("o-%s", outputID))
}
