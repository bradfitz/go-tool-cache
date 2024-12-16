// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cacheproc implements the mechanics of talking to cmd/go's GOCACHE protocol
// so you can write a caching child process at a higher level.
package cacheproc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/bradfitz/go-tool-cache/wire"
)

// Process implements the cmd/go JSON protocol over stdin & stdout via three
// funcs that callers can optionally implement.
type Process struct {
	// Get optionally specifies a func to look up something from the cache. If
	// nil, all gets are treated as cache misses.touch
	//
	// The actionID is a lowercase hex string of unspecified format or length.
	//
	// The returned outputID must be the same outputID provided to Put earlier;
	// it will be a lowercase hex string of unspecified hash function or length.
	//
	// On cache miss, return all zero values (no error). On cache hit, diskPath
	// must be the absolute path to a regular file; its size and modtime are
	// returned to cmd/go.
	//
	// If the returned diskPath doesn't exist, it's treated as a cache miss.
	Get func(ctx context.Context, actionID string) (outputID, diskPath string, _ error)

	// Put optionally specifies a func to add something to the cache.
	// The actionID and outputID is a lowercase hex string of unspecified format or length.
	// On success, diskPath must be the absolute path to a regular file.
	// If nil, cmd/go may write to disk elsewhere as needed.
	Put func(ctx context.Context, actionID, outputID string, size int64, r io.Reader) (diskPath string, _ error)

	// Close optionally specifies a func to run when the cmd/go tool is
	// shutting down.
	Close func() error

	Gets      atomic.Int64
	GetHits   atomic.Int64
	GetMisses atomic.Int64
	GetErrors atomic.Int64
	Puts      atomic.Int64
	PutErrors atomic.Int64
}

func (p *Process) Run() error {
	br := bufio.NewReader(os.Stdin)
	jd := json.NewDecoder(br)

	bw := bufio.NewWriter(os.Stdout)
	je := json.NewEncoder(bw)

	var caps []wire.Cmd
	if p.Get != nil {
		caps = append(caps, "get")
	}
	if p.Put != nil {
		caps = append(caps, "put")
	}
	if p.Close != nil {
		caps = append(caps, "close")
	}
	je.Encode(&wire.Response{KnownCommands: caps})
	if err := bw.Flush(); err != nil {
		return err
	}

	var wmu sync.Mutex // guards writing responses

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		var req wire.Request
		if err := jd.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		// For Go1.23 backward compatibility, remove in Go1.25.
		if len(req.OutputID) == 0 && len(req.ObjectID) != 0 {
			req.OutputID = req.ObjectID
		}
		if req.Command == wire.CmdPut && req.BodySize > 0 {
			// TODO(bradfitz): stream this and pass a checksum-validating
			// io.Reader that validates on EOF.
			var bodyb []byte
			if err := jd.Decode(&bodyb); err != nil {
				log.Fatal(err)
			}
			if int64(len(bodyb)) != req.BodySize {
				log.Fatalf("only got %d bytes of declared %d", len(bodyb), req.BodySize)
			}
			req.Body = bytes.NewReader(bodyb)
		}
		go func() {
			res := &wire.Response{ID: req.ID}
			ctx := ctx // TODO: include req ID as a context.Value for tracing?
			if err := p.handleRequest(ctx, &req, res); err != nil {
				res.Err = err.Error()
			}
			wmu.Lock()
			defer wmu.Unlock()
			je.Encode(res)
			bw.Flush()
		}()
	}
}

func (p *Process) handleRequest(ctx context.Context, req *wire.Request, res *wire.Response) error {
	switch req.Command {
	default:
		return errors.New("unknown command")
	case "close":
		if p.Close != nil {
			return p.Close()
		}
		return nil
	case "get":
		return p.handleGet(ctx, req, res)
	case "put":
		return p.handlePut(ctx, req, res)
	}
}

func (p *Process) handleGet(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	p.Gets.Add(1)
	defer func() {
		if retErr != nil {
			p.GetErrors.Add(1)
		} else if res.Miss {
			p.GetMisses.Add(1)
		} else {
			p.GetHits.Add(1)
		}
	}()
	if p.Get == nil {
		res.Miss = true
		return nil
	}
	outputID, diskPath, err := p.Get(ctx, fmt.Sprintf("%x", req.ActionID))
	if err != nil {
		return err
	}
	if outputID == "" && diskPath == "" {
		res.Miss = true
		return nil
	}
	if outputID == "" {
		return errors.New("no outputID")
	}
	res.OutputID, err = hex.DecodeString(outputID)
	if err != nil {
		return fmt.Errorf("invalid OutputID: %v", err)
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			res.Miss = true
			return nil
		}
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	res.Size = fi.Size()
	res.TimeNanos = fi.ModTime().UnixNano()
	res.DiskPath = diskPath
	return nil
}

func (p *Process) handlePut(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	actionID, outputID := fmt.Sprintf("%x", req.ActionID), fmt.Sprintf("%x", req.OutputID)
	p.Puts.Add(1)
	defer func() {
		if retErr != nil {
			p.PutErrors.Add(1)
			log.Printf("put(action %s, obj %s, %v bytes): %v", actionID, outputID, req.BodySize, retErr)
		}
	}()
	if p.Put == nil {
		if req.Body != nil {
			io.Copy(io.Discard, req.Body)
		}
		return nil
	}
	var body io.Reader = req.Body
	if body == nil {
		body = bytes.NewReader(nil)
	}
	diskPath, err := p.Put(ctx, actionID, outputID, req.BodySize, body)
	if err != nil {
		return err
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		return fmt.Errorf("stat after successful Put: %w", err)
	}
	if fi.Size() != req.BodySize {
		return fmt.Errorf("failed to write file to disk with right size: disk=%v; wanted=%v", fi.Size(), req.BodySize)
	}
	res.DiskPath = diskPath
	return nil
}
