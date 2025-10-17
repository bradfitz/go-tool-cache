package cacheproc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/go-tool-cache/wire"
)

// TestProcess_Run_WaitsForGoroutines verifies that the Run method waits for all
// request-handling goroutines to complete before returning. It tests this for
// different commands (e.g., "get", "put") by sending a request with a simulated
// delay in its handler, closing stdin to signal the end of requests, and then
// ensuring that the Run method doesn't return until the delayed handler has
// finished its work.
func TestProcess_Run_WaitsForGoroutines(t *testing.T) {
	type testCase struct {
		name         string
		process      func(wg *sync.WaitGroup, started chan struct{}, tmpFilePath string) *Process
		request      func() (wire.Request, []byte)
		validate     func(t *testing.T, dec *json.Decoder, req wire.Request, tmpFilePath string)
		wantKnownCmd wire.Cmd
	}

	tests := []testCase{
		{
			name: "Put",
			process: func(wg *sync.WaitGroup, started chan struct{}, tmpFilePath string) *Process {
				return &Process{
					Put: func(ctx context.Context, actionID, outputID string, size int64, r io.Reader) (string, error) {
						wg.Add(1)
						defer wg.Done()
						close(started)
						time.Sleep(100 * time.Millisecond)
						body, err := io.ReadAll(r)
						if err != nil {
							return "", err
						}
						if err := os.WriteFile(tmpFilePath, body, 0666); err != nil {
							return "", err
						}
						return tmpFilePath, nil
					},
				}
			},
			request: func() (wire.Request, []byte) {
				return wire.Request{
					ID:       1,
					Command:  wire.CmdPut,
					ActionID: []byte{1},
					OutputID: []byte{2},
					BodySize: 4,
				}, []byte("test")
			},
			validate: func(t *testing.T, dec *json.Decoder, req wire.Request, tmpFilePath string) {
				var putRes wire.Response
				if err := dec.Decode(&putRes); err != nil {
					t.Fatalf("failed to decode put response: %v", err)
				}
				if putRes.Err != "" {
					t.Errorf("unexpected error in put response: %s", putRes.Err)
				}
				if putRes.ID != req.ID {
					t.Errorf("put response ID mismatch: got %d, want %d", putRes.ID, req.ID)
				}
				if putRes.DiskPath != tmpFilePath {
					t.Errorf("put response DiskPath mismatch: got %q, want %q", putRes.DiskPath, tmpFilePath)
				}
			},
			wantKnownCmd: wire.CmdPut,
		},
		{
			name: "Get",
			process: func(wg *sync.WaitGroup, started chan struct{}, tmpFilePath string) *Process {
				if err := os.WriteFile(tmpFilePath, []byte("data"), 0666); err != nil {
					t.Fatal(err)
				}
				return &Process{
					Get: func(ctx context.Context, actionID string) (outputID, diskPath string, _ error) {
						wg.Add(1)
						defer wg.Done()
						close(started)
						time.Sleep(100 * time.Millisecond)
						return "0123456789abcdef", tmpFilePath, nil
					},
				}
			},
			request: func() (wire.Request, []byte) {
				return wire.Request{
					ID:       1,
					Command:  wire.CmdGet,
					ActionID: []byte{1},
				}, nil
			},
			validate: func(t *testing.T, dec *json.Decoder, req wire.Request, tmpFilePath string) {
				var getRes wire.Response
				if err := dec.Decode(&getRes); err != nil {
					t.Fatalf("failed to decode get response: %v", err)
				}
				if getRes.Err != "" {
					t.Errorf("unexpected error in get response: %s", getRes.Err)
				}
				if getRes.ID != req.ID {
					t.Errorf("get response ID mismatch: got %d, want %d", getRes.ID, req.ID)
				}
				const mockOutputID = "0123456789abcdef"
				if hex.EncodeToString(getRes.OutputID) != mockOutputID {
					t.Errorf("get response OutputID mismatch: got %q, want %q", hex.EncodeToString(getRes.OutputID), mockOutputID)
				}
				if getRes.DiskPath != tmpFilePath {
					t.Errorf("get response DiskPath mismatch: got %q, want %q", getRes.DiskPath, tmpFilePath)
				}
			},
			wantKnownCmd: wire.CmdGet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdinReader, stdinWriter, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			origStdin := os.Stdin
			os.Stdin = stdinReader
			defer func() { os.Stdin = origStdin }()

			stdoutReader, stdoutWriter, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			origStdout := os.Stdout
			os.Stdout = stdoutWriter
			defer func() { os.Stdout = origStdout }()

			var handlerWg sync.WaitGroup
			handlerStarted := make(chan struct{})

			tmpDir := t.TempDir()
			tmpFilePath := filepath.Join(tmpDir, "cache-file")

			p := tt.process(&handlerWg, handlerStarted, tmpFilePath)

			runDone := make(chan error, 1)
			go func() {
				runDone <- p.Run()
			}()

			req, body := tt.request()

			je := json.NewEncoder(stdinWriter)
			if err := je.Encode(req); err != nil {
				t.Fatal(err)
			}
			if body != nil {
				if err := je.Encode(body); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case <-handlerStarted:
				// continue
			case <-time.After(1 * time.Second):
				t.Fatal("timed out waiting for handler to start")
			}

			// Close stdin to signal EOF, which should cause Run() to exit.
			stdinWriter.Close()

			select {
			case err := <-runDone:
				if err != nil {
					t.Errorf("p.Run() returned an error: %v", err)
				}
			case <-time.After(1 * time.Second):
				t.Fatal("p.Run() did not return after stdin was closed")
			}

			handlerWg.Wait()

			stdoutWriter.Close()
			out, _ := io.ReadAll(stdoutReader)
			dec := json.NewDecoder(bytes.NewReader(out))

			var capsRes wire.Response
			if err := dec.Decode(&capsRes); err != nil {
				t.Fatalf("failed to decode capabilities response: %v, output: %s", err, out)
			}
			if capsRes.Err != "" {
				t.Errorf("unexpected error in capabilities response: %s", capsRes.Err)
			}
			if len(capsRes.KnownCommands) != 1 || capsRes.KnownCommands[0] != tt.wantKnownCmd {
				t.Errorf("unexpected KnownCommands: got %v, want %v", capsRes.KnownCommands, []wire.Cmd{tt.wantKnownCmd})
			}

			tt.validate(t, dec, req, tmpFilePath)

			if err := dec.Decode(&struct{}{}); err != io.EOF {
				t.Fatalf("expected EOF after all responses, but got more data or error: %v", err)
			}
		})
	}
}
