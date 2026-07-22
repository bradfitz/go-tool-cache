package cachers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pierrec/lz4/v4"
)

func lz4Compress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if err := w.Apply(lz4.SizeOption(uint64(len(data)))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	compressed := buf.Bytes()

	// Verify the lz4 frame header contains the uncompressed content size
	// at bytes [6:14] (little-endian uint64), after the 4-byte magic number
	// and 2-byte FLG+BD descriptor.
	if len(compressed) < 14 {
		t.Fatalf("compressed output too short: %d bytes", len(compressed))
	}
	gotSize := binary.LittleEndian.Uint64(compressed[6:14])
	if gotSize != uint64(len(data)) {
		t.Fatalf("lz4 frame content size = %d, want %d", gotSize, len(data))
	}

	return compressed
}

func TestHTTPClientGetLZ4(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("hello, this is cached build output data for testing")

	tests := []struct {
		name     string
		compress bool
		oldProto bool
	}{
		{"new_protocol_uncompressed", false, false},
		{"new_protocol_lz4", true, false},
		{"old_protocol_uncompressed", false, true},
		{"old_protocol_lz4", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotAcceptEncoding []string

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAcceptEncoding = append(gotAcceptEncoding, r.Header.Get("Accept-Encoding"))

				switch {
				case tt.oldProto && r.URL.Path == "/action/"+testActionID:
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(ActionValue{
						OutputID: testOutputID,
						Size:     int64(len(testData)),
					})

				case tt.oldProto && r.URL.Path == "/output/"+testOutputID:
					body := testData
					if tt.compress {
						body = lz4Compress(t, testData)
						w.Header().Set("Content-Encoding", "lz4")
						w.Header().Set("X-Uncompressed-Length", strconv.Itoa(len(testData)))
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					w.Write(body)

				case !tt.oldProto && r.URL.Path == "/action/"+testActionID:
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Go-Output-Id", testOutputID)
					body := testData
					if tt.compress {
						body = lz4Compress(t, testData)
						w.Header().Set("Content-Encoding", "lz4")
						w.Header().Set("X-Uncompressed-Length", strconv.Itoa(len(testData)))
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					w.Write(body)

				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()

			hc := &HTTPClient{
				BaseURL: ts.URL,
				Disk:    &DiskCache{Dir: t.TempDir()},
			}

			outputID, diskPath, err := hc.Get(context.Background(), testActionID)
			if err != nil {
				t.Fatal(err)
			}
			if outputID != testOutputID {
				t.Errorf("outputID = %q, want %q", outputID, testOutputID)
			}
			if diskPath == "" {
				t.Fatal("diskPath is empty")
			}

			got, err := os.ReadFile(diskPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, testData) {
				t.Errorf("disk content = %q, want %q", got, testData)
			}

			// Verify Accept-Encoding: lz4 was sent on all requests.
			if len(gotAcceptEncoding) == 0 {
				t.Fatal("no requests received by server")
			}
			for i, ae := range gotAcceptEncoding {
				if ae != "lz4" {
					t.Errorf("request %d: Accept-Encoding = %q, want %q", i, ae, "lz4")
				}
			}
			if tt.oldProto && len(gotAcceptEncoding) != 2 {
				t.Errorf("old protocol: got %d requests, want 2", len(gotAcceptEncoding))
			}
		})
	}
}

// TestHTTPClientPutServerRejectsBody verifies that Put still writes to disk
// when the server returns 403 without reading the request body.
func TestHTTPClientPutServerRejectsBody(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)

	largeBody := bytes.Repeat([]byte("x"), 512<<10)

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("hello, this is cached build output data for testing")},
		{"512k", largeBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Return 403 immediately without reading the request body.
				http.Error(w, "forbidden", http.StatusForbidden)
			}))
			defer ts.Close()

			hc := &HTTPClient{
				BaseURL: ts.URL,
				Disk:    &DiskCache{Dir: t.TempDir()},
			}

			diskPath, err := hc.Put(context.Background(), testActionID, testOutputID, int64(len(tt.data)), bytes.NewReader(tt.data))
			if diskPath == "" {
				t.Fatalf("diskPath is empty; err = %v", err)
			}

			got, err := os.ReadFile(diskPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tt.data) {
				t.Errorf("disk content length = %d, want %d", len(got), len(tt.data))
			}
		})
	}
}

// blockingPutTransport is a fake http.RoundTripper whose PUTs block until
// release is closed. It counts how many round trips have started, letting tests
// observe in-flight uploads without a real network or wall-clock timing. It is
// safe to use inside a synctest bubble because it performs no real I/O.
type blockingPutTransport struct {
	started atomic.Int64
	release chan struct{}
}

func newBlockingPutTransport() *blockingPutTransport {
	return &blockingPutTransport{
		release: make(chan struct{}),
	}
}

func (tr *blockingPutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.started.Add(1)
	select {
	case <-tr.release:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	io.Copy(io.Discard, req.Body)
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Body:       http.NoBody,
		Header:     make(http.Header),
	}, nil
}

// TestHTTPClientAsyncPutConcurrencyLimit verifies that no more than
// AsyncPutMaxConcurrent uploads run at once, and that PUTs beyond the cap are
// queued.
func TestHTTPClientAsyncPutConcurrencyLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newBlockingPutTransport()
		hc := &HTTPClient{
			BaseURL:               "http://fake",
			Disk:                  &DiskCache{Dir: t.TempDir()},
			HTTPClient:            &http.Client{Transport: tr},
			BestEffortHTTP:        true,
			AsyncPutMaxConcurrent: 2,
		}

		data := []byte("some cached output")
		for i := range 5 {
			id := strconv.Itoa(i)
			diskPath, err := hc.Put(t.Context(), "aaaa"+id, "bbbb"+id, int64(len(data)), bytes.NewReader(data))
			if err != nil {
				t.Fatalf("Put %d returned error: %v", i, err)
			}
			if diskPath == "" {
				t.Fatalf("Put %d returned empty diskPath", i)
			}
		}

		// Only 2 uploads may run at once; the other 3 wait for a slot.
		synctest.Wait()
		if got := tr.started.Load(); got != 2 {
			t.Errorf("started %d uploads, want 2", got)
		}

		// Releasing the blocked uploads lets the queued ones drain until all 5
		// have run.
		close(tr.release)
		synctest.Wait()
		if got := tr.started.Load(); got != 5 {
			t.Errorf("started %d uploads after release, want 5", got)
		}
	})
}

// TestHTTPClientShutdownDrains verifies Shutdown blocks until in-flight PUTs
// finish and returns true when they drain in time.
func TestHTTPClientShutdownDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newBlockingPutTransport()
		hc := &HTTPClient{
			BaseURL:         "http://fake",
			Disk:            &DiskCache{Dir: t.TempDir()},
			HTTPClient:      &http.Client{Transport: tr},
			BestEffortHTTP:  true,
			AsyncPutTimeout: 2 * time.Second,
		}

		data := []byte("some cached output")
		if _, err := hc.Put(t.Context(), "aabbccdd", "eeff0011", int64(len(data)), bytes.NewReader(data)); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		synctest.Wait()
		if got := tr.started.Load(); got != 1 {
			t.Fatalf("started %d uploads, want 1", got)
		}

		shutdownReturned := make(chan bool, 1)
		go func() {
			shutdownReturned <- hc.Shutdown()
		}()

		// With the upload still blocked, Shutdown must not return.
		synctest.Wait()
		select {
		case <-shutdownReturned:
			t.Fatal("Shutdown returned before the in-flight PUT finished")
		default:
		}

		close(tr.release)
		synctest.Wait()
		select {
		case drained := <-shutdownReturned:
			if !drained {
				t.Error("Shutdown returned drained=false, want true")
			}
		default:
			t.Fatal("Shutdown did not return after the PUT was released")
		}
	})
}

// TestHTTPClientShutdownCancelsStragglers verifies that when the drain deadline
// elapses, Shutdown returns false and cancels the context shared by background
// PUTs, so an upload blocked past the deadline is abandoned rather than left to
// outlive Shutdown.
func TestHTTPClientShutdownCancelsStragglers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newBlockingPutTransport() // never released
		hc := &HTTPClient{
			BaseURL:         "http://fake",
			Disk:            &DiskCache{Dir: t.TempDir()},
			HTTPClient:      &http.Client{Transport: tr},
			BestEffortHTTP:  true,
			AsyncPutTimeout: 2 * time.Second,
		}

		data := []byte("some cached output")
		if _, err := hc.Put(t.Context(), "aabbccdd", "eeff0011", int64(len(data)), bytes.NewReader(data)); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		synctest.Wait()
		if got := tr.started.Load(); got != 1 {
			t.Fatalf("started %d uploads, want 1", got)
		}

		// The upload never unblocks, so Shutdown must hit its deadline, cancel
		// the shared context, and report that it did not drain.
		if drained := hc.Shutdown(); drained {
			t.Error("Shutdown returned drained=true, want false")
		}

		// Cancelling the shared context unblocks the straggler, so the
		// background goroutine exits rather than leaking.
		synctest.Wait()
	})
}

func TestHTTPClientBestEffort(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("hello, this is cached build output data for testing")

	// closedPortURL returns a URL pointing at a TCP port that immediately
	// refuses connections (RST).
	closedPortURL := func(t *testing.T) string {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		return "http://" + addr
	}

	tests := []struct {
		name    string
		httpErr string // "rst" or "500"
	}{
		{"connection_refused", "rst"},
		{"server_500", "500"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var baseURL string
			if tt.httpErr == "rst" {
				baseURL = closedPortURL(t)
			} else {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}))
				defer ts.Close()
				baseURL = ts.URL
			}

			t.Run("Get", func(t *testing.T) {
				hc := &HTTPClient{
					BaseURL:        baseURL,
					Disk:           &DiskCache{Dir: t.TempDir()},
					BestEffortHTTP: true,
				}

				outputID, diskPath, err := hc.Get(context.Background(), testActionID)
				if err != nil {
					t.Fatalf("BestEffortHTTP Get returned error: %v", err)
				}
				if outputID != "" || diskPath != "" {
					t.Errorf("expected cache miss, got outputID=%q, diskPath=%q", outputID, diskPath)
				}
			})

			t.Run("Get_without_best_effort", func(t *testing.T) {
				hc := &HTTPClient{
					BaseURL:        baseURL,
					Disk:           &DiskCache{Dir: t.TempDir()},
					BestEffortHTTP: false,
				}

				_, _, err := hc.Get(context.Background(), testActionID)
				if err == nil {
					t.Fatal("expected error from Get without BestEffortHTTP, got nil")
				}
			})

			t.Run("Put", func(t *testing.T) {
				hc := &HTTPClient{
					BaseURL:        baseURL,
					Disk:           &DiskCache{Dir: t.TempDir()},
					BestEffortHTTP: true,
				}

				diskPath, err := hc.Put(context.Background(), testActionID, testOutputID, int64(len(testData)), bytes.NewReader(testData))
				if err != nil {
					t.Fatalf("BestEffortHTTP Put returned error: %v", err)
				}
				if diskPath == "" {
					t.Fatal("diskPath is empty")
				}

				got, err := os.ReadFile(diskPath)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, testData) {
					t.Errorf("disk content = %q, want %q", got, testData)
				}
			})

			t.Run("Put_without_best_effort", func(t *testing.T) {
				hc := &HTTPClient{
					BaseURL:        baseURL,
					Disk:           &DiskCache{Dir: t.TempDir()},
					BestEffortHTTP: false,
				}

				_, err := hc.Put(context.Background(), testActionID, testOutputID, int64(len(testData)), bytes.NewReader(testData))
				if err == nil {
					t.Fatal("expected error from Put without BestEffortHTTP, got nil")
				}
			})
		})
	}
}
