package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// ActionValue is the JSON value returned by the cacher server for an GET /action request.
type ActionValue struct {
	OutputID string `json:"outputID"`
	Size     int64  `json:"size"`
}

// HTTPCache is a RemoteCache that talks to a cacher server over HTTP.
type HTTPCache struct {
	// baseURL is the base URL of the cacher server, like "http://localhost:31364".
	baseURL string

	// client optionally specifies the http.Client to use.
	// If nil, http.DefaultClient is used.
	client *http.Client

	// verbose optionally specifies whether to log verbose messages.
	verbose bool
}

func NewHttpCache(baseURL string, verbose bool) *HTTPCache {
	return &HTTPCache{
		baseURL: baseURL,
		verbose: verbose,
	}
}

func (c *HTTPCache) Start(context.Context) error {
	if c.verbose {
		log.Printf("[%s]\tconfigured to %s", c.Kind(), c.baseURL)
	}
	return nil
}

func (c *HTTPCache) Close() error {
	return nil
}

func (c *HTTPCache) Kind() string {
	return "http"
}

func (c *HTTPCache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/action/"+actionID, nil)
	res, err := c.httpClient().Do(req)
	if err != nil {
		return "", 0, nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return "", 0, nil, nil
	}
	if res.StatusCode != http.StatusOK {
		return "", 0, nil, fmt.Errorf("unexpected GET /action/%s status %v", actionID, res.Status)
	}
	var av ActionValue
	if err := json.NewDecoder(res.Body).Decode(&av); err != nil {
		return "", 0, nil, err
	}
	outputID = av.OutputID
	if av.Size == 0 {
		return outputID, av.Size, io.NopCloser(bytes.NewReader(nil)), nil
	}
	req, _ = http.NewRequestWithContext(ctx, "GET", c.baseURL+"/output/"+outputID, nil)
	res, err = c.httpClient().Do(req)
	if err != nil {
		return "", 0, nil, err
	}
	if res.StatusCode == http.StatusNotFound {
		return "", 0, nil, nil
	}
	if res.StatusCode != http.StatusOK {
		return "", 0, nil, fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
	}
	if res.ContentLength == -1 {
		return "", 0, nil, fmt.Errorf("no Content-Length from server")
	}
	return outputID, av.Size, res.Body, nil

}

func (c *HTTPCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {
		putBody = body
	}
	req, _ := http.NewRequestWithContext(ctx, "PUT", c.baseURL+"/"+actionID+"/"+outputID, putBody)
	req.ContentLength = size
	res, err := c.httpClient().Do(req)
	if err != nil {
		log.Printf("error PUT /%s/%s: %v", actionID, outputID, err)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		all, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		return fmt.Errorf("unexpected PUT /%s/%s status %v: %s", actionID, outputID, res.Status, all)
	}
	return nil
}

var _ RemoteCache = &HTTPCache{}

func (c *HTTPCache) httpClient() *http.Client {
	if c.client != nil {
		return c.client
	}
	return http.DefaultClient
}
