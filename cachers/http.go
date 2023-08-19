package cachers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type HTTPRemote struct {
	// BaseURL is the base URL of the cacher server, like "http://localhost:31364".
	BaseURL string

	// HTTPClient optionally specifies the http.Client to use.
	// If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// Verbose optionally specifies whether to log verbose messages.
	Verbose bool
}

var _ Upstream = (*HTTPRemote)(nil)

func (r *HTTPRemote) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return http.DefaultClient
}

func (r *HTTPRemote) GetAction(ctx context.Context, actionID string) (*ActionValue, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", r.BaseURL+"/action/"+actionID, nil)
	if err != nil {
		return nil, err
	}
	res, err := r.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected GET /action/%s status %v", actionID, res.Status)
	}
	var av ActionValue
	if err := json.NewDecoder(res.Body).Decode(&av); err != nil {
		return nil, err
	}
	return &av, nil
}

// GetOutput implements Upstream.
func (r *HTTPRemote) GetOutput(ctx context.Context, outputID string) (body io.ReadCloser, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", r.BaseURL+"/output/"+outputID, nil)
	if err != nil {
		return nil, err
	}
	res, err := r.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
	}
	if res.ContentLength == -1 {
		return nil, fmt.Errorf("no Content-Length from server")
	}
	return res.Body, nil // let caller to close body
}

func (r *HTTPRemote) Put(
	ctx context.Context,
	actionID string,
	outputID string,
	size int64,
	body io.Reader,
) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", r.BaseURL+"/"+actionID+"/"+outputID, body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	res, err := r.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		all, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		return fmt.Errorf("unexpected PUT /%s/%s status %v: %s", actionID, outputID, res.Status, all)
	}
	return nil
}