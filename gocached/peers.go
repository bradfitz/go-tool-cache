// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

// This file implements opt-in LAN peering between gocached servers.
//
// A set of gocached servers on the same LAN (for example, the Mac CI hosts in
// a colo rack) can pool their caches without any client changes: each build
// client keeps talking only to the gocached on its own machine, and that
// server forwards requests it doesn't own to the peer that does. Which peer
// owns an action ID is decided by rendezvous hashing, so every server routes
// the same key to the same place and the loss of one peer only remaps that
// peer's share of the keyspace. The one exception is inline-sized objects
// (at most smallObjectSize bytes): a server that fetches one from its owner,
// or forwards one to its owner, also keeps a copy, since a small SQLite row
// is cheaper than a LAN round trip on every later read. So small objects
// live on every server that has touched them; large ones only on their
// owner.
//
// Finding peers, trusting them, and reaching them is not caching's business
// and lives elsewhere: the tagpeers package tracks which tailnet nodes carry
// the pool's tag, the lansport package turns those into pinned TLS
// transports over the LAN, and the rendezvous package picks the owner of a
// key. This file asks the rendezvous router for a transport and forwards.
// Requests that arrive from a peer are always served from local storage,
// never forwarded again, so a disagreement between two servers' views of the
// pool costs at most one extra hop and never loops.
//
// Peering runs on the wall clock (time.Now), not the server's mockable clock:
// it drives real network timeouts and its timestamps are only for display.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/tb/lansport"
	"github.com/tailscale/tb/rendezvous"
	"github.com/tailscale/tb/tagpeers"
	"tailscale.com/net/netx"
)

// peerForwardHeader is set on requests one gocached forwards to a peer. Its
// value is the forwarding server's peer name, for the peer's logs. A request
// bearing it is served from local storage without forwarding, which is
// harmless for anyone else to ask for; the real loop guard is that requests
// from peers, identified by lansport, are never forwarded.
const peerForwardHeader = "Gocached-Forwarded-By"

// peerStatusPath is the path where a peering server reports its stats to
// other peers, for their /peers debug pages. It is served to peers on the
// lansport TLS port and, for debugging, on the debug server.
const peerStatusPath = "/peer/status"

// defaultPeerAdvertAddr is where the lansport advert listener listens
// unless [PeerConfig.AdvertAddr] overrides it. Every member of a pool must
// use the same port, since discovery learns only a peer's tailnet address.
const defaultPeerAdvertAddr = ":7890"

// defaultPeerTLSAddr is where the lansport TLS listener for peers listens
// unless [PeerConfig.TLSAddr] overrides it. Peers learn the actual port from
// the advert, so it needn't match across the pool; a fixed default just
// makes firewall rules predictable.
const defaultPeerTLSAddr = ":31367"

// PeerConfig configures LAN peering between gocached servers. See
// [WithPeers]. Peering is enabled when Tag or Peers is set.
type PeerConfig struct {
	// Name identifies this server to its peers and, without a Tag, is its
	// rendezvous hashing key, so it must be unique within the pool and
	// should be stable across restarts. If empty, the hostname is used.
	Name string

	// Tag, if non-empty, is the tailnet tag (such as "tag:ci-mac-colo")
	// whose nodes form the pool, tracked through the local tailscaled.
	// Nodes are keyed for hashing by their stable tailnet node IDs.
	Tag string

	// TailscaledSocket is the path of tailscaled's LocalAPI socket, used
	// with Tag. If empty, the platform's default is used, including the
	// macOS GUI variants' socket. TailscaledDial, if non-nil, replaces how
	// tailscaled is reached altogether; it exists for tests.
	TailscaledSocket string
	TailscaledDial   func(ctx context.Context, network, addr string) (net.Conn, error)

	// AdvertAddr is the listen address of the plain-HTTP lansport advert
	// server, which peers reach over the tailnet. If empty,
	// [defaultPeerAdvertAddr] is used. AdvertListener, if non-nil, is used
	// instead.
	AdvertAddr     string
	AdvertListener net.Listener

	// TLSAddr is the listen address of the TLS server peers send forwarded
	// requests to over the LAN. If empty, [defaultPeerTLSAddr] is used.
	// TLSListener, if non-nil, is used instead.
	TLSAddr     string
	TLSListener net.Listener

	// LANIP, if non-empty, is the IPv4 address peers are told to reach the
	// TLS server at. By default it is the address of the interface with
	// the default route.
	LANIP string

	// Dial, if non-nil, replaces how peers are reached, both for fetching
	// their adverts and for forwarding to them. It exists for tests, which
	// run whole pools on an in-memory network.
	Dial netx.DialFunc

	// Weight is this server's share of the keyspace relative to peers with
	// weight 1. If zero, 1 is used.
	Weight float64

	// Peers is a static list of peer advert addresses (host:port of their
	// advert servers) to pool with instead of a Tag. They are trusted
	// because they are configured. This is for tests and for hosts without
	// a tailscaled.
	Peers []string
}

// WithPeers enables LAN peering with the given configuration. Peering is
// disabled by default and is not supported together with [WithJWTAuth].
func WithPeers(cfg PeerConfig) ServerOption {
	return func(srv *Server) {
		srv.peerCfg = &cfg
	}
}

// peerStatus is what a peering server reports at [peerStatusPath].
type peerStatus struct {
	Name      string    `json:"name"`
	ID        string    `json:"id"` // random per-process instance ID
	Version   string    `json:"version"`
	StartTime time.Time `json:"startTime"`
	Now       time.Time `json:"now"`

	// Usage is the current stored blob count and size, dead-reckoned from
	// the last shard scan. MaxSize is the configured cache size limit in
	// bytes, or 0 for none.
	Usage   countAndSize `json:"usage"`
	MaxSize int64        `json:"maxSize"`

	Gets       int64 `json:"gets"`
	GetHits    int64 `json:"getHits"`
	Puts       int64 `json:"puts"`
	PutErrs    int64 `json:"putErrs"`
	ActiveGets int64 `json:"activeGets"`
	ActivePuts int64 `json:"activePuts"`

	// Members is the sorted set of rendezvous keys (including the
	// reporting server's own) the reporting server currently routes over.
	// Comparing across peers shows whether they agree on membership.
	Members []string `json:"members"`
}

// fwdCounters counts requests this server forwarded to one peer.
type fwdCounters struct {
	gets, getHits, getErrs, puts, putErrs atomic.Int64
}

// peerSet is the server's peering state: the tracker, transport, and
// router it is built from, plus forwarding counters.
type peerSet struct {
	srv       *Server
	cfg       PeerConfig
	name      string // cfg.Name or the hostname
	id        string // random per-process instance ID
	startTime time.Time
	tracker   *tagpeers.Tracker // nil without a Tag
	ls        *lansport.Server
	router    *rendezvous.Router

	// pick chooses the owner of an action ID; it is router.Pick unless a
	// test substitutes it.
	pick func(actionID string) (lansport.Peer, bool)

	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	fwd map[lansport.Key]*fwdCounters
}

// newPeerSet starts peering for srv according to cfg. It opens the lansport
// listeners and begins discovery, so it is only called from Server.start.
func newPeerSet(srv *Server, cfg PeerConfig) (*peerSet, error) {
	if cfg.Name == "" {
		h, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("peer name not set and hostname unavailable: %w", err)
		}
		cfg.Name = h
	}
	if cfg.Tag != "" && len(cfg.Peers) > 0 {
		return nil, errors.New("a tag and a static peer list can't be combined")
	}
	if cfg.AdvertAddr == "" {
		cfg.AdvertAddr = defaultPeerAdvertAddr
	}
	if cfg.TLSAddr == "" {
		cfg.TLSAddr = defaultPeerTLSAddr
	}
	var lanIP netip.Addr
	if cfg.LANIP != "" {
		ip, err := netip.ParseAddr(cfg.LANIP)
		if err != nil {
			return nil, fmt.Errorf("bad LAN IP %q: %w", cfg.LANIP, err)
		}
		lanIP = ip
	}

	ctx, cancel := context.WithCancel(srv.shutdownCtx)
	ps := &peerSet{
		srv:       srv,
		cfg:       cfg,
		name:      cfg.Name,
		id:        randHex(8),
		startTime: time.Now(),
		cancel:    cancel,
		done:      make(chan struct{}),
		fwd:       make(map[lansport.Key]*fwdCounters),
	}
	logf := func(format string, args ...any) { srv.logf("peers: "+format, args...) }

	var source lansport.Source
	if cfg.Tag != "" {
		tr, err := tagpeers.Start(ctx, tagpeers.Config{
			Tag:    cfg.Tag,
			Socket: cfg.TailscaledSocket,
			Dial:   cfg.TailscaledDial,
			Logf:   logf,
		})
		if err != nil {
			cancel()
			return nil, err
		}
		ps.tracker = tr
		port, err := advertPort(cfg)
		if err != nil {
			ps.close()
			return nil, err
		}
		source = lansport.TailnetSource(tr, port)
	} else {
		source = lansport.StaticSource(cfg.Name, cfg.Peers...)
	}

	ls, err := lansport.Listen(lansport.Config{
		Source:         source,
		Name:           cfg.Name,
		Weight:         cfg.Weight,
		AdvertAddr:     cfg.AdvertAddr,
		AdvertListener: cfg.AdvertListener,
		TLSAddr:        cfg.TLSAddr,
		TLSListener:    cfg.TLSListener,
		LANIP:          lanIP,
		Dial:           cfg.Dial,
		Handler:        srv,
		ProbeInterval:  srv.peerPollInterval,
		// A forwarded PUT can sit in the peer's put-queue backpressure
		// before it is acknowledged.
		ResponseHeaderTimeout: 30 * time.Second,
		Logf:                  logf,
	})
	if err != nil {
		ps.close()
		return nil, err
	}
	ps.ls = ls
	ps.router = rendezvous.NewRouter(ls)
	ps.pick = ps.router.Pick
	go ps.followGauges(ctx, ls.Subscribe())
	return ps, nil
}

// advertPort returns the port peers will fetch adverts at: the advert
// listener's, or the one in the configured address.
func advertPort(cfg PeerConfig) (int, error) {
	if cfg.AdvertListener != nil {
		return cfg.AdvertListener.Addr().(*net.TCPAddr).Port, nil
	}
	_, portStr, err := net.SplitHostPort(cfg.AdvertAddr)
	if err != nil {
		return 0, fmt.Errorf("advert address %q: %w", cfg.AdvertAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return 0, fmt.Errorf("advert address %q needs a fixed port, since peers are told to use the same one", cfg.AdvertAddr)
	}
	return port, nil
}

// close stops peering: the router, the lansport listeners and probes, and
// the tracker.
func (ps *peerSet) close() {
	ps.cancel()
	if ps.router != nil {
		ps.router.Close()
	}
	if ps.ls != nil {
		ps.ls.Close()
	}
	if ps.tracker != nil {
		ps.tracker.Close()
	}
	if ps.ls != nil {
		<-ps.done
	}
}

// followGauges keeps the peer gauges current as the reachable set changes.
func (ps *peerSet) followGauges(ctx context.Context, changes <-chan struct{}) {
	defer close(ps.done)
	for {
		ps.srv.m.PeersKnown.Set(int64(len(ps.ls.Snapshot())))
		ps.srv.m.PeersHealthy.Set(int64(len(ps.ls.Peers())))
		select {
		case <-ctx.Done():
			return
		case <-changes:
		}
	}
}

// counters returns the forwarding counters for the peer with the given
// key.
func (ps *peerSet) counters(key lansport.Key) *fwdCounters {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	c, ok := ps.fwd[key]
	if !ok {
		c = &fwdCounters{}
		ps.fwd[key] = c
	}
	return c
}

// peerOwner returns the peer that r's action should be forwarded to. It
// returns false when the action should be handled locally: because peering
// is off, because the request came from a peer (or is marked as
// forwarded), because this server owns the action, or because no peer is
// reachable.
func (srv *Server) peerOwner(r *http.Request, actionID string) (lansport.Peer, bool) {
	if srv.peers == nil || r.Header.Get(peerForwardHeader) != "" {
		return lansport.Peer{}, false
	}
	if _, fromPeer := lansport.FromPeer(r); fromPeer {
		return lansport.Peer{}, false
	}
	return srv.peers.pick(actionID)
}

// forwardedHeaders is the set of response headers a proxied GET copies from
// the peer's response to the client. Everything else (Date, Server, and so
// on) is regenerated locally.
var forwardedHeaders = []string{
	"Content-Type",
	"Content-Length",
	"Content-Encoding",
	"Go-Output-Id",
	"X-Uncompressed-Length",
}

// proxyGet forwards a GET or HEAD for actionID to p and relays the response.
// Peer failures are reported to the client as a cache miss, since the client
// can't do anything more useful with an error than with a miss.
func (ps *peerSet) proxyGet(w http.ResponseWriter, r *http.Request, stats *stats, p lansport.Peer, actionID string) writeObjectResponseLabels {
	c := ps.counters(p.Key)
	ps.srv.m.PeerFwdGets.Add(1)
	c.gets.Add(1)
	miss := func(err error) writeObjectResponseLabels {
		if err != nil {
			ps.srv.m.PeerFwdGetErrs.Add(1)
			c.getErrs.Add(1)
			if ps.srv.verbose {
				ps.srv.logf("peers: GET %s from %s: %v", actionID, p.Name, err)
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
		return writeObjectResponseLabels{storage: "peer", result: "miss"}
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, p.URL("/action/"+actionID), nil)
	if err != nil {
		return miss(err)
	}
	req.Header.Set("Want-Object", "1")
	req.Header.Set(peerForwardHeader, ps.name)
	if ae := r.Header.Get("Accept-Encoding"); ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}
	res, err := p.Transport.RoundTrip(req)
	if err != nil {
		return miss(err)
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return miss(nil)
	default:
		return miss(fmt.Errorf("unexpected status %s", res.Status))
	}

	// Inline-sized objects are read in full before the response starts,
	// so a copy can be stored locally as well (see populateInline). They
	// are never lz4-encoded, so an encoded response is by definition not
	// one of them.
	var small []byte // nil unless the object is inline-sized and this is a GET
	if r.Method == "GET" && res.Header.Get("Content-Encoding") == "" {
		if n, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64); err == nil && n >= 0 && n <= smallObjectSize {
			small, err = io.ReadAll(io.LimitReader(res.Body, n+1))
			if err != nil || int64(len(small)) != n {
				return miss(fmt.Errorf("reading %d-byte inline object: got %d bytes, %v", n, len(small), err))
			}
		}
	}

	h := w.Header()
	for _, k := range forwardedHeaders {
		if v := res.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	stats.GetHits++
	c.getHits.Add(1)
	ps.srv.m.PeerFwdGetHits.Add(1)
	labels := writeObjectResponseLabels{storage: "peer", result: "get"}
	if r.Method == "HEAD" {
		labels.storage = "none"
		return labels
	}
	if small != nil {
		w.Write(small)
		stats.GetBytes += int64(len(small))
		ps.srv.blobSize.WithLabelValues(labels.storage, labels.result).Observe(float64(len(small)))
		if ps.srv.populateInline(actionID, res.Header.Get("Go-Output-Id"), small) {
			ps.srv.m.PeerFwdGetPopulated.Add(1)
		}
		return labels
	}
	n, err := io.Copy(w, res.Body)
	stats.GetBytes += n
	if err != nil {
		// Too late to change the status; the client sees a short body and
		// its length check fails, which it treats as a miss.
		if ps.srv.verbose {
			ps.srv.logf("peers: relaying %s from %s: %v", actionID, p.Name, err)
		}
		return writeObjectResponseLabels{storage: "peer", result: "error"}
	}
	ps.srv.blobSize.WithLabelValues(labels.storage, labels.result).Observe(float64(n))
	return labels
}

// populateInline stores a copy of an inline-sized object that was fetched
// from or forwarded to a peer in this server's global namespace, so the next
// read of it here is local. It reports whether the object was queued. Inline
// objects cost one small SQLite row and no disk I/O, which is cheap enough
// to duplicate on every server that touches them; larger objects stay only
// on their owner. The write is opportunistic: if the put queue's inline lane
// is full, or the action is already pending, nothing is stored.
func (srv *Server) populateInline(actionID, outputID string, data []byte) bool {
	reserved, ok := srv.putq.tryReserveInline()
	if !ok {
		return false
	}
	return srv.enqueueInline(actionID, outputID, data, reserved)
}

// storeInline is like populateInline but waits for room in the inline lane
// (or for ctx to end) rather than giving up, for when the local copy is the
// only one: a small PUT whose owner couldn't take it. An action that is
// already pending locally counts as stored.
func (srv *Server) storeInline(ctx context.Context, actionID, outputID string, data []byte) error {
	reserved, err := srv.putq.reserve(ctx, int64(len(data)))
	if err != nil {
		return err
	}
	srv.enqueueInline(actionID, outputID, data, reserved)
	return nil
}

// enqueueInline hands an inline-sized object holding an inline-lane
// reservation to the put queue, releasing the reservation if the action was
// already pending. It reports whether the object was queued.
func (srv *Server) enqueueInline(actionID, outputID string, data []byte, reserved putReservation) bool {
	if len(data) > smallObjectSize || !validHex(outputID) {
		srv.putq.unreserve(reserved)
		return false
	}
	sha256hex := fmt.Sprintf("%x", sha256.Sum256(data))
	altOutputID := ""
	if sha256hex != outputID {
		altOutputID = outputID
	}
	p := &pendingPut{
		key:              actionKey{NamespaceID: srv.globalNamespaceID, ActionID: actionID},
		sha256hex:        sha256hex,
		storedSize:       int64(len(data)),
		uncompressedSize: int64(len(data)),
		altOutputID:      altOutputID,
		createTime:       srv.now().Unix(),
		smallData:        data,
		reservation:      reserved,
	}
	if srv.putq.enqueue(p) {
		// Already pending, which means a client PUT or another forwarded
		// request got here first.
		srv.putq.unreserve(reserved)
		return false
	}
	return true
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// forwardPut sends a PUT for actionID/outputID with the given n-byte body to
// p and returns nil if the peer stored it.
func (ps *peerSet) forwardPut(ctx context.Context, p lansport.Peer, actionID, outputID string, body io.Reader, n int64) error {
	c := ps.counters(p.Key)
	ps.srv.m.PeerFwdPuts.Add(1)
	c.puts.Add(1)
	fail := func(err error) error {
		ps.srv.m.PeerFwdPutErrs.Add(1)
		c.putErrs.Add(1)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", p.URL("/"+actionID+"/"+outputID), nil)
	if err != nil {
		return fail(err)
	}
	req.ContentLength = n
	if n > 0 {
		// NopCloser keeps the transport, which always closes the request
		// body, from closing a body the caller may still need.
		req.Body = io.NopCloser(body)
	} else {
		req.Body = http.NoBody
	}
	req.Header.Set(peerForwardHeader, ps.name)

	res, err := p.Transport.RoundTrip(req)
	if err != nil {
		return fail(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fail(fmt.Errorf("peer responded %s: %s", res.Status, strings.TrimSpace(string(msg))))
	}
	return nil
}

// putForwarded records a PUT that was accepted, by the peer or locally, on
// behalf of a client and writes the 204 response.
func (ps *peerSet) putForwarded(w http.ResponseWriter, stats *stats, n int64) {
	stats.Puts++
	stats.PutsBytes += n
	ps.srv.blobSize.WithLabelValues("peer", "put").Observe(float64(n))
	w.WriteHeader(http.StatusNoContent)
}

// proxyPut streams a PUT larger than inline size for actionID/outputID to p.
// It reports whether it handled the request. It returns false, without
// having written a response or consumed any of the body, when the peer could
// not be reached before the body was touched; the caller then stores the
// object locally instead. A failure after the body has started can only be
// reported to the client as an error.
func (ps *peerSet) proxyPut(w http.ResponseWriter, r *http.Request, stats *stats, p lansport.Peer, actionID, outputID string) (handled bool) {
	cr := &countingReader{r: r.Body}
	err := ps.forwardPut(r.Context(), p, actionID, outputID, cr, r.ContentLength)
	if err == nil {
		ps.putForwarded(w, stats, r.ContentLength)
		return true
	}
	if r.Context().Err() != nil {
		stats.PutErrs++
		http.Error(w, "client went away", http.StatusServiceUnavailable)
		return true
	}
	if cr.n.Load() == 0 {
		ps.srv.m.PeerFwdPutFallbacks.Add(1)
		if ps.srv.verbose {
			ps.srv.logf("peers: PUT %s to %s failed before body was read; storing locally: %v", actionID, p.Name, err)
		}
		return false
	}
	ps.srv.logf("peers: PUT %s to %s failed mid-body: %v", actionID, p.Name, err)
	stats.PutErrs++
	http.Error(w, "peer put failed", http.StatusBadGateway)
	return true
}

// proxyPutSmall forwards an inline-sized PUT, whose whole body is in data,
// to p, and stores a copy locally as well (see populateInline). Because the
// body is buffered, a peer failure at any point falls back to storing the
// object locally, so the client only sees an error if it went away itself
// or the local put queue can't take the object before it does.
func (ps *peerSet) proxyPutSmall(w http.ResponseWriter, r *http.Request, stats *stats, p lansport.Peer, actionID, outputID string, data []byte) {
	n := int64(len(data))
	err := ps.forwardPut(r.Context(), p, actionID, outputID, bytes.NewReader(data), n)
	if err == nil {
		if ps.srv.populateInline(actionID, outputID, data) {
			ps.srv.m.PeerFwdPutPopulated.Add(1)
		}
		ps.putForwarded(w, stats, n)
		return
	}
	if r.Context().Err() != nil {
		stats.PutErrs++
		http.Error(w, "client went away", http.StatusServiceUnavailable)
		return
	}
	ps.srv.m.PeerFwdPutFallbacks.Add(1)
	if ps.srv.verbose {
		ps.srv.logf("peers: PUT %s to %s failed; storing locally: %v", actionID, p.Name, err)
	}
	if err := ps.srv.storeInline(r.Context(), actionID, outputID, data); err != nil {
		stats.PutErrs++
		http.Error(w, "canceled while awaiting queue room", http.StatusServiceUnavailable)
		return
	}
	ps.putForwarded(w, stats, n)
}

// status returns this server's own peerStatus.
func (ps *peerSet) status() *peerStatus {
	srv := ps.srv
	st := &peerStatus{
		Name:       ps.name,
		ID:         ps.id,
		Version:    buildVersion(),
		StartTime:  ps.startTime,
		Now:        time.Now(),
		MaxSize:    srv.maxSize,
		Gets:       srv.m.Gets.Value(),
		GetHits:    srv.m.GetHits.Value(),
		Puts:       srv.m.Puts.Value(),
		PutErrs:    srv.m.PutErrs.Value(),
		ActiveGets: srv.m.ActiveGets.Value(),
		ActivePuts: srv.m.ActivePuts.Value(),
		Members:    ps.router.Members(),
	}
	if u, ok := srv.liveUsage(); ok {
		st.Usage = u
	}
	return st
}

// servePeerStatus serves [peerStatusPath]. The caller has decided whether r
// may see it.
func (srv *Server) servePeerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(srv.peers.status()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// fetchPeerStatus asks a reachable peer for its status, for the /peers
// page. It gives up quickly; the page must not hang on a slow peer.
func (ps *peerSet) fetchPeerStatus(ctx context.Context, p lansport.Peer) (*peerStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", p.URL(peerStatusPath), nil)
	if err != nil {
		return nil, err
	}
	res, err := p.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", res.Status)
	}
	st := new(peerStatus)
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(st); err != nil {
		return nil, err
	}
	return st, nil
}

// servePeers serves the /peers debug page.
func (srv *Server) servePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<html><body><h1>gocached peers</h1>\n")
	ps := srv.peers
	if ps == nil {
		fmt.Fprintf(w, "<p>Peering is not enabled on this server.</p>\n")
		return
	}
	now := time.Now()
	ago := func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return durFmt(now.Sub(t).Round(time.Second)) + " ago"
	}
	esc := html.EscapeString
	adv := ps.ls.Advert()
	self := ps.ls.Self()

	fmt.Fprintf(w, "<h2>This server</h2>\n<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Name</th><td>%s</td></tr>\n", esc(ps.name))
	fmt.Fprintf(w, "<tr><th>Routing key</th><td>%s</td></tr>\n", esc(string(self.Key)))
	fmt.Fprintf(w, "<tr><th>Instance ID</th><td>%s</td></tr>\n", esc(ps.id))
	fmt.Fprintf(w, "<tr><th>Version</th><td>%s</td></tr>\n", esc(buildVersion()))
	fmt.Fprintf(w, "<tr><th>Up</th><td>%s</td></tr>\n", durFmt(now.Sub(ps.startTime).Round(time.Second)))
	fmt.Fprintf(w, "<tr><th>Advert</th><td>served on %v to tailnet-origin connections: LAN TLS at %s, certificate pin %s (new every start), weight %v</td></tr>\n",
		ps.ls.AdvertAddr(), esc(adv.IPPort), esc(adv.TLSCertHash), adv.Weight)
	if ps.tracker != nil {
		ts := ps.tracker.State()
		watching := fmt.Sprintf("<b style='color:red'>not connected</b> (%s)", esc(ts.LastError))
		if ts.Connected {
			watching = "connected " + ago(ts.Since)
		}
		events := ""
		if ts.Events > 0 {
			events = fmt.Sprintf("; %d notifications, last %s", ts.Events, ago(ts.LastEvent))
		}
		socket := ps.cfg.TailscaledSocket
		if socket == "" {
			socket = "the default socket"
		}
		fmt.Fprintf(w, "<tr><th>Discovery</th><td>tailnet nodes tagged %s, tracked via tailscaled at %s: %s%s</td></tr>\n",
			esc(ps.cfg.Tag), esc(socket), watching, events)
	} else {
		fmt.Fprintf(w, "<tr><th>Discovery</th><td>static peer list: %s</td></tr>\n", esc(strings.Join(ps.cfg.Peers, ", ")))
	}
	reachable := ps.ls.Peers()
	names := map[lansport.Key]string{self.Key: ps.name}
	for key, p := range reachable {
		names[key] = p.Name
	}
	var members []string
	for _, key := range ps.router.Members() {
		if n, ok := names[lansport.Key(key)]; ok && n != key {
			members = append(members, fmt.Sprintf("%s (%s)", esc(n), esc(key)))
		} else {
			members = append(members, esc(key))
		}
	}
	fmt.Fprintf(w, "<tr><th>Members</th><td>%s</td></tr>\n", strings.Join(members, ", "))
	fmt.Fprintf(w, "<tr><th>Forwarded</th><td>gets: %d (%d hits, of which %d small objects also stored locally; %d errors); puts: %d (%d small objects also stored locally; %d errors, %d stored locally after peer failure)</td></tr>\n",
		srv.m.PeerFwdGets.Value(), srv.m.PeerFwdGetHits.Value(), srv.m.PeerFwdGetPopulated.Value(), srv.m.PeerFwdGetErrs.Value(),
		srv.m.PeerFwdPuts.Value(), srv.m.PeerFwdPutPopulated.Value(), srv.m.PeerFwdPutErrs.Value(), srv.m.PeerFwdPutFallbacks.Value())
	fmt.Fprintf(w, "<tr><th>Served for peers</th><td>gets: %d; puts: %d</td></tr>\n",
		srv.m.PeerServedGets.Value(), srv.m.PeerServedPuts.Value())
	fmt.Fprintf(w, "</table>\n")

	snap := ps.ls.Snapshot()
	slices.SortFunc(snap, func(a, b lansport.PeerState) int {
		return strings.Compare(a.Name(), b.Name())
	})
	fmt.Fprintf(w, "<h2>Peers</h2>\n")
	if len(snap) == 0 {
		fmt.Fprintf(w, "<p>No peers known.</p>\n")
		return
	}
	fmt.Fprintf(w, "<p>%d known, %d reachable. Members that differ from ours mean that peer routes some keys differently than we do.</p>\n", len(snap), len(reachable))
	fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Name</th><th>Advert from</th><th>LAN TLS</th><th>Reachable</th><th>Pin</th><th>Weight</th><th>Version</th><th>Up</th><th>Stored</th><th>Gets (hits)</th><th>Puts (errs)</th><th>Active</th><th>Their members</th><th>Forwarded to it</th></tr>\n")
	for _, st := range snap {
		lan := ""
		if st.Advert != nil {
			lan = esc(st.Advert.IPPort)
		}
		reach := fmt.Sprintf("<b style='color:green'>yes</b> since %s<br>LAN RTT %v", ago(st.Since), st.LANRTT.Round(100*time.Microsecond))
		if !st.Reachable {
			reach = "<b style='color:red'>no</b>"
			if st.AdvertErr != "" {
				reach += "<br>advert: " + esc(st.AdvertErr)
			}
			if st.LANErr != "" {
				reach += "<br>LAN: " + esc(st.LANErr)
			}
		}
		pin, weight := "", ""
		if st.Advert != nil {
			pin = fmt.Sprintf("%s<br>%d transport builds", st.Pin.Short(), st.Rebuilds)
			weight = fmt.Sprint(st.Advert.Weight)
		}
		version, up, stored, gets, puts, active, theirs := "", "", "", "", "", "", ""
		if p, ok := reachable[st.Key()]; ok {
			if ps, err := ps.fetchPeerStatus(r.Context(), p); err != nil {
				version = "<i>" + esc(err.Error()) + "</i>"
			} else {
				version = esc(ps.Version)
				up = durFmt(ps.Now.Sub(ps.StartTime).Round(time.Second))
				stored = ps.Usage.String()
				if ps.MaxSize > 0 {
					stored += " of " + bytesFmt(ps.MaxSize)
				}
				gets = fmt.Sprintf("%d (%d)", ps.Gets, ps.GetHits)
				puts = fmt.Sprintf("%d (%d)", ps.Puts, ps.PutErrs)
				active = fmt.Sprintf("%d gets, %d puts", ps.ActiveGets, ps.ActivePuts)
				theirs = esc(strings.Join(ps.Members, ", "))
				if !slices.Equal(ps.Members, srv.peers.router.Members()) {
					theirs = "<b style='color:orange'>differs:</b> " + theirs
				}
			}
		}
		c := ps.counters(st.Key())
		fwd := fmt.Sprintf("gets: %d (%d hits, %d errs)<br>puts: %d (%d errs)",
			c.gets.Load(), c.getHits.Load(), c.getErrs.Load(), c.puts.Load(), c.putErrs.Load())
		fmt.Fprintf(w, "<tr><td>%s<br><small>%s</small></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
			esc(st.Name()), esc(string(st.Key())), esc(st.Candidate.AdvertAddr), lan, reach, pin, weight, version, up, stored, gets, puts, active, theirs, fwd)
	}
	fmt.Fprintf(w, "</table>\n")
}

// liveUsage returns the current stored count and size: the last shard scan
// aggregate dead-reckoned forward by the deltas since. It reports false if
// no aggregate is available yet.
func (srv *Server) liveUsage() (countAndSize, bool) {
	us := srv.lastUsage.Load()
	if us == nil {
		return countAndSize{}, false
	}
	dCount, dBytes := srv.sumShardDeltas()
	live := us.All()
	live.Count += dCount
	live.Size += dBytes
	return live, true
}

// buildVersion returns a short description of the running binary's version
// from its embedded build info: the main module version if it has one, else
// the VCS revision (with a "-dirty" suffix for a modified tree), else
// "unknown".
var buildVersion = sync.OnceValue(func() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if v := bi.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var rev, dirty string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return "unknown"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	return rev + dirty
})

// randHex returns n random bytes as 2n lowercase hex characters.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
