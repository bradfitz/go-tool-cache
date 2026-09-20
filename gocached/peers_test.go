// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bradfitz/go-tool-cache/cachers"
	"github.com/tailscale/tb/lansport"
	"github.com/tailscale/tb/lansport/lansporttest"
	"github.com/tailscale/tb/tagpeers/tagpeerstest"
)

// The peering tests run whole pools under testing/synctest on an in-memory
// LAN: each server lives at its own loopback-range address with the build
// port, the advert port, and the peer TLS port at fixed port numbers, as
// on a real host. Probe intervals are virtual, so waiting a round costs
// nothing.

const (
	testBuildPort  = "31364"
	testAdvertPort = "7890"
	testTLSPort    = "31367"
	probeInterval  = time.Second
)

var (
	ipA = netip.MustParseAddr("127.0.0.1")
	ipB = netip.MustParseAddr("127.0.0.2")
	ipC = netip.MustParseAddr("127.0.0.3")
)

// sleepAndWait advances the bubble's clock by d and then waits for every
// goroutine the passage of time woke to finish what it was doing, so the
// caller can assert on the resulting state. Callers say what d is for.
func sleepAndWait(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

// withPeerPollInterval overrides how often peers are probed.
func withPeerPollInterval(d time.Duration) ServerOption {
	return func(srv *Server) {
		srv.peerPollInterval = d
	}
}

// peerTester is a gocached server with peering enabled on the test LAN,
// plus a client on the same host to drive it, like a build guest would.
type peerTester struct {
	t      testing.TB
	srv    *Server
	ip     netip.Addr
	base   string // build-facing URL
	client *http.Client
	hs     *http.Server
}

// newPeerTester starts a peering server at ip on lan. cfg.Name is required;
// the listeners, LAN address, and dialer are filled in from ip.
func newPeerTester(t testing.TB, lan *lansporttest.LAN, ip netip.Addr, cfg PeerConfig) *peerTester {
	t.Helper()
	cfg.AdvertListener = lan.Listen(net.JoinHostPort(ip.String(), testAdvertPort))
	cfg.TLSListener = lan.Listen(net.JoinHostPort(ip.String(), testTLSPort))
	cfg.LANIP = ip.String()
	cfg.Dial = lan.DialFrom(ip)
	srv, err := NewServer(
		WithDir(t.TempDir()),
		WithLogf(func(format string, args ...any) { t.Logf(cfg.Name+": "+format, args...) }),
		WithVerbose(true),
		withClock(func() time.Time { return time.Unix(1234, 0) }),
		withoutBackgroundLoops(),
		WithShardPrefixLen(1),
		WithPeers(cfg),
		withPeerPollInterval(probeInterval),
	)
	if err != nil {
		t.Fatalf("starting gocached %q: %v", cfg.Name, err)
	}
	pt := &peerTester{
		t:      t,
		srv:    srv,
		ip:     ip,
		base:   "http://" + net.JoinHostPort(ip.String(), testBuildPort),
		client: &http.Client{Transport: &http.Transport{DialContext: lan.DialFrom(ip)}},
		hs:     &http.Server{Handler: srv},
	}
	go pt.hs.Serve(lan.Listen(net.JoinHostPort(ip.String(), testBuildPort)))
	t.Cleanup(pt.close)
	return pt
}

// close stops the server; it is safe to call more than once.
func (pt *peerTester) close() {
	pt.hs.Close()
	pt.srv.Close()
	pt.client.CloseIdleConnections()
}

// name returns the server's peer name.
func (pt *peerTester) name() string { return pt.srv.peers.name }

// advertAddr returns the server's advert address, for other servers' static
// peer lists.
func (pt *peerTester) advertAddr() string {
	return net.JoinHostPort(pt.ip.String(), testAdvertPort)
}

// cacheClient returns a real cachers client pointed at the server, for
// exercising lz4 negotiation and the on-disk output path.
func (pt *peerTester) cacheClient() *cachers.HTTPClient {
	return &cachers.HTTPClient{
		BaseURL:    pt.base,
		HTTPClient: pt.client,
		Disk: &cachers.DiskCache{
			Dir:  pt.t.TempDir(),
			Logf: func(format string, args ...any) { pt.t.Logf("client-disk: "+format, args...) },
		},
	}
}

// testObject is a synthetic cache entry for peer tests.
type testObject struct {
	actionID string
	outputID string
	body     []byte
}

// mkTestObjects returns n objects whose bodies cycle through inline-sized,
// disk-sized (lz4-compressed on disk), and empty, so every storage path gets
// forwarded.
func mkTestObjects(n int) []testObject {
	objs := make([]testObject, n)
	for i := range objs {
		action := sha256.Sum256(fmt.Appendf(nil, "action-%d", i))
		var body []byte
		switch i % 3 {
		case 0:
			body = fmt.Appendf(nil, "small body %d", i)
		case 1:
			body = bytes.Repeat(fmt.Appendf(nil, "big body %d ", i), 500)
		}
		out := sha256.Sum256(body)
		objs[i] = testObject{
			actionID: fmt.Sprintf("%x", action),
			outputID: fmt.Sprintf("%x", out),
			body:     body,
		}
	}
	return objs
}

// put PUTs obj to the server and returns the status code.
func (pt *peerTester) put(obj testObject, hdr http.Header) int {
	pt.t.Helper()
	var body io.Reader = bytes.NewReader(obj.body)
	if len(obj.body) == 0 {
		body = http.NoBody // so the client sends Content-Length: 0 rather than chunking
	}
	req, err := http.NewRequest("PUT", pt.base+"/"+obj.actionID+"/"+obj.outputID, body)
	if err != nil {
		pt.t.Fatal(err)
	}
	req.ContentLength = int64(len(obj.body))
	maps.Copy(req.Header, hdr)
	res, err := pt.client.Do(req)
	if err != nil {
		pt.t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	return res.StatusCode
}

// get GETs actionID from the server without lz4 negotiation and returns the
// status, Go-Output-Id, and body.
func (pt *peerTester) get(actionID string, hdr http.Header) (status int, outputID string, body []byte) {
	pt.t.Helper()
	req, err := http.NewRequest("GET", pt.base+"/action/"+actionID, nil)
	if err != nil {
		pt.t.Fatal(err)
	}
	req.Header.Set("Want-Object", "1")
	maps.Copy(req.Header, hdr)
	res, err := pt.client.Do(req)
	if err != nil {
		pt.t.Fatal(err)
	}
	defer res.Body.Close()
	body, err = io.ReadAll(res.Body)
	if err != nil {
		pt.t.Fatal(err)
	}
	return res.StatusCode, res.Header.Get("Go-Output-Id"), body
}

// localOnly is the header that makes a peering server serve a request from
// its own storage without forwarding.
var localOnly = http.Header{peerForwardHeader: {"test"}}

// membersAre reports whether ps routes over exactly the given keys.
func membersAre(ps *peerSet, want ...string) bool {
	return strings.Join(ps.router.Members(), ",") == strings.Join(want, ",")
}

// ownedBy reports whether ps would forward actionID to the peer named name.
func ownedBy(ps *peerSet, actionID, name string) bool {
	p, ok := ps.pick(actionID)
	return ok && p.Name == name
}

// newPeerPair starts two peering servers, a at ipA and b at ipB, that list
// each other as static peers, and lets them reach each other.
func newPeerPair(t testing.TB, lan *lansporttest.LAN) (a, b *peerTester) {
	t.Helper()
	advA := net.JoinHostPort(ipA.String(), testAdvertPort)
	advB := net.JoinHostPort(ipB.String(), testAdvertPort)
	a = newPeerTester(t, lan, ipA, PeerConfig{Name: "a", Peers: []string{advB}})
	b = newPeerTester(t, lan, ipB, PeerConfig{Name: "b", Peers: []string{advA}})
	// Each server probes once at start and then every probeInterval. At
	// start, a can't reach b's advert (b doesn't exist yet) and b fetches
	// a's advert but a doesn't yet know b's pin, so neither is reachable.
	// One interval later a fetches b's advert and its LAN check passes,
	// since b knows a; b's LAN check of a races a's fetch, so it may need
	// the interval after that. Two intervals is therefore enough for both.
	sleepAndWait(2 * probeInterval)
	if !membersAre(a.srv.peers, "a", "b") || !membersAre(b.srv.peers, "a", "b") {
		t.Fatalf("a routes over %v, b over %v; want both [a b]", a.srv.peers.router.Members(), b.srv.peers.router.Members())
	}
	return a, b
}

func TestPeeringForwardsToOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lan := new(lansporttest.LAN)
		a, b := newPeerPair(t, lan)

		// Every PUT goes through a; each object should end up on its
		// owner, and small ones also on a, which forwarded them.
		objs := mkTestObjects(40)
		onA, onB := 0, 0
		smallOnA, smallOnB := 0, 0 // inline-sized objects per owner
		var bigBOwned *testObject  // a disk-sized object b owns
		for i, o := range objs {
			if code := a.put(o, nil); code != http.StatusNoContent {
				t.Fatalf("PUT %s via a: status %d", o.actionID, code)
			}
			ownerIsB := ownedBy(a.srv.peers, o.actionID, "b")
			small := len(o.body) <= smallObjectSize
			owner, other := a, b
			if ownerIsB {
				owner, other = b, a
				onB++
				if small {
					smallOnB++
				} else if bigBOwned == nil {
					bigBOwned = &objs[i]
				}
			} else {
				onA++
				if small {
					smallOnA++
				}
			}
			if code, _, _ := owner.get(o.actionID, localOnly); code != http.StatusOK {
				t.Errorf("object %s not stored on its owner: status %d", o.actionID, code)
			}
			code, _, _ := other.get(o.actionID, localOnly)
			if wantOnOther := small && other == a; wantOnOther && code != http.StatusOK {
				t.Errorf("small object %s forwarded by a not also stored on a: status %d", o.actionID, code)
			} else if !wantOnOther && code != http.StatusNotFound {
				t.Errorf("object %s (%d bytes) also stored on non-owner %s: status %d", o.actionID, len(o.body), other.name(), code)
			}
		}
		if onA == 0 || onB == 0 || smallOnA == 0 || smallOnB == 0 || bigBOwned == nil {
			t.Fatalf("keys split a=%d (%d small) b=%d (%d small), big b-owned: %v; want all non-zero", onA, smallOnA, onB, smallOnB, bigBOwned != nil)
		}
		t.Logf("keys split a=%d (%d small) b=%d (%d small)", onA, smallOnA, onB, smallOnB)

		// Reads through either server find everything, with a's half
		// proxied by b and vice versa. Use the real client, so lz4
		// negotiation across the proxy hop gets exercised for the
		// disk-sized objects too.
		for _, pt := range []*peerTester{a, b} {
			c := pt.cacheClient()
			for _, o := range objs {
				outputID, diskPath, err := c.Get(context.Background(), o.actionID)
				if err != nil {
					t.Fatalf("GET %s via %s: %v", o.actionID, pt.name(), err)
				}
				if outputID != o.outputID {
					t.Fatalf("GET %s via %s: outputID %q, want %q", o.actionID, pt.name(), outputID, o.outputID)
				}
				got, err := os.ReadFile(diskPath)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, o.body) {
					t.Fatalf("GET %s via %s: body mismatch", o.actionID, pt.name())
				}
			}
		}

		// Reading through populated the reader with the small objects it
		// fetched from the owner, and only those; a second read is local.
		for _, o := range objs {
			ownerIsB := ownedBy(a.srv.peers, o.actionID, "b")
			small := len(o.body) <= smallObjectSize
			for _, pt := range []*peerTester{a, b} {
				isOwner := (pt == b) == ownerIsB
				code, outputID, body := pt.get(o.actionID, localOnly)
				wantLocal := isOwner || small
				if wantLocal && (code != http.StatusOK || outputID != o.outputID || !bytes.Equal(body, o.body)) {
					t.Errorf("%s: object %s (%d bytes, owner is b: %v) not stored locally: status %d", pt.name(), o.actionID, len(o.body), ownerIsB, code)
				}
				if !wantLocal && code != http.StatusNotFound {
					t.Errorf("%s: big object %s owned elsewhere is stored locally: status %d", pt.name(), o.actionID, code)
				}
			}
		}
		// a wrote everything, so it kept every small b-owned object at PUT
		// time and had nothing left to populate on read; b never forwarded
		// a PUT and populated a-owned small objects as it read them.
		for _, c := range []struct {
			name      string
			got, want int64
		}{
			{"a put-populated", a.srv.m.PeerFwdPutPopulated.Value(), int64(smallOnB)},
			{"a get-populated", a.srv.m.PeerFwdGetPopulated.Value(), 0},
			{"b put-populated", b.srv.m.PeerFwdPutPopulated.Value(), 0},
			{"b get-populated", b.srv.m.PeerFwdGetPopulated.Value(), int64(smallOnA)},
			// The forwarding counters line up on both sides. a's
			// forwarded reads are only b's big objects, since it
			// already held the small ones.
			{"a forwarded puts", a.srv.m.PeerFwdPuts.Value(), int64(onB)},
			{"b served forwarded puts", b.srv.m.PeerServedPuts.Value(), int64(onB)},
			{"a forwarded get hits", a.srv.m.PeerFwdGetHits.Value(), int64(onB - smallOnB)},
			{"b forwarded get hits", b.srv.m.PeerFwdGetHits.Value(), int64(onA)},
			{"a peers known", a.srv.m.PeersKnown.Value(), 1},
			{"a peers healthy", a.srv.m.PeersHealthy.Value(), 1},
		} {
			if c.got != c.want {
				t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
			}
		}
		if got := a.srv.m.PeerFwdGetErrs.Value() + a.srv.m.PeerFwdPutErrs.Value() + b.srv.m.PeerFwdGetErrs.Value() + b.srv.m.PeerFwdPutErrs.Value(); got != 0 {
			t.Errorf("%d forwarding errors, want 0", got)
		}

		// A miss stays a miss, and is forwarded to the owner once.
		missObj := mkTestObjects(41)[40]
		if code, _, _ := a.get(missObj.actionID, nil); code != http.StatusNotFound {
			t.Fatalf("GET of never-stored object: status %d, want 404", code)
		}

		// The status JSON (on the debug server) and /peers page reflect
		// the pairing; the build-facing port doesn't serve status.
		rec := httptest.NewRecorder()
		a.srv.ServeHTTPDebug(rec, httptest.NewRequest("GET", peerStatusPath, nil))
		var st peerStatus
		if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
			t.Fatal(err)
		}
		if st.Name != "a" || strings.Join(st.Members, ",") != "a,b" || st.Puts != int64(len(objs)) {
			t.Errorf("a's status = %+v", st)
		}
		if res, err := a.client.Get(a.base + peerStatusPath); err != nil {
			t.Fatal(err)
		} else if res.Body.Close(); res.StatusCode != http.StatusNotFound {
			t.Errorf("status on the build-facing port: %d, want 404", res.StatusCode)
		}
		rec = httptest.NewRecorder()
		a.srv.ServeHTTPDebug(rec, httptest.NewRequest("GET", "/peers", nil))
		page := rec.Body.String()
		pb := a.srv.peers.ls.Peers()["b"]
		for _, want := range []string{"<td>b<br>", "color:green'>yes", pb.Addr, "a, b", b.srv.peers.ls.Identity().Pin().Short()} {
			if !strings.Contains(page, want) {
				t.Errorf("/peers page missing %q:\n%s", want, page)
			}
		}

		// Take b away. a's next probe, one interval on, fails to fetch b's
		// advert and drops it, so a's members shrink to itself, b's big
		// objects become misses (its small ones were copied to a by the
		// reads above), and new PUTs that b would have owned land on a.
		b.close()
		sleepAndWait(probeInterval)
		if !membersAre(a.srv.peers, "a") {
			t.Fatalf("a still routes over %v after b closed", a.srv.peers.router.Members())
		}
		if code, _, _ := a.get(bigBOwned.actionID, nil); code != http.StatusNotFound {
			t.Errorf("GET of big b-owned object with b down: status %d, want 404", code)
		}
		if code := a.put(*bigBOwned, nil); code != http.StatusNoContent {
			t.Fatalf("PUT with b down: status %d", code)
		}
		if code, _, _ := a.get(bigBOwned.actionID, localOnly); code != http.StatusOK {
			t.Errorf("object PUT while b was down not stored locally on a: status %d", code)
		}
	})
}

// ownedObjects returns one inline-sized and one disk-sized test object that
// ps would forward to the peer named name.
func ownedObjects(t testing.TB, ps *peerSet, name string) (small, big testObject) {
	t.Helper()
	var haveSmall, haveBig bool
	for _, o := range mkTestObjects(200) {
		if !ownedBy(ps, o.actionID, name) {
			continue
		}
		if len(o.body) > smallObjectSize {
			big, haveBig = o, true
		} else if len(o.body) > 0 {
			small, haveSmall = o, true
		}
		if haveSmall && haveBig {
			return small, big
		}
	}
	t.Fatalf("no small and big objects owned by %s found", name)
	panic("unreachable")
}

// TestPeeringUnreachableOwner covers forwards to a peer that the router
// still names but that can't be reached, as happens in the moments between
// a peer dying and lansport noticing: GETs become misses and PUTs are stored
// locally, so the client sees a miss or success, never an error.
func TestPeeringUnreachableOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lan := new(lansporttest.LAN)
		a := newPeerTester(t, lan, ipA, PeerConfig{Name: "a"})
		ps := a.srv.peers

		// Nothing listens at the dead peer's address; the transport is
		// pinned to a certificate nothing will ever present, which
		// doesn't matter since nothing answers.
		pin := lansport.Pin(sha256.Sum256([]byte("dead")))
		deadPeer := lansport.Peer{
			Key: "dead", Name: "dead", Addr: net.JoinHostPort(ipC.String(), testTLSPort), Weight: 1,
			Transport: &http.Transport{
				DialContext:     lan.DialFrom(ipA),
				TLSClientConfig: ps.ls.Identity().ClientTLSConfig(&pin),
			},
		}
		// Route everything to the dead peer.
		ps.pick = func(string) (lansport.Peer, bool) { return deadPeer, true }
		small, big := mkTestObjects(2)[0], mkTestObjects(2)[1]

		if code, _, _ := a.get(big.actionID, nil); code != http.StatusNotFound {
			t.Fatalf("GET: status %d, want 404", code)
		}
		if got := a.srv.m.PeerFwdGetErrs.Value(); got != 1 {
			t.Errorf("PeerFwdGetErrs = %d, want 1", got)
		}

		// The forward fails before any body is read (for the big object)
		// or at all (for the small one), and the object is stored locally
		// either way.
		for i, o := range []testObject{small, big} {
			if code := a.put(o, nil); code != http.StatusNoContent {
				t.Fatalf("PUT %d bytes: status %d, want 204", len(o.body), code)
			}
			if got, want := a.srv.m.PeerFwdPutFallbacks.Value(), int64(i+1); got != want {
				t.Errorf("PeerFwdPutFallbacks = %d, want %d", got, want)
			}
			if code, outputID, body := a.get(o.actionID, localOnly); code != http.StatusOK || outputID != o.outputID || !bytes.Equal(body, o.body) {
				t.Errorf("%d-byte object not stored locally after fallback: status %d, outputID %q", len(o.body), code, outputID)
			}
		}
	})
}

// TestPeeringOwnerRejectsPut covers an owner that is reachable but fails the
// PUT after reading the body. A small object, whose body was buffered, is
// stored locally and the client sees success; a big one, already streamed,
// can only be reported as an error.
func TestPeeringOwnerRejectsPut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lan := new(lansporttest.LAN)
		fakeAdvert := net.JoinHostPort(ipC.String(), testAdvertPort)
		a := newPeerTester(t, lan, ipA, PeerConfig{Name: "a", Peers: []string{fakeAdvert}})
		fake, err := lansport.Listen(lansport.Config{
			Source:         lansport.StaticSource("fake", a.advertAddr()),
			Name:           "fake",
			AdvertListener: lan.Listen(fakeAdvert),
			TLSListener:    lan.Listen(net.JoinHostPort(ipC.String(), testTLSPort)),
			LANIP:          ipC,
			Dial:           lan.DialFrom(ipC),
			ProbeInterval:  probeInterval,
			Logf:           func(f string, args ...any) { t.Logf("fake: "+f, args...) },
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" {
					io.Copy(io.Discard, r.Body)
					http.Error(w, "disk full", http.StatusInternalServerError)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer fake.Close()
		ps := a.srv.peers
		// Two intervals for a and fake to reach each other, for the same
		// reasons as in newPeerPair.
		sleepAndWait(2 * probeInterval)
		if !membersAre(ps, "a", "fake") {
			t.Fatalf("a routes over %v, want [a fake]", ps.router.Members())
		}
		small, big := ownedObjects(t, ps, "fake")

		if code := a.put(small, nil); code != http.StatusNoContent {
			t.Errorf("small PUT: status %d, want 204", code)
		}
		if code, _, body := a.get(small.actionID, localOnly); code != http.StatusOK || !bytes.Equal(body, small.body) {
			t.Errorf("small object not stored locally after owner rejected it: status %d", code)
		}
		if code := a.put(big, nil); code != http.StatusBadGateway {
			t.Errorf("big PUT: status %d, want 502", code)
		}
		if code, _, _ := a.get(big.actionID, localOnly); code != http.StatusNotFound {
			t.Errorf("big object stored locally after mid-stream failure: status %d", code)
		}
		if got, want := a.srv.m.PeerFwdPutErrs.Value(), int64(2); got != want {
			t.Errorf("PeerFwdPutErrs = %d, want %d", got, want)
		}
		if got, want := a.srv.m.PeerFwdPutFallbacks.Value(), int64(1); got != want {
			t.Errorf("PeerFwdPutFallbacks = %d, want %d", got, want)
		}
		if got, want := a.srv.m.PutErrs.Value(), int64(1); got != want {
			t.Errorf("PutErrs = %d, want %d", got, want)
		}
		// A rejected PUT is the peer's problem, not a reachability
		// failure, so it stays a member.
		if !membersAre(ps, "a", "fake") {
			t.Errorf("members = %v, want [a fake]", ps.router.Members())
		}
	})
}

func TestPeeringDisabled(t *testing.T) {
	st := newServerTester(t)
	res, err := http.Get(st.hs.URL + peerStatusPath)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("%s with peering off: status %d, want 404", peerStatusPath, res.StatusCode)
	}
	req := httptest.NewRequest("GET", "/peers", nil)
	rec := httptest.NewRecorder()
	st.srv.ServeHTTPDebug(rec, req)
	if !strings.Contains(rec.Body.String(), "not enabled") {
		t.Errorf("/peers with peering off:\n%s", rec.Body.String())
	}
}

func TestPeeringRejectsJWTAuth(t *testing.T) {
	_, err := NewServer(
		WithDir(t.TempDir()),
		WithLogf(t.Logf),
		WithJWTAuth("https://example.com"),
		WithPeers(PeerConfig{Name: "a", Peers: []string{"127.0.0.1:1"}}),
	)
	if err == nil || !strings.Contains(err.Error(), "JWT") {
		t.Fatalf("NewServer with JWT auth and peering: err = %v, want a JWT error", err)
	}
}

// TestPeerTailnetDiscovery runs two servers that learn of each other from
// fake tailscaleds and reach each other's adverts at their tailnet
// addresses on the shared advert port, then pool. The pieces are tested in
// depth in their own packages; this is the wiring.
func TestPeerTailnetDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const tag = "tag:test-colo"
		lan := new(lansporttest.LAN)
		nodeA := tagpeerstest.Node{ID: 1, Name: "a", IP: ipA, Tags: []string{tag}, Online: true}
		nodeB := tagpeerstest.Node{ID: 2, Name: "b", IP: ipB, Tags: []string{tag}, Online: true}
		tsA := tagpeerstest.New(t, nodeA, nodeB)
		tsB := tagpeerstest.New(t, nodeB, nodeA)

		a := newPeerTester(t, lan, ipA, PeerConfig{Name: "a", Tag: tag, TailscaledDial: tsA.Dial})
		b := newPeerTester(t, lan, ipB, PeerConfig{Name: "b", Tag: tag, TailscaledDial: tsB.Dial})
		// Two intervals for a and b to reach each other, for the same
		// reasons as in newPeerPair; learning of each other from the fake
		// tailscaleds happens at start, before the first probe.
		sleepAndWait(2 * probeInterval)
		if !membersAre(a.srv.peers, "stable-1", "stable-2") || !membersAre(b.srv.peers, "stable-1", "stable-2") {
			t.Fatalf("a routes over %v, b over %v", a.srv.peers.router.Members(), b.srv.peers.router.Members())
		}
		for _, o := range mkTestObjects(20) {
			if code := a.put(o, nil); code != http.StatusNoContent {
				t.Fatalf("PUT: status %d", code)
			}
			if code, _, body := b.get(o.actionID, nil); code != http.StatusOK || !bytes.Equal(body, o.body) {
				t.Fatalf("GET via b: status %d", code)
			}
		}

		// Losing the tag drops the peer: the tracker signals, lansport
		// re-probes, and the router follows.
		untagged := nodeB
		untagged.Tags = []string{"tag:other"}
		tsA.ChangeNode(untagged)
		synctest.Wait()
		if !membersAre(a.srv.peers, "stable-1") {
			t.Errorf("a still routes over %v after b lost the tag", a.srv.peers.router.Members())
		}
	})
}
