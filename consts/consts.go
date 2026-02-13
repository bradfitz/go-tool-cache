// Package consts defines shared constants used by both the gocached server
// and the cachers HTTP client.
package consts

// CapHeader is the HTTP response header used by the server to advertise
// its capabilities as a comma-separated list of tokens.
const CapHeader = "Gocached-Cap"

// CapPutLZ4 is the Gocached-Cap token indicating that the server accepts
// lz4-compressed PUT request bodies (Content-Encoding: lz4).
const CapPutLZ4 = "putlz4"
