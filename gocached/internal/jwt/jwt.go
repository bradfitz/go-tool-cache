// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package jwt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sync/atomic"
	"time"

	"github.com/bradfitz/go-tool-cache/gocached/logger"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

const oidcConfigWellKnownPath string = "/.well-known/openid-configuration"

var (
	// Required + recommended algorithms from https://datatracker.ietf.org/doc/html/rfc7518#section-3.1
	supportedAlgorithms = []string{"HS256", "RS256", "ES256"}
)

// issuer holds the per-issuer state: its own JWT parser and signing keys.
type issuer struct {
	iss         string
	parser      *jwt.Parser
	signingKeys atomic.Value // []jose.JSONWebKey
}

// keyFunc is how github.com/golang-jwt/jwt gets the public key it needs to
// verify a JWT signature. Each issuer entry only checks its own keys.
func (ie *issuer) keyFunc(t *jwt.Token) (any, error) {
	var kid string
	if v, ok := t.Header["kid"]; ok {
		kid, _ = v.(string)
	}
	if kid == "" {
		return nil, fmt.Errorf("no kid found in token header")
	}

	signingKeys := ie.signingKeys.Load().([]jose.JSONWebKey)
	for _, k := range signingKeys {
		if k.KeyID == kid {
			return k.Key, nil
		}
	}

	return nil, fmt.Errorf("unknown key ID: %s", kid)
}

// NewJWTValidator constructs a [Validator] for validating JWTs. Must call
// [RunUpdateJWKSLoop] before validating any JWTs. Every JWT must exactly match
// one of the provided issuers and the audience value in its "iss" and "aud"
// claims respectively. Each issuer must be a reachable HTTP server that serves
// the JWT public signing keys via the path defined by [oidcConfigWellKnownPath],
// and the audience should be a value specific to the trust boundary that
// gocached resides within.
func NewJWTValidator(logf logger.Logf, audience string, issuerURLs []string) *Validator {
	var issuers []*issuer
	for _, iss := range issuerURLs {
		issuers = append(issuers, &issuer{
			iss: iss,
			parser: jwt.NewParser(
				jwt.WithValidMethods(supportedAlgorithms),
				jwt.WithIssuer(iss),
				jwt.WithAudience(audience),
				jwt.WithLeeway(10*time.Second),
				jwt.WithIssuedAt(),
			),
		})
	}
	return &Validator{
		logf:    logf,
		issuers: issuers,
	}
}

// RunUpdateJWKSLoop fetches the JWKS synchronously once to surface any config
// errors early, and then starts a background goroutine that periodically fetches
// the JWKS from all issuers to keep the signing keys up to date. Must be called
// before validating any JWTs.
func (v *Validator) RunUpdateJWKSLoop(ctx context.Context) error {
	// Initial fetch to error early on misconfiguration.
	if err := v.updateJWKS(ctx); err != nil {
		return fmt.Errorf("failed to initialize JWT validator: %w", err)
	}

	go v.runUpdateJWKSLoop(ctx)

	return nil
}

// Validator provides methods for validating JWTs. Use [NewJWTValidator] to
// construct a working Validator.
type Validator struct {
	logf    logger.Logf
	issuers []*issuer

	// TODO(tomhjp): metrics
}

// Validate returns an error if the provided JWT fails validation for an invalid
// signature or standard claim (iss, aud, iat, nbf, exp). It tries each
// configured issuer and returns the verified claims from the first successful
// parse. If all issuers fail, it returns the last error.
func (v *Validator) Validate(ctx context.Context, jwtString string) (map[string]any, error) {
	var lastErr error
	for _, ie := range v.issuers {
		tk, err := ie.parser.Parse(jwtString, ie.keyFunc)
		if err != nil {
			lastErr = err
			continue
		}

		if !tk.Valid {
			lastErr = fmt.Errorf("invalid token")
			continue
		}

		gotClaims, ok := tk.Claims.(jwt.MapClaims)
		if !ok {
			lastErr = fmt.Errorf("unexpected claims type: %T", tk.Claims)
			continue
		}

		return gotClaims, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to parse token: %w", lastErr)
	}
	return nil, fmt.Errorf("no issuers configured")
}

func (v *Validator) runUpdateJWKSLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(12 * time.Hour):
		}

		if err := v.updateJWKS(ctx); err != nil {
			// Non-fatal; in practice, the most recent keys will normally
			// still be valid for a long time, but JWT validation will start
			// erroring more loudly than this if not.
			v.logf("jwt: failed to update JWKS: %v", err)
		}
	}
}

func (v *Validator) updateJWKS(ctx context.Context) error {
	var errs []error
	for _, ie := range v.issuers {
		if err := ie.updateJWKS(ctx, v.logf); err != nil {
			errs = append(errs, fmt.Errorf("issuer %q: %w", ie.iss, err))
		}
	}
	return errors.Join(errs...)
}

func (ie *issuer) updateJWKS(ctx context.Context, logf logger.Logf) error {
	logf("jwt: fetching JWKS from issuer %q", ie.iss)
	u, err := url.Parse(ie.iss)
	if err != nil {
		return fmt.Errorf("failed to parse issuer URL %q: %w", ie.iss, err)
	}

	u.Path = path.Join(u.Path, oidcConfigWellKnownPath)

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Discover the JWKS endpoint.
	var config struct {
		JSONWebKeySetURI string `json:"jwks_uri"`
	}
	if err := get(ctx, u.String(), &config); err != nil {
		return fmt.Errorf("failed to fetch OIDC configuration from %q: %w", u, err)
	}
	if config.JSONWebKeySetURI == "" {
		return fmt.Errorf("jwks_uri not found in OIDC configuration")
	}

	// Fetch JWKS.
	var keySet jose.JSONWebKeySet
	if err := get(ctx, config.JSONWebKeySetURI, &keySet); err != nil {
		return fmt.Errorf("failed to fetch JWKS from %q: %w", config.JSONWebKeySetURI, err)
	}

	var signingKeys []jose.JSONWebKey
	for _, k := range keySet.Keys {
		if k.Use != "sig" {
			continue
		}
		if !slices.Contains(supportedAlgorithms, k.Algorithm) {
			continue
		}
		signingKeys = append(signingKeys, k)
	}

	ie.signingKeys.Store(signingKeys)
	return nil
}

func get(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("received non-200 response: %d, body: %q", resp.StatusCode, body)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	return nil
}
