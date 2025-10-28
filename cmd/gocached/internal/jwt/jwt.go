package jwt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sync/atomic"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

const oidcConfigWellKnownPath string = "/.well-known/openid-configuration"

var (
	// Required + recommended algorithms from https://datatracker.ietf.org/doc/html/rfc7518#section-3.1
	supportedAlgorithms = []string{"HS256", "RS256", "ES256"}
)

// NewJWTValidator constructs a [Validator] for validating JWTs. Must call
// [RunUpdateJWKSLoop] before validating any JWTs. Every JWT must exactly match
// the provided issuer and audience values in its "iss" and "aud" claims
// respectively. The issuer must be a reachable HTTP server that serves the JWT
// public signing keys via the path defined by [oidcConfigWellKnownPath], and
// the audience should be a value specific to the trust boundary that gocached
// resides within.
func NewJWTValidator(issuer, audience string) *Validator {
	return &Validator{
		Logf:   log.Printf,
		issuer: issuer,
		parser: jwt.NewParser(
			jwt.WithValidMethods(supportedAlgorithms),
			jwt.WithIssuer(issuer),
			jwt.WithAudience(audience),
			jwt.WithLeeway(10*time.Second),
			jwt.WithIssuedAt(),
		),
	}
}

// RunUpdateJWKSLoop fetches the JWKS synchronously once to surface any config
// errors early, and then starts a background goroutine that periodically fetches
// the JWKS from the issuer to keep the signing keys up to date. Must be called
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
	Logf   func(format string, args ...any)
	issuer string
	parser *jwt.Parser

	signingKeys atomic.Value // []jose.JSONWebKey

	// TODO(tomhjp): metrics
}

// Validate returns an error if the provided JWT fails validation for an invalid
// signature or standard claim (iss, aud, iat, nbf, exp). It returns the token's
// verified claims if validation succeeds. The caller should then make policy
// decisions based on other claims such as "sub" or other custom claims.
func (v *Validator) Validate(ctx context.Context, jwtString string) (map[string]any, error) {
	tk, err := v.parser.Parse(jwtString, v.keyFunc)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !tk.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	gotClaims, ok := tk.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type: %T", tk.Claims)
	}

	return gotClaims, nil
}

// keyFunc is how github.com/golang-jwt/jwt gets the public key it needs to
// verify a JWT signature.
func (v *Validator) keyFunc(t *jwt.Token) (any, error) {
	var kid string
	if v, ok := t.Header["kid"]; ok {
		kid, _ = v.(string)
	}
	if kid == "" {
		return nil, fmt.Errorf("no kid found in token header")
	}

	signingKeys := v.signingKeys.Load().([]jose.JSONWebKey)
	for _, k := range signingKeys {
		if k.KeyID == kid {
			return k.Key, nil
		}
	}

	return nil, fmt.Errorf("unknown key ID: %s", kid)
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
			v.Logf("jwt: failed to update JWKS: %v", err)
		}
	}
}

func (v *Validator) updateJWKS(ctx context.Context) error {
	v.Logf("jwt: fetching JWKS from issuer %q", v.issuer)
	u, err := url.Parse(v.issuer)
	if err != nil {
		return fmt.Errorf("failed to parse issuer URL %q: %w", v.issuer, err)
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

	v.signingKeys.Store(signingKeys)
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
