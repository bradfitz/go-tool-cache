// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestJWTClaimFlag(t *testing.T) {
	for name, tc := range map[string]struct {
		input []string
		want  jwtClaimValue
	}{
		"single_claim": {
			input: []string{"role=builder"},
			want:  jwtClaimValue{"role": "builder"},
		},
		"multiple_claims": {
			input: []string{"env=prod", "team=devops"},
			want:  jwtClaimValue{"env": "prod", "team": "devops"},
		},
		"duplicate_keys": {
			input: []string{"key=value1", "key=value2"},
			want:  jwtClaimValue{"key": "value2"},
		},
		"value_with_equals": {
			input: []string{"data=a=b=c"},
			want:  jwtClaimValue{"data": "a=b=c"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			claim := make(jwtClaimValue)
			for _, v := range tc.input {
				if err := claim.Set(v); err != nil {
					t.Fatalf("Set failed: %v", err)
				}
			}
			if diff := cmp.Diff(claim, tc.want); diff != "" {
				t.Errorf("jwtClaimValue mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestInvalidJWTClaimFlags(t *testing.T) {
	for _, s := range []string{
		"invalidclaim",
		"=nokey",
		"novalue=",
	} {
		claim := make(jwtClaimValue)
		if err := claim.Set(s); err == nil {
			t.Fatalf("Set with invalid claim %q did not return error", s)
		}
	}
}

func TestParsePutMovers(t *testing.T) {
	for _, tc := range []struct {
		in       string
		min, max int
		wantErr  bool
	}{
		{in: "8,256", min: 8, max: 256},
		{in: "32", min: 32, max: 32},
		{in: " 4 , 4 ", min: 4, max: 4},
		{in: "0", wantErr: true},
		{in: "0,8", wantErr: true},
		{in: "16,8", wantErr: true},
		{in: "8,", wantErr: true},
		{in: "", wantErr: true},
		{in: "x", wantErr: true},
		{in: "8,16,32", wantErr: true},
	} {
		lo, hi, err := parsePutMovers(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parsePutMovers(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && (lo != tc.min || hi != tc.max) {
			t.Errorf("parsePutMovers(%q) = %d,%d; want %d,%d", tc.in, lo, hi, tc.min, tc.max)
		}
	}
}
