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
