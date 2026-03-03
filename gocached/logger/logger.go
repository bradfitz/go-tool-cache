// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package logger

// Logf is a logging function type. It is implemented by log.Printf.
type Logf func(format string, args ...any)
