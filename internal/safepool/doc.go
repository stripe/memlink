// Package safepool provides type-safe wrappers around `sync.Pool`.
// Until the go stdlib sync.Pool becomes generic, safepool wrappers
// should be used instead of raw sync.Pool access.
package safepool

//stripe_lint_level:stricter
