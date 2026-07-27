//go:build tools
// +build tools

// Package tools tracks build-time tool dependencies for `go install`.
// This file is excluded from regular builds via the "tools" build tag and
// is used solely to lock the versions of tools that the CI workflow
// installs at runtime (currently `gosec`). SonarQube rule S8545 requires
// version predictability for CI dependencies.
package tools

import (
	_ "github.com/securego/gosec/v2/cmd/gosec"
)
