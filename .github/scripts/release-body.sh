#!/usr/bin/env bash
# Generates a professional release body markdown file.
# Usage: release-body.sh <version> <previous-tag> <dist-dir>
set -euo pipefail

VERSION="$1"
TAG_NUM="${VERSION#v}"
PREVIOUS_TAG="$2"
DIST_DIR="$3"

# ── Extract CHANGELOG section ──────────────────────────────────────
CHANGELOG_SECTION=$(
  awk -v ver="$TAG_NUM" '
    BEGIN { in_block = 0 }
    # Escape dots in version for regex match
    { line = $0 }
    /^## \[/ {
      if (in_block) { print ""; exit }
      split($0, a, "[][]")
      if (length(a) >= 2 && a[2] == ver) { in_block = 1; print; next }
    }
    in_block { print }
  ' CHANGELOG.md
)

# ── Contributors list ──────────────────────────────────────────────
CONTRIBUTORS=$(
  if [ -n "$PREVIOUS_TAG" ] && git rev-parse --verify "$PREVIOUS_TAG" &>/dev/null 2>&1; then
    git shortlog -sn "$PREVIOUS_TAG..HEAD" 2>/dev/null
  else
    git shortlog -sn HEAD 2>/dev/null
  fi | grep -viE "bot|dependabot|github-actions" | head -20 \
    | awk '{ $1=""; sub(/^ /, ""); print "- " $0 }' || echo "- No contributors listed."
)

# ── Checksums ──────────────────────────────────────────────────────
CHECKSUMS=""
if [ -d "$DIST_DIR" ] && [ "$(ls -A "$DIST_DIR"/*.tar.gz 2>/dev/null)" ]; then
  CHECKSUMS=$(cd "$DIST_DIR" && sha256sum *.tar.gz)
fi

# ── Build the markdown body ────────────────────────────────────────
{
  echo "# 🚀 Aqueduct ${VERSION}"
  echo
  echo "> **Ultra-high performance, zero-allocation QUIC message broker.**"
  echo "> [GitHub](https://github.com/kshishtovsky/aqueduct) ·"
  echo "> [Documentation](https://github.com/kshishtovsky/aqueduct/tree/main/docs/en) ·"
  echo "> [Benchmarks](https://github.com/kshishtovsky/aqueduct#benchmarking-aqueduct-bench)"
  echo
  echo "---"
  echo
  echo "## 📋 Changelog"
  echo
if [ -z "${CHANGELOG_SECTION}" ]; then
  echo "_No changelog entry for this version._"
else
  echo "${CHANGELOG_SECTION}"
fi
echo
echo "---"
echo
echo "## 🧑‍💻 Contributors"
  echo
  echo "This release includes contributions from:"
  echo
  echo "${CONTRIBUTORS}"
  echo
  echo "---"
  echo
  echo "## 📦 Downloads"
  echo
  echo "| Platform | Architecture | File |"
  echo "|---|---|---|"
  echo "| Linux | \`amd64\` | \`aqueduct-${VERSION}-linux-amd64.tar.gz\` |"
  echo "| Linux | \`arm64\` | \`aqueduct-${VERSION}-linux-arm64.tar.gz\` |"
  echo "| macOS (Intel) | \`amd64\` | \`aqueduct-${VERSION}-darwin-amd64.tar.gz\` |"
  echo "| macOS (Apple Silicon) | \`arm64\` | \`aqueduct-${VERSION}-darwin-arm64.tar.gz\` |"
  echo
  echo "## 🔐 Checksums (SHA256)"
  echo
  echo '```'
  if [ -n "$CHECKSUMS" ]; then
    echo "${CHECKSUMS}"
  else
    echo "  (generated during CI build)"
  fi
  echo '```'
  echo
  echo "---"
  echo
  echo "*Full changelog: [CHANGELOG.md](https://github.com/kshishtovsky/aqueduct/blob/main/CHANGELOG.md)*"
  echo "*Previous release: [${PREVIOUS_TAG}](https://github.com/kshishtovsky/aqueduct/releases/tag/${PREVIOUS_TAG})*"
} > release_body.md

echo "✓ Release body written: $(wc -c < release_body.md) bytes"
