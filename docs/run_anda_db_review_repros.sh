#!/usr/bin/env bash
# Runs the permanent fixed-behavior regression suite from the core review.
set -euo pipefail
review_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$review_repo"
cargo test -p anda_db --all-features --test review_regressions -- "$@"
