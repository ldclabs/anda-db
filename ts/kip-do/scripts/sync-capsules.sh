#!/usr/bin/env bash
#
# Regenerates `src/capsules.generated.ts` from `rs/anda_kip/capsules/*.kip`.
#
# Both engines apply the *same* capsule text on first connect, so a fresh
# database starts with an identical base schema. The sources are inlined into
# TypeScript rather than imported as raw files so the published package needs
# no bundler configuration from consumers. Re-run and commit whenever a
# capsule changes.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node "$here/codegen-capsules.mjs"
