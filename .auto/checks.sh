#!/usr/bin/env bash
set -euo pipefail

candidate_src=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
candidate_binary="$candidate_src/.auto/bin/candidate-openobserve"
bench_root=/data/o2-bench-bulk

[[ -x "$candidate_binary" ]] || {
    echo 'ERROR: run ./.auto/measure.sh before the black-box checks' >&2
    exit 1
}

git -C "$candidate_src" diff --check
"$bench_root/scripts/run_remote_pipeline_smoke.sh" \
    --enterprise-binary "$candidate_binary" --scenario all
