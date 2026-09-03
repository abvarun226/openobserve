#!/usr/bin/env bash
set -euo pipefail

candidate_src=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
baseline_src=/data/o2-autoresearch/realtime-pipeline-baseline/openobserve
enterprise_src=$(cd -- "$(dirname -- "$candidate_src")/o2-enterprise" && pwd)
baseline_enterprise_src=/data/o2-autoresearch/realtime-pipeline-baseline/o2-enterprise
bench_root=/data/o2-bench-bulk
bin_dir="$candidate_src/.auto/bin"

[[ -e "$baseline_src/.git" && -e "$enterprise_src/.git" && -e "$baseline_enterprise_src/.git" && -x "$bench_root/scripts/run_remote_pipeline_benchmark.sh" ]] || {
    echo 'ERROR: required autoresearch sources or benchmark runner are unavailable' >&2
    exit 1
}
[[ -r "$candidate_src/Cargo.lock" && -r "$enterprise_src/Cargo.toml.openobserve" ]] || {
    echo 'ERROR: the candidate enterprise build overlay is unavailable' >&2
    exit 1
}

mkdir -p "$bin_dir"

build_baseline_binary() (
    set -euo pipefail
    local backup_dir
    backup_dir=$(mktemp -d /data/tmp/o2-enterprise-baseline-build.XXXXXX)
    cp -f "$baseline_src/Cargo.toml" "$backup_dir/Cargo.toml"
    cp -f "$baseline_src/Cargo.lock" "$backup_dir/Cargo.lock"
    restore() {
        cp -f "$backup_dir/Cargo.toml" "$baseline_src/Cargo.toml"
        cp -f "$backup_dir/Cargo.lock" "$baseline_src/Cargo.lock"
        rm -rf "$backup_dir"
    }
    trap restore EXIT

    cp -f "$baseline_enterprise_src/Cargo.toml.openobserve" "$baseline_src/Cargo.toml"
    (
        cd "$baseline_src"
        cargo build --release --features enterprise
    )
    cp -f "$baseline_src/target/release/openobserve" "$bin_dir/baseline-openobserve"
    git -C "$baseline_src" rev-parse HEAD >"$bin_dir/baseline-revision"
)

if [[ ! -x "$bin_dir/baseline-openobserve" ]]; then
    build_baseline_binary
fi
(
    cd "$candidate_src"
    cargo build --release --features enterprise
)
cp -f "$candidate_src/target/release/openobserve" "$bin_dir/candidate-openobserve"
git -C "$candidate_src" rev-parse HEAD >"$bin_dir/candidate-revision"

benchmark_output=$(
    "$bench_root/scripts/run_remote_pipeline_benchmark.sh" \
        --baseline-binary "$bin_dir/baseline-openobserve" \
        --candidate-binary "$bin_dir/candidate-openobserve" \
        --artifact-parent /data/tmp
)
printf '%s\n' "$benchmark_output"
artifact_root=$(awk -F': ' '/^Artifacts: / { print $2 }' <<<"$benchmark_output")
[[ -n "$artifact_root" && -r "$artifact_root/comparison.json" ]] || {
    echo 'ERROR: benchmark did not report comparison.json' >&2
    exit 1
}

python3 - "$artifact_root/comparison.json" <<'PY'
import json
import sys

comparison = json.load(open(sys.argv[1], encoding="utf-8"))["comparison"]
latency = comparison["latency"]
throughput = comparison["throughput"]
latency_candidate = latency["candidate_medians"]
throughput_candidate = throughput["candidate_medians"]
latency_delta = latency["baseline_vs_candidate"]
throughput_delta = throughput["baseline_vs_candidate"]

metrics = {
    "candidate_client_latency_ms": latency_candidate["client_latency_ms"],
    "candidate_remote_delivery_latency_ms": latency_candidate["remote_delivery_latency_ms"],
    "candidate_prometheus_response_time_ms": latency_candidate["prometheus_response_time_mean_ms"],
    "candidate_throughput_records_per_sec": throughput_candidate["records_per_second"],
    "candidate_throughput_mb_per_sec": throughput_candidate["mb_per_second"],
    "latency_vs_baseline_percent": latency_delta["client_latency_ms"]["percent_change"],
    "throughput_vs_baseline_percent": throughput_delta["records_per_second"]["percent_change"],
}
for name, value in metrics.items():
    if value is None:
        raise SystemExit(f"missing metric: {name}")
    print(f"METRIC {name}={value}")
PY
