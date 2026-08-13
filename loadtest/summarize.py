import json
import sys


def format_summary(path: str) -> str:
    with open(path) as f:
        data = json.load(f)

    rps = data.get("rps", 0)
    percentiles = {p["percentage"]: p["latency"] for p in data.get("latencyDistribution", [])}
    p50_ms = percentiles.get(50, 0) / 1_000_000
    p99_ms = percentiles.get(99, 0) / 1_000_000
    status_codes = data.get("statusCodeDistribution", {})

    lines = [
        f"file: {path}",
        f"requests: {data.get('count', 0)}",
        f"throughput: {rps:.1f} req/s",
        f"p50 latency: {p50_ms:.2f}ms",
        f"p99 latency: {p99_ms:.2f}ms",
        f"status codes: {status_codes}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    for path in sys.argv[1:]:
        print(format_summary(path))
        print()
