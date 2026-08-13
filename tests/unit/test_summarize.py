import json

from loadtest.summarize import format_summary


def test_format_summary_extracts_key_metrics(tmp_path):
    payload = {
        "count": 20000,
        "total": 15000000000,  # nanoseconds
        "rps": 1333.3,
        "latencyDistribution": [
            {"percentage": 50, "latency": 20000000},
            {"percentage": 99, "latency": 45000000},
        ],
        "statusCodeDistribution": {"OK": 20000},
    }
    path = tmp_path / "result.json"
    path.write_text(json.dumps(payload))

    summary = format_summary(str(path))

    assert "1333" in summary
    assert "p99" in summary
    assert "45.00ms" in summary or "45ms" in summary
