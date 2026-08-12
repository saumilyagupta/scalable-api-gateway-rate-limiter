import asyncio
import os
from dataclasses import dataclass

import yaml


@dataclass(frozen=True)
class Policy:
    client_key_prefix: str
    method: str
    algorithm: str  # "token_bucket" | "sliding_window_log"
    limit: int
    refill_rate_per_second: float | None = None
    window_seconds: float | None = None


class PolicyRegistry:
    """Loads rate-limit policies from a YAML file and resolves the policy that
    applies to a given (client key, gRPC method) pair. `start_watching()` polls
    the file's mtime and hot-reloads on change, so policies can be updated
    without restarting the gateway."""

    def __init__(self, path: str, poll_interval_seconds: float = 2.0) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError(
                f"poll_interval_seconds must be positive, got {poll_interval_seconds}"
            )
        self._path = path
        self._poll_interval_seconds = poll_interval_seconds
        self._policies: list[Policy] = []
        self._default: Policy | None = None
        self._mtime: float | None = None
        self._watching = False
        self._load()

    def _load(self) -> None:
        with open(self._path) as f:
            raw = yaml.safe_load(f)

        self._policies = [
            Policy(
                client_key_prefix=p["client_key_prefix"],
                method=p["method"],
                algorithm=p["algorithm"],
                limit=p["limit"],
                refill_rate_per_second=p.get("refill_rate_per_second"),
                window_seconds=p.get("window_seconds"),
            )
            for p in raw.get("policies", [])
        ]

        default_raw = raw["default"]
        self._default = Policy(
            client_key_prefix="",
            method="*",
            algorithm=default_raw["algorithm"],
            limit=default_raw["limit"],
            refill_rate_per_second=default_raw.get("refill_rate_per_second"),
            window_seconds=default_raw.get("window_seconds"),
        )
        self._mtime = os.path.getmtime(self._path)

    def resolve(self, client_key: str, method: str) -> Policy:
        for policy in self._policies:
            if client_key.startswith(policy.client_key_prefix) and policy.method == method:
                return policy
        return self._default

    async def start_watching(self) -> None:
        self._watching = True
        while self._watching:
            await asyncio.sleep(self._poll_interval_seconds)
            try:
                current_mtime = os.path.getmtime(self._path)
            except OSError:
                continue
            if current_mtime != self._mtime:
                self._load()

    def stop_watching(self) -> None:
        self._watching = False
