from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: int
    reset_after_seconds: float


class RateLimiter(ABC):
    @abstractmethod
    async def allow(self, key: str) -> Decision:
        """Consume one unit of quota for `key`. Returns the resulting Decision."""
        raise NotImplementedError
