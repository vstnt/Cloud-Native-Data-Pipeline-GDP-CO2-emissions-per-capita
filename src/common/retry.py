from __future__ import annotations

import random
import time
from typing import Iterable, Mapping, Optional, Sequence

import requests


def _compute_sleep_seconds(
    attempt: int,
    *,
    backoff_base: float,
    backoff_max: float,
) -> float:
    # Exponential backoff with jitter
    base = backoff_base * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0, backoff_base)
    return min(backoff_max, base + jitter)


def http_get_with_retries(
    url: str,
    *,
    params: Optional[Mapping[str, str | int]] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout: int = 30,
    max_attempts: int = 4,
    backoff_base: float = 0.5,
    backoff_max: float = 8.0,
    status_forcelist: Sequence[int] = (429, 500, 502, 503, 504),
) -> requests.Response:
    """
    Perform a GET with lightweight retries for transient failures.

    Retries on:
    - Connection/timeout errors (Requests exceptions)
    - HTTP status in `status_forcelist` (e.g., 429/5xx)

    Uses exponential backoff with jitter and honors `Retry-After` when present.
    Returns the last successful response (caller should still call raise_for_status()).
    Raises the last exception if all attempts fail.
    """
    attempt = 0
    last_exc: Exception | None = None
    while attempt < max_attempts:
        attempt += 1
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            # If status is in the forcelist, treat as transient and retry
            if resp.status_code in status_forcelist and attempt < max_attempts:
                # Respect Retry-After header when available
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        sleep_sec = float(retry_after)
                    except ValueError:
                        sleep_sec = _compute_sleep_seconds(
                            attempt, backoff_base=backoff_base, backoff_max=backoff_max
                        )
                else:
                    sleep_sec = _compute_sleep_seconds(
                        attempt, backoff_base=backoff_base, backoff_max=backoff_max
                    )
                time.sleep(sleep_sec)
                continue
            return resp
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as exc:  # transient network errors
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_sec = _compute_sleep_seconds(
                attempt, backoff_base=backoff_base, backoff_max=backoff_max
            )
            time.sleep(sleep_sec)
        except Exception as exc:  # Non-transient or unexpected
            # Don't retry on generic exceptions by default; raise immediately
            raise exc

    # Exhausted attempts on transient errors
    assert last_exc is not None
    raise last_exc


__all__ = ["http_get_with_retries"]

