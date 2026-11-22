"""Structured pipeline logging with run_id tracking and credential redaction.

Implements OPS-07: Structured run logging across all pipeline stages with unique
run identifiers (run_id) and sensitive credential scrubbing.
"""
from __future__ import annotations

import contextvars
import datetime
import json
import logging
import re
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

# Context variable tracking active run_id across execution stack
_CURRENT_RUN_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "current_run_id", default=""
)

# Regex patterns matching sensitive keys or credentials
_SENSITIVE_PATTERNS = [
    re.compile(r'(?i)(api[_-]?key|token|secret|password|auth)\s*[:=]\s*["\']?([^"\'\s&,]+)["\']?'),
    re.compile(r'(?i)bearer\s+([a-zA-Z0-9_\-\.]+)'),
]


def generate_run_id() -> str:
    """Generate a unique UUID4 hex string for a pipeline execution run."""
    return uuid.uuid4().hex


def get_current_run_id() -> str:
    """Retrieve the current active run_id from context, generating one if not set."""
    val = _CURRENT_RUN_ID.get()
    if not val:
        val = generate_run_id()
        _CURRENT_RUN_ID.set(val)
    return val


@contextmanager
def run_context(run_id: Optional[str] = None) -> Iterator[str]:
    """Context manager scoping execution blocks to a specific run_id."""
    target_id = run_id or generate_run_id()
    token = _CURRENT_RUN_ID.set(target_id)
    try:
        yield target_id
    finally:
        _CURRENT_RUN_ID.reset(token)


def redact_sensitive(text: str) -> str:
    """Scrub sensitive credentials such as API keys, tokens, and passwords from strings."""
    if not isinstance(text, str):
        return text

    redacted = text
    for pattern in _SENSITIVE_PATTERNS:
        # Preserve the key label and replace the secret value with ***REDACTED***
        redacted = pattern.sub(r"\1=***REDACTED***", redacted)
    return redacted


def _redact_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively redact dictionary keys and string values."""
    sanitized: Dict[str, Any] = {}
    for k, v in data.items():
        if any(term in k.lower() for term in ("key", "token", "secret", "password", "auth")):
            sanitized[k] = "***REDACTED***"
        elif isinstance(v, dict):
            sanitized[k] = _redact_dict(v)
        elif isinstance(v, str):
            sanitized[k] = redact_sensitive(v)
        else:
            sanitized[k] = v
    return sanitized


class JsonPipelineFormatter(logging.Formatter):
    """JSON log formatter injecting UTC timestamps, run_id, component, and sanitized extra fields."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        run_id = getattr(record, "run_id", None) or get_current_run_id()
        component = getattr(record, "component", record.name)

        raw_message = record.getMessage()
        sanitized_message = redact_sensitive(raw_message)

        log_payload: Dict[str, Any] = {
            "timestamp_utc": timestamp_utc,
            "run_id": run_id,
            "level": record.levelname,
            "component": component,
            "message": sanitized_message,
        }

        # Include contextual metrics if provided in record.extra
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            log_payload["extra"] = _redact_dict(record.extra_data)

        return json.dumps(log_payload)


def get_logger(component: str) -> logging.Logger:
    """Create or retrieve a configured PipelineLogger for a component."""
    logger = logging.getLogger(f"skywatch.{component}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(JsonPipelineFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return logger
