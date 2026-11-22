"""Unit tests for structured logging and credential scrubbing."""
from __future__ import annotations

import json
import logging

from skywatch.pipeline.logging import (
    JsonPipelineFormatter,
    generate_run_id,
    get_current_run_id,
    get_logger,
    redact_sensitive,
    run_context,
)


def test_generate_run_id():
    rid1 = generate_run_id()
    rid2 = generate_run_id()
    assert isinstance(rid1, str)
    assert len(rid1) == 32
    assert rid1 != rid2


def test_run_context_propagation():
    custom_id = "test-run-123456"
    assert get_current_run_id() != custom_id

    with run_context(custom_id) as active_id:
        assert active_id == custom_id
        assert get_current_run_id() == custom_id

    # Exiting context reverts run_id
    assert get_current_run_id() != custom_id


def test_redact_sensitive_strings():
    sensitive = "Calling OpenAQ with OPENAQ_API_KEY=abc123secret and Bearer eyJhbGciOi"
    redacted = redact_sensitive(sensitive)
    assert "abc123secret" not in redacted
    assert "OPENAQ_API_KEY=***REDACTED***" in redacted


def test_json_pipeline_formatter():
    formatter = JsonPipelineFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg="Processed records with token=secret123",
        args=(),
        exc_info=None,
    )
    record.component = "collector"
    record.run_id = "fixed-run-uuid"
    record.extra_data = {"record_count": 100, "api_key": "supersecret"}

    output = formatter.format(record)
    parsed = json.loads(output)

    assert parsed["run_id"] == "fixed-run-uuid"
    assert parsed["component"] == "collector"
    assert parsed["level"] == "INFO"
    assert "token=***REDACTED***" in parsed["message"]
    assert "secret123" not in parsed["message"]
    assert parsed["extra"]["record_count"] == 100
    assert parsed["extra"]["api_key"] == "***REDACTED***"


def test_get_logger_output(capsys):
    logger = get_logger("test_unit")
    with run_context("logger-run-id"):
        logger.info("Executing collector with api_key=topsecret")

    captured = capsys.readouterr()
    parsed = json.loads(captured.err or captured.out)

    assert parsed["run_id"] == "logger-run-id"
    assert parsed["component"] == "skywatch.test_unit"
    assert "topsecret" not in parsed["message"]
    assert "***REDACTED***" in parsed["message"]
