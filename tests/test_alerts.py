"""
Tests for src/alerts.py.

The module docstring states every function is a side-effect-free no-op unless
the relevant environment variable is set — which makes it fully testable by
mocking ``urllib.request.urlopen`` and ``smtplib.SMTP`` rather than needing a
live Slack webhook or mail server. This file had 0% coverage before: not
because it needs live services, but because nothing exercised it yet.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.alerts import AlertPayload, send_alert, send_pipeline_failure_alert


@pytest.fixture
def payload():
    return AlertPayload(
        run_id="run-1",
        stage="validate",
        status="failed",
        error_message="completeness below threshold",
        records_processed=42,
    )


# ---------------------------------------------------------------------------
# AlertPayload formatting
# ---------------------------------------------------------------------------


class TestAlertPayload:
    def test_as_text_includes_all_fields(self, payload):
        text = payload.as_text()
        assert "run-1" in text
        assert "validate" in text
        assert "42" in text
        assert "completeness below threshold" in text

    def test_as_text_includes_extra_fields(self, payload):
        payload.extra = {"filings_checked": 10}
        assert "filings_checked: 10" in payload.as_text()

    def test_as_slack_payload_is_json_serialisable(self, payload):
        json.dumps(payload.as_slack_payload())

    def test_as_slack_payload_mentions_the_stage(self, payload):
        blocks = payload.as_slack_payload()["blocks"]
        assert "validate" in blocks[0]["text"]["text"]


# ---------------------------------------------------------------------------
# send_alert dispatch order and fallback
# ---------------------------------------------------------------------------


class TestSendAlertDispatch:
    def test_no_channels_configured_logs_and_does_not_raise(self, payload, monkeypatch, caplog):
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("SMTP_HOST", raising=False)
        monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)

        with caplog.at_level("ERROR"):
            send_alert(payload)

        assert any("no channel configured" in r.message for r in caplog.records)

    def test_slack_dispatched_when_webhook_configured(self, payload, monkeypatch):
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/abc")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        response = MagicMock()
        response.status = 200
        response.__enter__ = MagicMock(return_value=response)
        response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            send_alert(payload)

        urlopen.assert_called_once()

    def test_slack_payload_posts_to_the_configured_url(self, payload, monkeypatch):
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/abc")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        response = MagicMock()
        response.status = 200
        response.__enter__ = MagicMock(return_value=response)
        response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            send_alert(payload)

        sent_request = urlopen.call_args[0][0]
        assert sent_request.full_url == "https://hooks.slack.example/abc"

    def test_slack_failure_falls_through_to_fallback_log(self, payload, monkeypatch, caplog):
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/abc")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        with (
            patch("urllib.request.urlopen", side_effect=OSError("connection refused")),
            caplog.at_level("ERROR"),
        ):
            send_alert(payload)

        assert any("Slack alert failed" in r.message for r in caplog.records)
        assert any("no channel configured" in r.message for r in caplog.records)

    def test_email_dispatched_when_smtp_configured(self, payload, monkeypatch):
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.setenv("ALERT_EMAIL_TO", "oncall@example.com")

        smtp_instance = MagicMock()
        smtp_instance.__enter__ = MagicMock(return_value=smtp_instance)
        smtp_instance.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=smtp_instance) as smtp_cls:
            send_alert(payload)

        smtp_cls.assert_called_once()
        smtp_instance.send_message.assert_called_once()

    def test_email_requires_both_host_and_recipient(self, payload, monkeypatch, caplog):
        """SMTP_HOST alone, with no ALERT_EMAIL_TO, must not attempt to send."""
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)

        with patch("smtplib.SMTP") as smtp_cls, caplog.at_level("ERROR"):
            send_alert(payload)

        smtp_cls.assert_not_called()

    def test_email_failure_falls_through_to_fallback_log(self, payload, monkeypatch, caplog):
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.setenv("ALERT_EMAIL_TO", "oncall@example.com")

        with (
            patch("smtplib.SMTP", side_effect=OSError("connection refused")),
            caplog.at_level("ERROR"),
        ):
            send_alert(payload)

        assert any("Email alert failed" in r.message for r in caplog.records)

    def test_both_channels_attempted_independently(self, payload, monkeypatch):
        """A Slack failure must not prevent the email channel from being tried."""
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/abc")
        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.setenv("ALERT_EMAIL_TO", "oncall@example.com")

        smtp_instance = MagicMock()
        smtp_instance.__enter__ = MagicMock(return_value=smtp_instance)
        smtp_instance.__exit__ = MagicMock(return_value=False)

        with (
            patch("urllib.request.urlopen", side_effect=OSError("slack down")),
            patch("smtplib.SMTP", return_value=smtp_instance),
        ):
            send_alert(payload)

        smtp_instance.send_message.assert_called_once()

    def test_non_2xx_slack_response_counts_as_failure(self, payload, monkeypatch, caplog):
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/abc")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        response = MagicMock()
        response.status = 500
        response.__enter__ = MagicMock(return_value=response)
        response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=response), caplog.at_level("ERROR"):
            send_alert(payload)

        assert any("Slack alert failed" in r.message for r in caplog.records)


class TestSendPipelineFailureAlert:
    def test_wraps_arguments_into_a_payload(self, monkeypatch):
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("SMTP_HOST", raising=False)

        captured = {}

        def _capture(p):
            captured["payload"] = p

        with patch("src.alerts.send_alert", side_effect=_capture):
            send_pipeline_failure_alert(
                run_id="run-9", stage="load", error_message="db unavailable", records_processed=7
            )

        assert captured["payload"].run_id == "run-9"
        assert captured["payload"].stage == "load"
        assert captured["payload"].status == "failed"
        assert captured["payload"].records_processed == 7
