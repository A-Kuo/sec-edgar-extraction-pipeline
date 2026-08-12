"""
Alerting hooks for the SEC EDGAR extraction pipeline.

Integrates with the Airflow DAG's ``send_alerts_on_failure`` task.
Alerts are dispatched in priority order:
  1. Slack webhook  (if SLACK_WEBHOOK_URL is set)
  2. Email via SMTP (if SMTP_HOST is set)
  3. Logged to stderr as a fallback

All functions are intentionally side-effect-free no-ops unless the
relevant environment variables are configured, so the test suite
can import this module without external dependencies.
"""

from __future__ import annotations

import logging
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert payload
# ---------------------------------------------------------------------------


@dataclass
class AlertPayload:
    """Structured alert message dispatched by the pipeline."""

    run_id: str
    stage: str  # e.g. "validate", "parse"
    status: str  # "failed"
    error_message: str
    records_processed: int = 0
    extra: dict[str, Any] | None = None

    def as_text(self) -> str:
        lines = [
            "[SEC EDGAR Pipeline] Stage failed",
            f"  run_id : {self.run_id}",
            f"  stage  : {self.stage}",
            f"  status : {self.status}",
            f"  records: {self.records_processed}",
            f"  error  : {self.error_message}",
        ]
        if self.extra:
            for k, v in self.extra.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def as_slack_payload(self) -> dict:
        return {
            "text": f":x: *SEC EDGAR Pipeline failure* — `{self.stage}`",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Run:* `{self.run_id}`\n"
                            f"*Stage:* `{self.stage}`\n"
                            f"*Error:* {self.error_message}"
                        ),
                    },
                }
            ],
        }


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def send_alert(payload: AlertPayload) -> None:
    """
    Dispatch *payload* to all configured channels.

    Channels are tried in order; each failure is logged but does not
    prevent the next channel from being attempted.
    """
    dispatched = False

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            _send_slack(payload, slack_url)
            dispatched = True
        except Exception as exc:
            logger.error("Slack alert failed: %s", exc)

    smtp_host = os.getenv("SMTP_HOST")
    alert_email = os.getenv("ALERT_EMAIL_TO")
    if smtp_host and alert_email:
        try:
            _send_email(payload, smtp_host, alert_email)
            dispatched = True
        except Exception as exc:
            logger.error("Email alert failed: %s", exc)

    if not dispatched:
        logger.error("PIPELINE ALERT (no channel configured):\n%s", payload.as_text())


def send_pipeline_failure_alert(
    run_id: str,
    stage: str,
    error_message: str,
    records_processed: int = 0,
) -> None:
    """Convenience wrapper used by the Airflow DAG's failure callback."""
    send_alert(
        AlertPayload(
            run_id=run_id,
            stage=stage,
            status="failed",
            error_message=error_message,
            records_processed=records_processed,
        )
    )


# ---------------------------------------------------------------------------
# Channel implementations
# ---------------------------------------------------------------------------


def _send_slack(payload: AlertPayload, webhook_url: str) -> None:
    """POST to a Slack Incoming Webhook."""
    import json
    import urllib.request

    body = json.dumps(payload.as_slack_payload()).encode()
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status not in (200, 204):
            raise RuntimeError(f"Slack responded HTTP {resp.status}")
    logger.info("Slack alert sent for run=%s stage=%s", payload.run_id, payload.stage)


def _send_email(payload: AlertPayload, smtp_host: str, to_addr: str) -> None:
    """Send a plain-text alert email via SMTP."""
    from_addr = os.getenv("SMTP_FROM", "noreply@edgar-pipeline.local")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))

    msg = EmailMessage()
    msg["Subject"] = f"[EDGAR Pipeline] Failure in stage '{payload.stage}' (run {payload.run_id})"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(payload.as_text())

    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
        server.send_message(msg)
    logger.info("Email alert sent to %s for run=%s", to_addr, payload.run_id)
