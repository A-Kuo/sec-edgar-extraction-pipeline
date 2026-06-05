import logging
import json
import os
import smtplib
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
import requests

logger = logging.getLogger(__name__)


@dataclass
class AlertPayload:
    run_id: str
    stage: str
    status: str
    error_message: Optional[str] = None
    row_count: Optional[int] = None

    def to_slack_format(self) -> dict:
        color = "danger" if self.status == "failed" else "good"
        return {
            "color": color,
            "title": f"Pipeline {self.status.upper()}: {self.stage}",
            "fields": [
                {"title": "Run ID", "value": self.run_id, "short": True},
                {"title": "Status", "value": self.status, "short": True},
                {"title": "Rows Processed", "value": str(self.row_count or 0), "short": True},
            ],
            "text": self.error_message or "No errors"
        }

    def to_plain_text(self) -> str:
        return f"""
Pipeline Alert
==============
Run ID: {self.run_id}
Stage: {self.stage}
Status: {self.status}
Rows: {self.row_count or 0}
Error: {self.error_message or 'None'}
"""


def send_alert(payload: AlertPayload) -> None:
    """Send alert to Slack and SMTP."""
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    smtp_host = os.getenv("SMTP_HOST")
    alert_email = os.getenv("ALERT_EMAIL_TO")

    if slack_url:
        try:
            response = requests.post(
                slack_url,
                json={
                    "attachments": [payload.to_slack_format()]
                },
                timeout=5
            )
            response.raise_for_status()
            logger.info(f"Alert sent to Slack for {payload.run_id}")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")

    if smtp_host and alert_email:
        try:
            msg = MIMEMultipart()
            msg["Subject"] = f"Pipeline {payload.status.upper()}: {payload.stage}"
            msg["From"] = "pipeline@sec-edgar.local"
            msg["To"] = alert_email

            msg.attach(MIMEText(payload.to_plain_text(), "plain"))

            with smtplib.SMTP(smtp_host, 25) as server:
                server.sendmail(msg["From"], alert_email, msg.as_string())
            logger.info(f"Alert sent to {alert_email} for {payload.run_id}")
        except Exception as e:
            logger.error(f"Failed to send SMTP alert: {e}")

    if not slack_url and not smtp_host:
        logger.warning(f"No alerting configured. Printing to stderr:\n{payload.to_plain_text()}")


def send_pipeline_failure_alert(run_id: str, stage: str, error: str) -> None:
    """Convenience wrapper for pipeline failures."""
    payload = AlertPayload(
        run_id=run_id,
        stage=stage,
        status="failed",
        error_message=error
    )
    send_alert(payload)
