from __future__ import annotations

import logging
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler

from okk_agent.agent import Event

logger = logging.getLogger(__name__)


class CronScheduler:
    """Schedules periodic events like daily reports."""

    def __init__(self, on_event: Callable[[Event], None], daily_report_hour: int = 0):
        self.on_event = on_event
        self.daily_report_hour = daily_report_hour
        self._scheduler = BackgroundScheduler()

    def start(self):
        # Daily report
        self._scheduler.add_job(
            self._trigger_daily_report,
            trigger="cron",
            hour=self.daily_report_hour,
            minute=0,
            id="daily_report",
        )
        self._scheduler.start()
        logger.info("Cron scheduler started (daily report at %02d:00 UTC)", self.daily_report_hour)

    def stop(self):
        self._scheduler.shutdown(wait=False)

    def _trigger_daily_report(self):
        self.on_event(Event(
            type="daily_report",
            summary="Daily verification report is due",
            details={},
        ))

    def _trigger_health_check(self):
        self.on_event(Event(
            type="health_check",
            summary="Periodic health check: verify testcase status and check Oxia logs for anomalies.",
            details={},
        ))

    def _trigger_periodic_summary(self):
        self.on_event(Event(
            type="periodic_summary",
            summary="Time for a periodic summary. Check metrics, testcase status, and post a brief update on the daily issue.",
            details={},
        ))

    def add_interval_job(self, job_id: str, func, **kwargs):
        """Add an interval-based job (e.g., polling every N minutes)."""
        self._scheduler.add_job(func, trigger="interval", id=job_id, **kwargs)
        logger.info("Added interval job: %s (%s)", job_id, kwargs)

    def trigger_daily_report_now(self):
        """Manually trigger a daily report (for testing)."""
        self._trigger_daily_report()
