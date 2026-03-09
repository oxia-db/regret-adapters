from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import uvicorn
from kubernetes import client as k8s_client, config as k8s_config

from okk_agent.agent import Agent, Event
from okk_agent.config import Config
from okk_agent.events.cron import CronScheduler
from okk_agent.events.github_poller import GitHubPoller
from okk_agent.events.k8s import K8sWatcher
from okk_agent.events.webhook import create_webhook_app
from okk_agent.tools.observe import ObserveTools
from okk_agent.tools.act import ActTools
from okk_agent.tools.report import ReportTools
from okk_agent.tools.state import StateTools

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("okk-agent")


class OkkAgent:
    """Main application that wires event sources to the AI agent."""

    def __init__(self, config: Config):
        self.config = config
        self._shutdown = threading.Event()

        # Init Kubernetes clients
        if config.in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()

        k8s_custom = k8s_client.CustomObjectsApi()
        k8s_core = k8s_client.CoreV1Api()

        # Init tool providers
        observe = ObserveTools(config, k8s_core)
        act = ActTools(config, k8s_custom)
        report = ReportTools(config) if config.github_token else None
        state = StateTools(config, k8s_core)

        # Init the AI agent (None if no credentials — runs in dry-run mode)
        if config.has_ai:
            self.agent = Agent(config, observe, act, report, state)
        else:
            self.agent = None
            logger.warning("No AI credentials — running in dry-run mode (events logged, no AI decisions)")

        # Event processing
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._event_lock = threading.Lock()

        # Init event sources
        self.k8s_watcher = K8sWatcher(k8s_custom, k8s_core, config.namespace, self._on_event)
        self.cron = CronScheduler(self._on_event, config.daily_report_hour)

        # GitHub comment poller (fallback when webhooks aren't reachable)
        self.github_poller = GitHubPoller(config, self._on_event) if config.github_token else None

        self.webhook_app = create_webhook_app(
            self._on_event,
            webhook_secret=os.environ.get("GITHUB_WEBHOOK_SECRET"),
        )

    def _on_event(self, event: Event):
        """Handle an event from any source — dispatch to the AI agent."""
        self._executor.submit(self._handle_event, event)

    def _handle_event(self, event: Event):
        """Process an event with the agent. Serialized to avoid concurrent Claude calls."""
        with self._event_lock:
            try:
                if self.agent:
                    result = self.agent.react(event)
                    if result:
                        logger.info("Agent response for %s: %s", event.type, result[:300])
                else:
                    logger.info("[dry-run] Event: %s — %s", event.type, event.summary)
                    if event.details:
                        logger.info("[dry-run] Details: %s", event.details)
            except Exception:
                logger.exception("Failed to handle event: %s", event.type)

    def run(self):
        """Start all components and block until shutdown."""
        logger.info("Starting okk-agent")

        # Start K8s watchers
        self.k8s_watcher.start()

        # Start cron scheduler
        self.cron.start()

        # Periodic health check (every 5 minutes) — silent unless problems found
        self.cron.add_interval_job("health_check", self.cron._trigger_health_check, minutes=5)

        # Periodic summary (every 4 hours) — always posts to daily issue
        self.cron.add_interval_job("periodic_summary", self.cron._trigger_periodic_summary, hours=4)

        # Start GitHub comment polling (every 2 minutes)
        if self.github_poller:
            self.cron.add_interval_job("github_poll", self.github_poller.poll, minutes=2)

        # Fire startup event — agent will check cluster state and ensure tests are running
        self._on_event(Event(
            type="startup",
            summary="okk-agent started. Check cluster state and ensure continuous verification is running.",
            details={
                "namespace": self.config.namespace,
                "oxia_image": self.config.oxia_image,
                "okk_worker_image": self.config.okk_worker_image,
            },
        ))

        # Start webhook server (blocks)
        uvicorn.run(
            self.webhook_app,
            host=self.config.webhook_host,
            port=self.config.webhook_port,
            log_level="info",
        )

    def shutdown(self):
        logger.info("Shutting down okk-agent")
        self._shutdown.set()
        self.k8s_watcher.stop()
        self.cron.stop()
        self._executor.shutdown(wait=False)


def main():
    config = Config.from_env()

    if not config.has_ai:
        logger.warning("No AI credentials — running in dry-run mode")
    else:
        logger.info("AI provider: %s", config.ai_provider)
    if not config.github_token:
        logger.warning("GITHUB_TOKEN not set — GitHub reporting disabled")

    app = OkkAgent(config)

    def handle_signal(signum, frame):
        app.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    app.run()


if __name__ == "__main__":
    main()
