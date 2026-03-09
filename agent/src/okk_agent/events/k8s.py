from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from kubernetes import client as k8s_client, watch

from okk_agent.agent import Event

logger = logging.getLogger(__name__)

OKK_GROUP = "core.oxia.io"
OKK_VERSION = "v1"


class K8sWatcher:
    """Watches Kubernetes events relevant to okk and Oxia."""

    def __init__(
        self, k8s_custom: k8s_client.CustomObjectsApi,
        k8s_core: k8s_client.CoreV1Api,
        namespace: str, on_event: Callable[[Event], None],
    ):
        self.k8s_custom = k8s_custom
        self.k8s_core = k8s_core
        self.namespace = namespace
        self.on_event = on_event
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._start_time = time.time()

    def start(self):
        """Start watching for K8s events in background threads."""
        watchers = [
            ("pod-watcher", self._watch_pods),
            ("event-watcher", self._watch_events),
        ]
        for name, func in watchers:
            t = threading.Thread(target=func, name=name, daemon=True)
            t.start()
            self._threads.append(t)
            logger.info("Started %s", name)

    def stop(self):
        self._stop.set()

    def _watch_pods(self):
        """Watch for pod restarts and failures in the okk namespace."""
        w = watch.Watch()
        # Track restart counts seen at startup to avoid re-reporting old restarts
        seen_restarts: dict[str, int] = {}
        initial_list_done = False
        while not self._stop.is_set():
            try:
                for event in w.stream(
                    self.k8s_core.list_namespaced_pod,
                    namespace=self.namespace,
                    timeout_seconds=300,
                ):
                    if self._stop.is_set():
                        break

                    event_type = event["type"]
                    pod = event["object"]
                    pod_name = pod.metadata.name

                    # On initial list, record existing restart counts
                    if not initial_list_done and event_type in ("ADDED",):
                        if pod.status.container_statuses:
                            for cs in pod.status.container_statuses:
                                key = f"{pod_name}/{cs.name}"
                                seen_restarts[key] = cs.restart_count
                        continue

                    if event_type == "ADDED" and not initial_list_done:
                        continue

                    if event_type == "BOOKMARK":
                        initial_list_done = True
                        continue

                    # Detect pod restarts (only new restarts)
                    if event_type == "MODIFIED" and pod.status.container_statuses:
                        initial_list_done = True  # Any MODIFIED means we're past initial list
                        for cs in pod.status.container_statuses:
                            key = f"{pod_name}/{cs.name}"
                            prev_count = seen_restarts.get(key, 0)
                            seen_restarts[key] = cs.restart_count
                            if cs.restart_count > prev_count and cs.last_state and cs.last_state.terminated:
                                reason = cs.last_state.terminated.reason or "Unknown"
                                self.on_event(Event(
                                    type="pod_restart",
                                    summary=f"Pod {pod_name} container {cs.name} restarted (reason: {reason}, count: {cs.restart_count})",
                                    details={
                                        "pod": pod_name,
                                        "container": cs.name,
                                        "restart_count": cs.restart_count,
                                        "reason": reason,
                                        "exit_code": cs.last_state.terminated.exit_code,
                                    },
                                ))

                    # Detect crash loops
                    if event_type == "MODIFIED" and pod.status.container_statuses:
                        for cs in pod.status.container_statuses:
                            if cs.state and cs.state.waiting and cs.state.waiting.reason == "CrashLoopBackOff":
                                self.on_event(Event(
                                    type="crash_loop",
                                    summary=f"Pod {pod_name} is in CrashLoopBackOff",
                                    details={
                                        "pod": pod_name,
                                        "container": cs.name,
                                        "restart_count": cs.restart_count,
                                    },
                                ))
            except Exception:
                if not self._stop.is_set():
                    logger.exception("Pod watcher error, restarting")

    def _watch_events(self):
        """Watch K8s events for warnings related to okk/oxia."""
        w = watch.Watch()
        while not self._stop.is_set():
            try:
                for event in w.stream(
                    self.k8s_core.list_namespaced_event,
                    namespace=self.namespace,
                    timeout_seconds=300,
                ):
                    if self._stop.is_set():
                        break

                    k8s_event = event["object"]
                    # Skip stale events from before the agent started
                    if k8s_event.last_timestamp:
                        event_ts = k8s_event.last_timestamp.timestamp()
                        if event_ts < self._start_time:
                            continue
                    elif k8s_event.event_time:
                        event_ts = k8s_event.event_time.timestamp()
                        if event_ts < self._start_time:
                            continue

                    if k8s_event.type == "Warning":
                        self.on_event(Event(
                            type="k8s_warning",
                            summary=f"K8s warning on {k8s_event.involved_object.name}: {k8s_event.reason} — {k8s_event.message}",
                            details={
                                "object": k8s_event.involved_object.name,
                                "kind": k8s_event.involved_object.kind,
                                "reason": k8s_event.reason,
                                "message": k8s_event.message,
                                "count": k8s_event.count,
                            },
                        ))
            except Exception:
                if not self._stop.is_set():
                    logger.exception("Event watcher error, restarting")
