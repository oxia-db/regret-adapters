from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass

from okk_pilot.config import Config
from okk_pilot.pipeline import PipelineConfig, default_pipeline
from okk_pilot.tools.observe import ObserveTools
from okk_pilot.tools.act import ActTools
from okk_pilot.tools.report import ReportTools
from okk_pilot.tools.state import StateTools
from okk_pilot.tools.invariants import InvariantChecker

logger = logging.getLogger(__name__)


@dataclass
class Event:
    type: str
    summary: str
    details: dict


class Pilot:
    """Code-driven verification pilot. AI is optional, used only for log analysis."""

    def __init__(
        self, config: Config,
        observe: ObserveTools, act: ActTools,
        report: ReportTools | None, state: StateTools,
        invariants: InvariantChecker,
        pipeline: PipelineConfig | None = None,
    ):
        self.config = config
        self.observe = observe
        self.act = act
        self.report = report
        self.state = state
        self.invariants = invariants
        self.pipeline = pipeline or default_pipeline()

        # Optional AI for log analysis
        self._ai = None
        if config.ai_enabled:
            try:
                from openai import OpenAI
                self._ai = OpenAI(
                    base_url=f"{config.ai_url}/v1",
                    api_key="ollama",
                    max_retries=0,
                    timeout=120.0,
                )
                logger.info("AI analysis enabled: %s at %s", config.ai_model, config.ai_url)
            except Exception as e:
                logger.warning("Failed to init AI client: %s", e)

        # Event dedup
        self._recent_events: dict[str, float] = {}
        self._event_cooldown = 300
        self._last_invariant_verdict: str | None = None

        logger.info("Pilot initialized with pipeline: %s",
                     [a.raw for a in self.pipeline.actions])

    def handle(self, event: Event) -> str:
        """Route event to handler."""
        logger.info("Handling event: %s — %s", event.type, event.summary)

        if self._is_duplicate(event):
            return ""

        handler = {
            "startup": self._handle_startup,
            "check_invariants": self._handle_check_invariants,
            "inject_chaos": self._handle_inject_chaos,
            "test_scaling": self._handle_test_scaling,
            "post_report": self._handle_post_report,
            "daily_report": self._handle_daily_report,
            "github_comment": self._handle_github_comment,
            "pod_restart": self._handle_pod_event,
            "crash_loop": self._handle_pod_event,
            "k8s_warning": self._handle_k8s_warning,
        }.get(event.type)

        if handler:
            try:
                return handler(event)
            except Exception:
                logger.exception("Failed to handle event: %s", event.type)
                return ""

        logger.info("Unhandled event type: %s", event.type)
        return ""

    def _is_duplicate(self, event: Event) -> bool:
        non_dedup = (
            "github_comment", "daily_report", "startup",
            "check_invariants", "post_report", "inject_chaos", "test_scaling",
        )
        if event.type in non_dedup:
            return False
        key = f"{event.type}:{event.summary[:80]}"
        now = time.time()
        last = self._recent_events.get(key)
        if last and (now - last) < self._event_cooldown:
            logger.info("Skipping duplicate: %s", key)
            return True
        self._recent_events[key] = now
        return False

    # ── Action Handlers ──────────────────────────────────────

    def _handle_startup(self, event: Event) -> str:
        snapshot = self._gather_snapshot()
        self._ensure_testcases_running()
        verdict = self._verdict_from_snapshot(snapshot)
        self._post_stats("startup", verdict, snapshot)
        return verdict

    def _handle_check_invariants(self, event: Event) -> str:
        snapshot = self._gather_snapshot()
        inv = self._parse_invariants(snapshot)

        if inv.get("passed", True):
            logger.info("Invariants: all hold.")
            self._last_invariant_verdict = None
            return "healthy"

        verdict = inv.get("summary", "Invariant violation detected.")
        analysis = self._analyze_logs_if_needed(verdict)
        if analysis:
            verdict += f"\n{analysis}"

        # Only post if the verdict changed to avoid flooding the issue with repeated comments
        if verdict != self._last_invariant_verdict:
            self._post_stats("check_invariants", verdict, snapshot)
            self._last_invariant_verdict = verdict
        else:
            logger.info("Invariant verdict unchanged, skipping comment: %s", verdict)

        return verdict

    def _handle_inject_chaos(self, event: Event) -> str:
        """Inject one chaos type, wait for it to finish, then return.
        The scheduler calls this in a loop to cycle through all types."""

        # Pre-check: cluster must be healthy
        snapshot = self._gather_snapshot()
        inv = self._parse_invariants(snapshot)
        if not inv.get("passed", True):
            logger.info("Skipping chaos: cluster unhealthy — %s", inv.get("summary", ""))
            return "skipped: cluster unhealthy"

        # Clean up any stuck chaos
        self._cleanup_chaos()

        # Pick a random chaos type
        chaos_types = self.pipeline.chaos_types
        if not chaos_types:
            return "skipped: no chaos types configured"

        chaos_type = random.choice(chaos_types)

        # Inject
        logger.info("Injecting chaos: %s", chaos_type)
        result = self.act.inject_chaos(
            type=chaos_type,
            target=self.config.chaos_target,
            duration=self.config.chaos_duration,
            namespace=self.config.namespace,
        )
        result_data = json.loads(result)
        if "error" in result_data:
            logger.warning("Chaos injection failed: %s", result_data["error"])
            return f"chaos failed: {result_data['error']}"

        # Wait for chaos to expire
        self._wait_for_chaos_cleanup(timeout=90)

        # Post-check
        post_snapshot = self._gather_snapshot()
        post_inv = self._parse_invariants(post_snapshot)
        if post_inv.get("passed", True):
            verdict = f"Injected {chaos_type} ({self.config.chaos_duration}). Cluster recovered."
        else:
            verdict = f"Injected {chaos_type} ({self.config.chaos_duration}). RECOVERY ISSUE: {post_inv.get('summary', '')}"
            analysis = self._analyze_logs_if_needed(verdict)
            if analysis:
                verdict += f"\n{analysis}"

        self._post_stats("inject_chaos", verdict, post_snapshot)
        return verdict

    def _handle_test_scaling(self, event: Event) -> str:
        """Scale test: can expand by any amount, but must reduce one at a time.
        Before each scale-down step, verify the oxia-status configmap shows
        the node has drained (no pending shard deletions)."""
        # Pre-check
        snapshot = self._gather_snapshot()
        inv = self._parse_invariants(snapshot)
        if not inv.get("passed", True):
            logger.info("Skipping scale: cluster unhealthy")
            return "skipped: cluster unhealthy"

        # Check for active chaos
        chaos_status = self.observe.get_chaos_status(namespace=self.config.namespace)
        if json.loads(chaos_status):
            logger.info("Skipping scale: active chaos experiments")
            return "skipped: active chaos"

        original = self.config.oxia_replicas
        scale_up_target = original + random.randint(1, 3)
        steps = []
        issues = []

        # Phase 1: Scale up (can jump by any amount)
        logger.info("Scaling up %d → %d", original, scale_up_target)
        self.act.scale_oxia(replicas=scale_up_target, namespace=self.config.namespace)
        time.sleep(30)

        up_snapshot = self._gather_snapshot()
        up_inv = self._parse_invariants(up_snapshot)
        steps.append(f"{original}→{scale_up_target}")
        if not up_inv.get("passed", True):
            issues.append(f"After scale-up to {scale_up_target}: {up_inv.get('summary', '')}")

        # Phase 2: Scale down one at a time back to original
        current = scale_up_target
        while current > original:
            removed_replica = current  # the node being removed (0-indexed)
            current -= 1
            logger.info("Scaling down %d → %d (removing oxia-%d)", current + 1, current, removed_replica)
            self.act.scale_oxia(replicas=current, namespace=self.config.namespace)

            # Wait for shard drain — check oxia-status configmap
            if not self._wait_for_shard_drain(removed_replica, timeout=120):
                issues.append(f"Shard drain timeout for oxia-{removed_replica} at {current} replicas")

            time.sleep(30)

            down_snapshot = self._gather_snapshot()
            down_inv = self._parse_invariants(down_snapshot)
            steps.append(f"→{current}")
            if not down_inv.get("passed", True):
                issues.append(f"After scale-down to {current}: {down_inv.get('summary', '')}")

        post_snapshot = self._gather_snapshot()
        path = "".join(steps)

        if not issues:
            verdict = f"Scaled {path}. All invariants held throughout."
        else:
            verdict = f"Scaled {path}. ISSUES: {'; '.join(issues)}"

        self._post_stats("test_scaling", verdict, post_snapshot)
        return verdict

    def _wait_for_shard_drain(self, removed_replica: int, timeout: int = 120) -> bool:
        """Wait until oxia-status configmap shows no pendingDeleteShardNodes.
        Logs progress so the user knows what's happening."""
        node_name = f"oxia-{removed_replica}"
        logger.info("Waiting for shard drain from %s...", node_name)
        start = time.time()
        last_pending = None

        while time.time() - start < timeout:
            try:
                status = self.observe.get_oxia_status()
                pending = self._get_pending_shard_ops(status)
                if not pending:
                    elapsed = time.time() - start
                    logger.info("Shard drain complete for %s (took %.0fs)", node_name, elapsed)
                    return True
                if pending != last_pending:
                    logger.info("Shard drain in progress — pending: %s", pending)
                    last_pending = pending
            except Exception as e:
                logger.debug("Failed to check shard status: %s", e)
            time.sleep(5)

        logger.warning("Shard drain timed out for %s after %ds", node_name, timeout)
        return False

    def _get_pending_shard_ops(self, status: dict) -> list[str]:
        """Get list of pending shard operations from oxia-status."""
        pending = []
        for ns_name, ns_data in status.get("namespaces", {}).items():
            for shard_id, shard_data in ns_data.get("shards", {}).items():
                for node in shard_data.get("pendingDeleteShardNodes", []):
                    pending.append(f"shard-{shard_id}@{node.get('internal', '?')}")
        return pending

    def _handle_post_report(self, event: Event) -> str:
        snapshot = self._gather_snapshot()
        verdict = self._verdict_from_snapshot(snapshot)
        header, tc_lines = self._format_stats_line("periodic_summary", snapshot)
        body = header + "\n" + "\n".join(tc_lines)
        if verdict != "All invariants hold.":
            body += f"\n{verdict}"
        else:
            body += "\nAll invariants hold."
        if self.report:
            try:
                daily = json.loads(self.report.get_or_create_daily_issue())
                if "number" in daily:
                    self.report.comment_on_issue(issue_number=daily["number"], body=body)
            except Exception as e:
                logger.warning("Failed to post periodic summary: %s", e)
        return verdict

    def _handle_daily_report(self, event: Event) -> str:
        snapshot = self._gather_snapshot()
        verdict = self._verdict_from_snapshot(snapshot)
        self._post_stats("daily_report", f"End of day. {verdict}", snapshot)

        if self.report:
            try:
                daily = json.loads(self.report.get_or_create_daily_issue())
                if "number" in daily:
                    self.report.close_issue(daily["number"])
            except Exception as e:
                logger.warning("Failed to close daily issue: %s", e)
        return verdict

    def _handle_github_comment(self, event: Event) -> str:
        body = event.details.get("comment_body", "").lower()
        issue_number = event.details.get("issue_number")

        # Parse command after @okk-pilot
        cmd = ""
        for mention in ("@okk-pilot", "@okk-agent"):
            if mention in body:
                cmd = body.split(mention, 1)[1].strip().split()[0] if body.split(mention, 1)[1].strip() else ""
                break

        if cmd == "status":
            snapshot = self._gather_snapshot()
            header, tc_lines = self._format_stats_line("status", snapshot)
            reply = header + "\n" + "\n".join(tc_lines)
        elif cmd == "chaos":
            reply = self._handle_inject_chaos(event)
        elif cmd == "stop":
            tc_data = self.observe.list_testcases()
            try:
                for tc in json.loads(tc_data).get("testcases", []):
                    self.act.delete_testcase(tc["name"])
            except (json.JSONDecodeError, TypeError):
                pass
            reply = "All testcases stopped."
        elif cmd == "start":
            self._ensure_testcases_running()
            reply = "Testcases started."
        elif cmd.startswith("scale"):
            parts = body.split(mention, 1)[1].strip().split()
            if len(parts) >= 2 and parts[1].isdigit():
                n = int(parts[1])
                result = self.act.scale_oxia(replicas=n, namespace=self.config.namespace)
                reply = f"Scale result: {result}"
            else:
                reply = "Usage: @okk-pilot scale N"
        else:
            reply = "Available commands: status, chaos, stop, start, scale N"

        if self.report and issue_number:
            self.report.comment_on_issue(issue_number=issue_number, body=reply)
        return reply

    def _handle_pod_event(self, event: Event) -> str:
        restart_count = event.details.get("restart_count", 0)
        reason = event.details.get("reason", "")

        if restart_count < 3 and reason not in ("OOMKilled",):
            return ""

        msg = f"Pod {event.details.get('pod', '?')} restarted (reason: {reason}, count: {restart_count})"
        analysis = self._analyze_logs_if_needed(msg, pod=event.details.get("pod"))
        if analysis:
            msg += f"\n{analysis}"

        snapshot = self._gather_snapshot()
        self._post_stats("pod_event", msg, snapshot)
        return msg

    def _handle_k8s_warning(self, event: Event) -> str:
        reason = event.details.get("reason", "")
        message = event.details.get("message", "")
        transient = (
            "FailedScheduling", "ImagePullBackOff", "Pulling", "Pulled",
            "FailedToUpdateEndpointSlices", "FailedToUpdateEndpoint",
            "FailedDelete", "FailedCreate",
            "FailedAttachVolume", "FailedMount",
        )
        if reason in transient:
            return ""

        # Also filter "Failed" reason when the message indicates a transient image pull issue
        if reason == "Failed" and any(kw in message for kw in ("ImagePullBackOff", "ErrImagePull", "image")):
            return ""

        if reason == "Unhealthy":
            count = event.details.get("count", 1)
            if count < 5:
                return ""

        msg = event.summary
        snapshot = self._gather_snapshot()
        self._post_stats("k8s_warning", msg, snapshot)
        return msg

    # ── Helpers ───────────────────────────────────────────────

    def _gather_snapshot(self) -> dict:
        snapshot = {}
        try:
            snapshot["testcases"] = self.observe.list_testcases()
        except Exception:
            pass
        try:
            snapshot["invariants"] = self.invariants.check_invariants(self.pipeline)
        except Exception:
            pass
        return snapshot

    def _parse_invariants(self, snapshot: dict) -> dict:
        raw = snapshot.get("invariants", "{}")
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return {"passed": True}

    def _verdict_from_snapshot(self, snapshot: dict) -> str:
        inv = self._parse_invariants(snapshot)
        return "All invariants hold." if inv.get("passed", True) else inv.get("summary", "Issues detected.")

    def _format_stats_line(self, event_type: str, snapshot: dict) -> tuple[str, list[str]]:
        total_ops = 0
        total_passed = 0
        total_failed = 0
        tc_lines = []

        tc_raw = snapshot.get("testcases", "")
        if tc_raw:
            try:
                tc_data = json.loads(tc_raw) if isinstance(tc_raw, str) else tc_raw
                testcases = tc_data.get("testcases", []) if isinstance(tc_data, dict) else []
                for tc in testcases:
                    ops = tc.get("operations", 0)
                    passed = tc.get("assertions_passed", 0)
                    failed = tc.get("assertions_failed", 0)
                    total_ops += ops
                    total_passed += passed
                    total_failed += failed
                    tc_lines.append(f"{tc['name']}: {ops:,} ops, {passed:,}✓ {failed}✗")
            except (json.JSONDecodeError, TypeError):
                pass

        p99 = "n/a"
        throughput = "n/a"
        inv_raw = snapshot.get("invariants", "")
        if inv_raw:
            try:
                inv_data = json.loads(inv_raw) if isinstance(inv_raw, str) else inv_raw
                for check in inv_data.get("checks", []):
                    if check["name"] == "performance.p99_latency" and check.get("value") is not None:
                        p99 = f"{check['value']}ms"
                    elif check["name"] == "performance.throughput" and check.get("value") is not None:
                        throughput = f"{check['value']} ops/s"
            except (json.JSONDecodeError, TypeError):
                pass

        header = (
            f"🤖 {event_type} | ops: {total_ops:,} | "
            f"assertions: {total_passed:,} passed, {total_failed} failed | "
            f"p99: {p99} | throughput: {throughput}"
        )
        return header, tc_lines

    def _post_stats(self, event_type: str, verdict: str, snapshot: dict):
        if not self.report:
            return
        try:
            body = self._format_report(event_type, verdict, snapshot)
            daily = json.loads(self.report.get_or_create_daily_issue())
            if "number" in daily:
                self.report.comment_on_issue(issue_number=daily["number"], body=body)
        except Exception as e:
            logger.warning("Failed to post stats: %s", e)

    def _format_report(self, event_type: str, verdict: str, snapshot: dict) -> str:
        """Format a detailed report comment."""
        lines = [f"### 🤖 {event_type}", ""]

        # Testcase table
        tc_raw = snapshot.get("testcases", "")
        if tc_raw:
            try:
                tc_data = json.loads(tc_raw) if isinstance(tc_raw, str) else tc_raw
                testcases = tc_data.get("testcases", []) if isinstance(tc_data, dict) else []
                if testcases:
                    lines.append("| Testcase | State | Operations | Passed | Failed |")
                    lines.append("|---|---|---|---|---|")
                    for tc in testcases:
                        name = tc.get("name", "?")
                        state = tc.get("state", "?")
                        ops = tc.get("operations", 0)
                        passed = tc.get("assertions_passed", 0)
                        failed = tc.get("assertions_failed", 0)
                        lines.append(f"| {name} | {state} | {ops:,} | {passed:,} | {failed} |")
                    lines.append("")
            except (json.JSONDecodeError, TypeError):
                pass

        # Invariant results
        inv_raw = snapshot.get("invariants", "")
        if inv_raw:
            try:
                inv_data = json.loads(inv_raw) if isinstance(inv_raw, str) else inv_raw
                checks = inv_data.get("checks", [])
                if checks:
                    lines.append("**Invariants:**")
                    for check in checks:
                        icon = "✅" if check.get("passed") else "❌"
                        lines.append(f"- {icon} {check.get('message', check.get('name', '?'))}")
                    lines.append("")
            except (json.JSONDecodeError, TypeError):
                pass

        # Verdict
        if verdict:
            lines.append(f"**Verdict:** {verdict}")

        return "\n".join(lines)

    def _ensure_testcases_running(self):
        try:
            tc_data = json.loads(self.observe.list_testcases())
            existing = {tc["name"] for tc in tc_data.get("testcases", [])}
        except (json.JSONDecodeError, TypeError):
            existing = set()

        defaults = [
            ("basic-kv-test", "basic"),
            ("streaming-seq-test", "streamingSequence"),
        ]
        for name, tc_type in defaults:
            if name not in existing:
                logger.info("Creating default testcase: %s", name)
                self.act.create_testcase(
                    name=name, type=tc_type,
                    op_rate=self.config.okk_op_rate,
                    key_space=self.config.okk_key_space,
                )

    def _get_chaos_index(self) -> int:
        raw = self.state.get_agent_state("chaos_round_index")
        try:
            data = json.loads(raw)
            val = data.get("value") if isinstance(data, dict) else data
            return int(val) if val is not None else 0
        except (json.JSONDecodeError, TypeError, ValueError):
            return 0

    def _set_chaos_index(self, index: int):
        self.state.set_agent_state("chaos_round_index", str(index))

    def _cleanup_chaos(self):
        try:
            status = json.loads(self.observe.get_chaos_status(namespace=self.config.namespace))
            for exp in status:
                name = exp.get("name", "")
                if name.startswith("pilot-") or name.startswith("agent-"):
                    logger.info("Cleaning up stuck chaos: %s", name)
                    self.act.delete_chaos(name=name, namespace=self.config.namespace)
        except (json.JSONDecodeError, TypeError):
            pass

    def _wait_for_chaos_cleanup(self, timeout: int = 90):
        start = time.time()
        while time.time() - start < timeout:
            try:
                status = json.loads(self.observe.get_chaos_status(namespace=self.config.namespace))
                active = [e for e in status if e.get("name", "").startswith(("pilot-", "agent-"))]
                if not active:
                    return
            except (json.JSONDecodeError, TypeError):
                return
            time.sleep(5)
        logger.warning("Chaos cleanup timed out after %ds", timeout)

    # ── AI Analysis (optional) ────────────────────────────────

    def _analyze_logs_if_needed(self, context: str, pod: str | None = None) -> str:
        if not self._ai:
            return ""

        try:
            logs = ""
            if pod:
                logs = self.observe.get_pod_logs(pod_name=pod, since_minutes=5)
            else:
                logs = self.observe.get_pod_logs(
                    pod_name="app.kubernetes.io/component=server",
                    since_minutes=5,
                )

            if not logs or len(logs) < 20:
                return ""

            if not any(kw in logs.lower() for kw in ["error", "panic", "fatal", "oom", "timeout"]):
                return ""

            response = self._ai.chat.completions.create(
                model=self.config.ai_model,
                messages=[{
                    "role": "user",
                    "content": (
                        "You are analyzing Oxia database server logs. "
                        "Context: " + context + "\n\n"
                        "Logs (last 5 min):\n" + logs[:3000] + "\n\n"
                        "In 1-2 sentences, what is the root cause? "
                        "If logs look normal, reply: NORMAL"
                    ),
                }],
                max_tokens=150,
                temperature=0,
            )
            answer = (response.choices[0].message.content or "").strip()
            if "NORMAL" in answer.upper():
                return ""
            return f"AI analysis: {answer}"
        except Exception as e:
            logger.debug("AI analysis failed: %s", e)
            return ""
