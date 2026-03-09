from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass

from okk_agent.config import Config
from okk_agent.prompt import SYSTEM_PROMPT, TOOL_DEFINITIONS
from okk_agent.tools.observe import ObserveTools
from okk_agent.tools.act import ActTools
from okk_agent.tools.report import ReportTools
from okk_agent.tools.state import StateTools

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 20


@dataclass
class Event:
    type: str  # "assertion_failure", "pod_restart", "daily_report", "github_webhook", "startup"
    summary: str
    details: dict


def _convert_tools_to_openai(tools: list[dict]) -> list[dict]:
    """Convert Anthropic-style tool definitions to OpenAI function calling format."""
    openai_tools = []
    for tool in tools:
        openai_tools.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"],
            },
        })
    return openai_tools


class Agent:
    def __init__(
        self, config: Config,
        observe: ObserveTools, act: ActTools,
        report: ReportTools | None, state: StateTools,
    ):
        self.config = config
        self.observe = observe
        self.act = act
        self.report = report
        self.state = state
        self._provider = config.ai_provider

        if self._provider == "anthropic":
            import anthropic
            self._anthropic = anthropic.Anthropic(api_key=config.anthropic_api_key)
        else:
            from openai import OpenAI
            self._openai = OpenAI(
                base_url="https://models.inference.ai.azure.com",
                api_key=config.github_token,
                max_retries=0,  # We handle rate limiting ourselves
            )

        # Rate limiting: track API calls to stay within quota
        # Free tier: gpt-4o-mini 150/day 10/min, gpt-4o 50/day 10/min
        self._call_timestamps: deque[float] = deque()
        self._max_calls_per_day = 120  # Leave headroom from 150
        self._max_calls_per_min = 8

        # Dedup: skip repeated events of the same type within cooldown
        self._recent_events: dict[str, float] = {}
        self._event_cooldown = 300  # 5 minutes

        # Local triage model (Ollama) for filtering noise
        self._triage_enabled = config.triage_enabled
        if self._triage_enabled:
            from openai import OpenAI
            self._triage_client = OpenAI(
                base_url=f"{config.triage_url}/v1",
                api_key="ollama",  # Ollama doesn't need a real key
                max_retries=0,
                timeout=120.0,  # Local model can be slow on first call
            )
            self._triage_model = config.triage_model
            logger.info("Triage model enabled: %s at %s", config.triage_model, config.triage_url)

        logger.info("Agent initialized with provider: %s", self._provider)

    def _triage_event(self, event: Event) -> bool:
        """Use local model to decide if event needs the real AI. Returns True if important."""
        # Always escalate these
        if event.type in ("github_comment", "daily_report", "startup", "health_check", "periodic_summary"):
            return True

        if not self._triage_enabled:
            return True

        try:
            prompt = (
                "You are a K8s event filter for an Oxia database testing system. "
                "Decide if this event needs investigation by the AI agent.\n\n"
                "SKIP (return NO) for: ImagePullBackOff, transient pod restarts during rollout, "
                "expected warnings, readiness probe failures during startup.\n\n"
                "ESCALATE (return YES) for: assertion failures, data corruption, OOMKilled, "
                "persistent crashes (>3 restarts), network errors between Oxia nodes, "
                "test failures, unexpected pod terminations.\n\n"
                f"Event type: {event.type}\n"
                f"Summary: {event.summary}\n\n"
                "Reply with only YES or NO."
            )
            response = self._triage_client.chat.completions.create(
                model=self._triage_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5,
                temperature=0,
            )
            answer = (response.choices[0].message.content or "").strip().upper()
            should_escalate = "YES" in answer
            if not should_escalate:
                logger.info("Triage filtered out event: %s — %s", event.type, event.summary[:60])
            return should_escalate
        except Exception as e:
            logger.warning("Triage model failed, escalating by default: %s", e)
            return True  # If triage fails, escalate to be safe

    def _check_rate_limit(self) -> bool:
        """Return True if we can make an API call, False if rate-limited."""
        now = time.time()
        # Clean old timestamps
        while self._call_timestamps and self._call_timestamps[0] < now - 86400:
            self._call_timestamps.popleft()

        # Check daily limit
        if len(self._call_timestamps) >= self._max_calls_per_day:
            logger.warning("Daily API rate limit reached (%d calls). Skipping.", len(self._call_timestamps))
            return False

        # Check per-minute limit
        recent = sum(1 for t in self._call_timestamps if t > now - 60)
        if recent >= self._max_calls_per_min:
            logger.warning("Per-minute API rate limit reached (%d calls). Skipping.", recent)
            return False

        self._call_timestamps.append(now)
        return True

    def _is_duplicate_event(self, event: Event) -> bool:
        """Check if this event type was processed recently."""
        now = time.time()
        # These event types always go through (no dedup)
        if event.type in ("github_comment", "daily_report", "startup", "health_check", "periodic_summary"):
            return False

        key = f"{event.type}:{event.summary[:80]}"
        last_time = self._recent_events.get(key)
        if last_time and (now - last_time) < self._event_cooldown:
            logger.info("Skipping duplicate event (cooldown): %s", key)
            return True

        self._recent_events[key] = now
        return False

    def react(self, event: Event) -> str:
        """Feed an event to AI and execute its decisions."""
        logger.info("Reacting to event: %s — %s", event.type, event.summary)

        if self._is_duplicate_event(event):
            return ""

        # Local triage: let small model filter noise before using API quota
        if not self._triage_event(event):
            return ""

        if not self._check_rate_limit():
            return self._fallback_to_triage(event)

        try:
            if self._provider == "anthropic":
                return self._react_anthropic(event)
            return self._react_copilot(event)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RateLimit" in err_str:
                logger.warning("API rate limited, falling back to local model: %s", err_str[:100])
                return self._fallback_to_triage(event)
            raise

    def _fallback_to_triage(self, event: Event) -> str:
        """Use local Qwen model as fallback when the main model is rate-limited.

        No tool calling — we gather data ourselves and ask the local model
        to analyze it and decide if a comment is needed.
        """
        if not self._triage_enabled:
            logger.info("No triage model available for fallback, skipping event.")
            return ""

        logger.info("Using local model (fallback) for event: %s", event.type)
        try:
            # Gather context ourselves (no tool calling for local model)
            context_parts = [self._format_event(event)]

            if event.type in ("health_check", "startup", "periodic_summary"):
                tc_data = self.observe.list_testcases()
                context_parts.append(f"## TestCases\n{tc_data}")

                pod_data = self.observe.get_pod_status(label_selector="app.kubernetes.io/name=oxia-cluster")
                context_parts.append(f"## Oxia Cluster Pods\n{pod_data}")

                try:
                    logs = self.observe.get_pod_logs(pod_name="app.kubernetes.io/component=node", since_minutes=5)
                    # Only include if there are errors
                    if any(kw in logs.lower() for kw in ["error", "panic", "fatal", "warn"]):
                        context_parts.append(f"## Data Server Logs (errors found)\n{logs[:1000]}")
                except Exception:
                    pass

            context = "\n\n".join(context_parts)

            prompt = (
                "You are the okk verification agent monitoring Oxia. "
                "Based on the data below, write a 1-2 sentence summary. "
                "If everything is healthy, reply with just: HEALTHY\n"
                "If there are problems, describe them briefly.\n\n"
                f"{context}"
            )

            response = self._triage_client.chat.completions.create(
                model=self._triage_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0,
            )
            answer = (response.choices[0].message.content or "").strip()
            logger.info("Local model assessment: %s", answer[:200])

            # Only comment if there's a problem or it's a periodic summary
            if "HEALTHY" in answer.upper() and event.type != "periodic_summary":
                logger.info("Local model: cluster healthy, no comment needed.")
                return answer

            # Post comment on daily issue
            if self.report:
                daily = self.report.get_or_create_daily_issue()
                daily_data = json.loads(daily)
                if "number" in daily_data:
                    self.report.comment_on_issue(
                        issue_number=daily_data["number"],
                        body=answer,
                    )

            return answer
        except Exception as e:
            logger.warning("Local model fallback failed: %s", e)
            return ""

    def _react_anthropic(self, event: Event) -> str:
        messages = [{"role": "user", "content": self._format_event(event)}]

        for _ in range(MAX_TOOL_ROUNDS):
            response = self._anthropic.messages.create(
                model=self.config.anthropic_model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                return self._extract_text_anthropic(response)

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = self._execute_tool(block.name, block.input)
                    logger.info("Tool %s → %s", block.name, result[:200])
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "user", "content": tool_results})

        logger.warning("Agent hit max tool rounds")
        return self._extract_text_anthropic(response)

    def _react_copilot(self, event: Event) -> str:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": self._format_event(event)},
        ]
        tools = _convert_tools_to_openai(TOOL_DEFINITIONS)

        for _ in range(MAX_TOOL_ROUNDS):
            response = self._openai.chat.completions.create(
                model=self.config.copilot_model,
                messages=messages,
                tools=tools,
                max_tokens=4096,
            )
            choice = response.choices[0]
            messages.append(choice.message)

            if choice.finish_reason != "tool_calls":
                return choice.message.content or ""

            # Execute tool calls
            for tool_call in choice.message.tool_calls:
                name = tool_call.function.name
                args = json.loads(tool_call.function.arguments)
                result = self._execute_tool(name, args)
                logger.info("Tool %s → %s", name, result[:200])
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result,
                })

        logger.warning("Agent hit max tool rounds")
        return choice.message.content or ""

    def _format_event(self, event: Event) -> str:
        parts = [
            f"## Event: {event.type}",
            f"**Summary**: {event.summary}",
        ]
        if event.details:
            parts.append(f"**Details**:\n```json\n{json.dumps(event.details, indent=2)}\n```")
        parts.append("\nPlease investigate and take appropriate action based on your responsibilities.")
        return "\n\n".join(parts)

    def _execute_tool(self, name: str, input: dict) -> str:
        try:
            # Observe tools
            if name == "query_metrics":
                return self.observe.query_metrics(**input)
            elif name == "get_testcase_status":
                return self.observe.get_testcase_status(**input)
            elif name == "list_testcases":
                return self.observe.list_testcases(**input)
            elif name == "get_pod_logs":
                return self.observe.get_pod_logs(**input)
            elif name == "get_pod_status":
                return self.observe.get_pod_status(**input)
            elif name == "get_chaos_status":
                return self.observe.get_chaos_status(**input)
            # Act tools
            elif name == "create_testcase":
                return self.act.create_testcase(**input)
            elif name == "delete_testcase":
                return self.act.delete_testcase(**input)
            elif name == "inject_chaos":
                return self.act.inject_chaos(**input)
            # Report tools
            elif name == "get_or_create_daily_issue":
                if self.report:
                    return self.report.get_or_create_daily_issue()
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            elif name == "comment_on_issue":
                if self.report:
                    return self.report.comment_on_issue(**input)
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            elif name == "search_github_issues":
                if self.report:
                    return self.report.search_github_issues(**input)
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            elif name == "create_github_issue":
                if self.report:
                    return self.report.create_github_issue(**input)
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            elif name == "create_pull_request":
                if self.report:
                    return self.report.create_pull_request(**input)
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            elif name == "read_okk_source":
                if self.report:
                    return self.report.read_okk_source(**input)
                return json.dumps({"status": "skipped", "reason": "GitHub reporting disabled"})
            # State tools
            elif name == "get_agent_state":
                return self.state.get_agent_state(**input)
            elif name == "set_agent_state":
                return self.state.set_agent_state(**input)
            else:
                return json.dumps({"error": f"Unknown tool: {name}"})
        except Exception as e:
            logger.exception("Tool %s failed", name)
            return json.dumps({"error": f"Tool {name} failed: {str(e)}"})

    def _extract_text_anthropic(self, response) -> str:
        texts = [b.text for b in response.content if hasattr(b, "text")]
        return "\n".join(texts) if texts else ""
