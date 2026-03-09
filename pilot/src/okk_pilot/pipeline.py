from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Valid action names — must match handler methods in Pilot
VALID_ACTIONS = {
    "check_invariants",
    "inject_chaos",
    "test_scaling",
    "post_report",
}

# Predefined invariant patterns
INVARIANT_PATTERNS = {
    r"zero assertion failures": ("assertion_failures", "==", 0),
    r"all pods healthy": ("pods_healthy", "==", True),
    r"restarts\s*<\s*(\d+)": ("restarts", "<", None),
    r"p99\s*<\s*(\d+)ms": ("p99_ms", "<", None),
    r"throughput\s*>\s*(\d+)": ("throughput", ">", None),
    r"shards balanced": ("shards_balanced", "==", True),
    r"leaders balanced": ("leaders_balanced", "==", True),
}


@dataclass
class ScheduledAction:
    action: str       # e.g. "check_invariants"
    schedule: str     # "every 5m", "daily", etc.
    interval_seconds: int | None = None
    cron_hour: int | None = None
    raw: str = ""


@dataclass
class InvariantRule:
    name: str         # e.g. "assertion_failures", "p99_ms"
    op: str           # "==", "<", ">"
    value: Any
    raw: str = ""


@dataclass
class PipelineConfig:
    actions: list[ScheduledAction] = field(default_factory=list)
    chaos_types: list[str] = field(default_factory=list)
    invariants: list[InvariantRule] = field(default_factory=list)

    def to_display(self) -> dict:
        """Return the natural, human-readable format."""
        plan = []
        for a in self.actions:
            plan.append(a.raw or f"{a.schedule}: {a.action}")

        inv = []
        for i in self.invariants:
            inv.append(i.raw or f"{i.name} {i.op} {i.value}")

        return {
            "plan": plan,
            "chaos": list(self.chaos_types),
            "invariants": inv,
        }

    def to_dict(self) -> dict:
        """Return structured representation."""
        return {
            "actions": [asdict(a) for a in self.actions],
            "chaos_types": self.chaos_types,
            "invariants": [asdict(i) for i in self.invariants],
        }

    def get_threshold(self, name: str) -> Any | None:
        """Get threshold value for an invariant by name."""
        for rule in self.invariants:
            if rule.name == name:
                return rule.value
        return None


def _parse_interval(text: str) -> int | None:
    """Parse '5m', '2h', '30s' into seconds."""
    m = re.match(r"(\d+)\s*(s|m|h)", text.strip())
    if not m:
        return None
    val, unit = int(m.group(1)), m.group(2)
    return val * {"s": 1, "m": 60, "h": 3600}[unit]


def _parse_plan_entry(line: str) -> ScheduledAction | None:
    """Parse 'every 5m: check_invariants', 'daily: post_report', or 'always: inject_chaos'."""
    line = line.strip()

    # Match "every <interval>: <action>"
    m = re.match(r"every\s+(\d+\s*[smh]):\s*(\w+)", line, re.IGNORECASE)
    if m:
        interval_str, action = m.group(1), m.group(2).strip()
        if action not in VALID_ACTIONS:
            logger.warning("Unknown action: %s (valid: %s)", action, ", ".join(sorted(VALID_ACTIONS)))
            return None
        return ScheduledAction(
            action=action,
            schedule=f"every {interval_str.strip()}",
            interval_seconds=_parse_interval(interval_str),
            raw=line,
        )

    # Match "always: <action>"
    m = re.match(r"always:\s*(\w+)", line, re.IGNORECASE)
    if m:
        action = m.group(1).strip()
        if action not in VALID_ACTIONS:
            logger.warning("Unknown action: %s (valid: %s)", action, ", ".join(sorted(VALID_ACTIONS)))
            return None
        return ScheduledAction(
            action=action,
            schedule="always",
            raw=line,
        )

    # Match "daily: <action>" or "daily at HH:MM: <action>"
    m = re.match(r"daily(?:\s+at\s+(\d{1,2}:\d{2}))?\s*:\s*(\w+)", line, re.IGNORECASE)
    if m:
        time_str, action = m.group(1), m.group(2).strip()
        hour = int(time_str.split(":")[0]) if time_str else 0
        if action not in VALID_ACTIONS:
            logger.warning("Unknown action: %s (valid: %s)", action, ", ".join(sorted(VALID_ACTIONS)))
            return None
        return ScheduledAction(
            action=action,
            schedule=f"daily at {hour:02d}:00",
            cron_hour=hour,
            raw=line,
        )

    logger.warning("Cannot parse plan entry: %s", line)
    return None


def _parse_invariant(line: str) -> InvariantRule | None:
    """Parse 'zero assertion failures' or 'p99 < 500ms'."""
    line = line.strip()

    for pattern, (name, op, default_val) in INVARIANT_PATTERNS.items():
        m = re.match(pattern, line, re.IGNORECASE)
        if m:
            value = default_val
            if value is None and m.lastindex and m.lastindex >= 1:
                raw_val = m.group(1)
                value = int(raw_val) if raw_val.isdigit() else float(raw_val)
            return InvariantRule(name=name, op=op, value=value, raw=line)

    logger.warning("Cannot parse invariant: %s", line)
    return None


def load_pipeline(path: str | Path) -> PipelineConfig:
    """Load pipeline from a YAML file."""
    path = Path(path)
    if not path.exists():
        logger.warning("Pipeline file not found: %s, using defaults", path)
        return default_pipeline()

    with open(path) as f:
        raw = yaml.safe_load(f)

    return parse_pipeline(raw)


def parse_pipeline(raw: dict) -> PipelineConfig:
    """Parse pipeline from a dict (from YAML or API)."""
    config = PipelineConfig()

    for line in raw.get("plan", []):
        action = _parse_plan_entry(line)
        if action:
            config.actions.append(action)

    config.chaos_types = raw.get("chaos", [])

    for line in raw.get("invariants", []):
        rule = _parse_invariant(line)
        if rule:
            config.invariants.append(rule)

    return config


def dump_pipeline(config: PipelineConfig) -> str:
    """Dump pipeline back to YAML in the natural format."""
    return yaml.dump(config.to_display(), default_flow_style=False, sort_keys=False)


def default_pipeline() -> PipelineConfig:
    return parse_pipeline({
        "plan": [
            "every 1h: check_invariants",
            "always: inject_chaos",
            "every 6h: test_scaling",
            "daily: post_report",
        ],
        "chaos": ["pod-kill", "network-delay", "pod-failure", "cpu-stress", "clock-skew"],
        "invariants": [
            "zero assertion failures",
            "all pods healthy",
            "restarts < 10",
            "p99 < 500ms",
            "throughput > 0",
            "shards balanced",
            "leaders balanced",
        ],
    })
