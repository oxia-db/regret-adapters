from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    # AI provider: "anthropic" or "copilot"
    ai_provider: str = "copilot"

    # Anthropic (when ai_provider == "anthropic")
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-20250514"

    # GitHub Copilot / GitHub Models (when ai_provider == "copilot")
    copilot_model: str = "gpt-4o-mini"

    # GitHub
    github_token: str = ""
    github_repo: str = "oxia-db/okk"

    # Kubernetes
    namespace: str = "okk-system"
    in_cluster: bool = True

    # Prometheus
    prometheus_url: str = "http://prometheus.monitoring.svc.cluster.local:9090"

    # Webhook server
    webhook_host: str = "0.0.0.0"
    webhook_port: int = 8080

    # Daily report schedule (hour in UTC)
    daily_report_hour: int = 0

    # Oxia cluster defaults
    oxia_image: str = "oxia/oxia:latest"
    oxia_shards: int = 4
    oxia_replicas: int = 3

    # okk defaults
    okk_worker_image: str = "oxia/okk-jvm-worker:local"
    okk_op_rate: int = 100
    okk_key_space: int = 10000

    # Local triage model (Ollama)
    triage_enabled: bool = True
    triage_url: str = "http://host.docker.internal:11434"  # Ollama from inside K8s
    triage_model: str = "qwen2.5:7b"

    # Agent state
    state_configmap: str = "okk-agent-state"

    @property
    def has_ai(self) -> bool:
        if self.ai_provider == "anthropic":
            return bool(self.anthropic_api_key)
        return bool(self.github_token)  # copilot uses github token

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            ai_provider=os.environ.get("AI_PROVIDER", cls.ai_provider),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            anthropic_model=os.environ.get("ANTHROPIC_MODEL", cls.anthropic_model),
            copilot_model=os.environ.get("COPILOT_MODEL", cls.copilot_model),
            github_token=os.environ.get("GITHUB_TOKEN", ""),
            github_repo=os.environ.get("GITHUB_REPO", cls.github_repo),
            namespace=os.environ.get("OKK_NAMESPACE", cls.namespace),
            in_cluster=os.environ.get("IN_CLUSTER", "true").lower() == "true",
            prometheus_url=os.environ.get("PROMETHEUS_URL", cls.prometheus_url),
            webhook_host=os.environ.get("WEBHOOK_HOST", cls.webhook_host),
            webhook_port=int(os.environ.get("WEBHOOK_PORT", cls.webhook_port)),
            daily_report_hour=int(os.environ.get("DAILY_REPORT_HOUR", cls.daily_report_hour)),
            oxia_image=os.environ.get("OXIA_IMAGE", cls.oxia_image),
            okk_worker_image=os.environ.get("OKK_WORKER_IMAGE", cls.okk_worker_image),
            triage_enabled=os.environ.get("TRIAGE_ENABLED", "true").lower() == "true",
            triage_url=os.environ.get("TRIAGE_URL", cls.triage_url),
            triage_model=os.environ.get("TRIAGE_MODEL", cls.triage_model),
        )
