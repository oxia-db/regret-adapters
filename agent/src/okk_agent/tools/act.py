from __future__ import annotations

import json
import logging

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)

OKK_GROUP = "core.oxia.io"
OKK_VERSION = "v1"
CHAOS_GROUP = "chaos-mesh.org"
CHAOS_VERSION = "v1alpha1"


class ActTools:
    def __init__(self, config: Config, k8s_custom: k8s_client.CustomObjectsApi):
        self.config = config
        self.k8s_custom = k8s_custom

    def create_testcase(
        self, name: str, type: str, duration: str = "24h",
        op_rate: int = 100, key_space: int = 10000, namespace: str = "okk-system",
    ) -> str:
        body = {
            "apiVersion": f"{OKK_GROUP}/{OKK_VERSION}",
            "kind": "TestCase",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "type": type,
                "duration": duration,
                "opRate": op_rate,
                "properties": {"keySpace": str(key_space)},
                "worker": {
                    "image": self.config.okk_worker_image,
                    "targetCluster": {
                        "name": "okk",
                        "namespace": namespace,
                    },
                },
            },
        }
        try:
            self.k8s_custom.create_namespaced_custom_object(
                group=OKK_GROUP, version=OKK_VERSION, namespace=namespace,
                plural="testcases", body=body,
            )
            return json.dumps({"status": "created", "name": name, "type": type})
        except k8s_client.ApiException as e:
            if e.status == 409:
                return json.dumps({"status": "already_exists", "name": name})
            return json.dumps({"error": f"Failed to create TestCase: {e.reason}"})

    def delete_testcase(self, name: str, namespace: str = "okk-system") -> str:
        try:
            self.k8s_custom.delete_namespaced_custom_object(
                group=OKK_GROUP, version=OKK_VERSION, namespace=namespace,
                plural="testcases", name=name,
            )
            return json.dumps({"status": "deleted", "name": name})
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to delete TestCase: {e.reason}"})

    def inject_chaos(
        self, type: str, target: str, duration: str = "30s",
        namespace: str = "okk-system",
    ) -> str:
        chaos_spec = self._build_chaos_spec(type, target, duration, namespace)
        if "error" in chaos_spec:
            return json.dumps(chaos_spec)

        plural = chaos_spec.pop("_plural")
        try:
            self.k8s_custom.create_namespaced_custom_object(
                group=CHAOS_GROUP, version=CHAOS_VERSION, namespace=namespace,
                plural=plural, body=chaos_spec,
            )
            return json.dumps({"status": "injected", "type": type, "duration": duration})
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to inject chaos: {e.reason}"})

    def _build_chaos_spec(self, type: str, target: str, duration: str, namespace: str) -> dict:
        # Parse label selector into dict
        labels = {}
        for part in target.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                labels[k.strip()] = v.strip()

        selector = {
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": labels,
            },
            "mode": "one",
        }
        name_suffix = type.replace("-", "")

        if type == "pod-kill":
            return {
                "_plural": "podchaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "PodChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {**selector, "action": "pod-kill", "duration": duration},
            }
        elif type == "pod-failure":
            return {
                "_plural": "podchaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "PodChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {**selector, "action": "pod-failure", "duration": duration},
            }
        elif type == "network-delay":
            return {
                "_plural": "networkchaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "NetworkChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {
                    **selector, "action": "delay", "duration": duration,
                    "delay": {"latency": "10ms", "jitter": "5ms"},
                },
            }
        elif type == "network-partition":
            return {
                "_plural": "networkchaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "NetworkChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {
                    **selector, "action": "partition", "duration": duration,
                    "direction": "both",
                },
            }
        elif type == "cpu-stress":
            return {
                "_plural": "stresschaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "StressChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {
                    **selector, "duration": duration,
                    "stressors": {"cpu": {"workers": 1, "load": 100}},
                },
            }
        elif type == "memory-stress":
            return {
                "_plural": "stresschaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "StressChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {
                    **selector, "duration": duration,
                    "stressors": {"memory": {"workers": 1, "size": "256MB"}},
                },
            }
        elif type == "clock-skew":
            return {
                "_plural": "timechaos",
                "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
                "kind": "TimeChaos",
                "metadata": {"name": f"agent-{name_suffix}", "namespace": namespace},
                "spec": {
                    **selector, "duration": duration,
                    "timeOffset": "+10s",
                },
            }
        return {"error": f"Unknown chaos type: {type}"}
