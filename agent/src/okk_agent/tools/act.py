from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)

CHAOS_GROUP = "chaos-mesh.org"
CHAOS_VERSION = "v1alpha1"


class ActTools:
    def __init__(self, config: Config, k8s_custom: k8s_client.CustomObjectsApi):
        self.config = config
        self.k8s_custom = k8s_custom
        self._coordinator_url = config.coordinator_url.rstrip("/")

    def create_testcase(
        self, name: str, type: str, worker_endpoint: str = "",
        duration: str = "24h", op_rate: int = 100, key_space: int = 10000,
        namespace: str = "default",
    ) -> str:
        if not worker_endpoint:
            worker_endpoint = f"okk-jvm-worker:6666"

        body = json.dumps({
            "name": name,
            "type": type,
            "namespace": namespace,
            "workerEndpoint": worker_endpoint,
            "opRate": op_rate,
            "duration": duration,
            "properties": {"keySpace": str(key_space)},
        }).encode()

        try:
            req = urllib.request.Request(
                f"{self._coordinator_url}/testcases",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            return json.dumps({"error": f"Failed to create testcase: {error_body}"})
        except Exception as e:
            return json.dumps({"error": f"Failed to create testcase: {str(e)}"})

    def delete_testcase(self, name: str) -> str:
        try:
            req = urllib.request.Request(
                f"{self._coordinator_url}/testcases/{name}",
                method="DELETE",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            return json.dumps({"error": f"Failed to delete testcase: {error_body}"})
        except Exception as e:
            return json.dumps({"error": f"Failed to delete testcase: {str(e)}"})

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
