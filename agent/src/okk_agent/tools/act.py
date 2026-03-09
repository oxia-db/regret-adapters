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
    def __init__(self, config: Config, k8s_custom: k8s_client.CustomObjectsApi,
                 k8s_apps: k8s_client.AppsV1Api | None = None):
        self.config = config
        self.k8s_custom = k8s_custom
        self.k8s_apps = k8s_apps or k8s_client.AppsV1Api()
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

    def delete_chaos(self, name: str, namespace: str = "okk-system") -> str:
        """Delete a chaos experiment by name."""
        # Try each chaos type
        for plural, kind in [
            ("podchaos", "PodChaos"),
            ("networkchaos", "NetworkChaos"),
            ("stresschaos", "StressChaos"),
            ("timechaos", "TimeChaos"),
        ]:
            try:
                self.k8s_custom.delete_namespaced_custom_object(
                    group=CHAOS_GROUP, version=CHAOS_VERSION,
                    namespace=namespace, plural=plural, name=name,
                )
                return json.dumps({"status": "deleted", "name": name, "kind": kind})
            except k8s_client.ApiException as e:
                if e.status != 404:
                    return json.dumps({"error": f"Failed to delete {name}: {e.reason}"})
        return json.dumps({"error": f"Chaos experiment '{name}' not found"})

    def scale_oxia(self, replicas: int, namespace: str = "okk-system") -> str:
        """Scale the Oxia StatefulSet to the specified number of replicas."""
        if replicas < 1 or replicas > 10:
            return json.dumps({"error": "Replicas must be between 1 and 10"})

        try:
            # Find the Oxia StatefulSet
            sts_list = self.k8s_apps.list_namespaced_stateful_set(
                namespace=namespace, label_selector="app.kubernetes.io/name=oxia-cluster",
            )
            if not sts_list.items:
                # Fallback: try by name
                sts_list = self.k8s_apps.list_namespaced_stateful_set(namespace=namespace)
                sts_list.items = [s for s in sts_list.items if "oxia" in s.metadata.name]

            if not sts_list.items:
                return json.dumps({"error": "No Oxia StatefulSet found"})

            sts = sts_list.items[0]
            old_replicas = sts.spec.replicas
            sts_name = sts.metadata.name

            self.k8s_apps.patch_namespaced_stateful_set_scale(
                name=sts_name, namespace=namespace,
                body={"spec": {"replicas": replicas}},
            )
            return json.dumps({
                "status": "scaled",
                "statefulset": sts_name,
                "old_replicas": old_replicas,
                "new_replicas": replicas,
            })
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to scale: {e.reason}"})
