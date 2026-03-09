from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)

# CRD group/version for okk resources
OKK_GROUP = "core.oxia.io"
OKK_VERSION = "v1"


class ObserveTools:
    def __init__(self, config: Config, k8s_custom: k8s_client.CustomObjectsApi, k8s_core: k8s_client.CoreV1Api):
        self.config = config
        self.k8s_custom = k8s_custom
        self.k8s_core = k8s_core
        self._prometheus_url = config.prometheus_url

    def query_metrics(self, query: str, range_minutes: int = 0) -> str:
        import urllib.request
        import urllib.parse

        if range_minutes > 0:
            now = datetime.now(timezone.utc)
            start = now - timedelta(minutes=range_minutes)
            params = urllib.parse.urlencode({
                "query": query,
                "start": start.isoformat(),
                "end": now.isoformat(),
                "step": "15s",
            })
            url = f"{self._prometheus_url}/api/v1/query_range?{params}"
        else:
            params = urllib.parse.urlencode({"query": query})
            url = f"{self._prometheus_url}/api/v1/query?{params}"

        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                if data.get("status") == "success":
                    results = data.get("data", {}).get("result", [])
                    # Summarize to avoid huge payloads
                    summary = []
                    for r in results[:20]:
                        metric = r.get("metric", {})
                        if "value" in r:
                            summary.append({"metric": metric, "value": r["value"]})
                        elif "values" in r:
                            values = r["values"]
                            summary.append({
                                "metric": metric,
                                "samples": len(values),
                                "first": values[0] if values else None,
                                "last": values[-1] if values else None,
                            })
                    return json.dumps(summary, indent=2)
                return json.dumps({"error": data.get("error", "unknown error")})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_testcase_status(self, name: str, namespace: str = "okk-system") -> str:
        try:
            tc = self.k8s_custom.get_namespaced_custom_object(
                group=OKK_GROUP, version=OKK_VERSION, namespace=namespace,
                plural="testcases", name=name,
            )
            return json.dumps({
                "name": tc["metadata"]["name"],
                "type": tc["spec"].get("type"),
                "duration": tc["spec"].get("duration"),
                "opRate": tc["spec"].get("opRate"),
                "status": tc.get("status", {}),
                "created": tc["metadata"].get("creationTimestamp"),
            }, indent=2)
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to get TestCase {name}: {e.reason}"})

    def list_testcases(self, namespace: str = "okk-system") -> str:
        try:
            result = self.k8s_custom.list_namespaced_custom_object(
                group=OKK_GROUP, version=OKK_VERSION, namespace=namespace,
                plural="testcases",
            )
            testcases = []
            for tc in result.get("items", []):
                testcases.append({
                    "name": tc["metadata"]["name"],
                    "type": tc["spec"].get("type"),
                    "duration": tc["spec"].get("duration"),
                    "opRate": tc["spec"].get("opRate"),
                    "created": tc["metadata"].get("creationTimestamp"),
                })
            return json.dumps(testcases, indent=2)
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to list TestCases: {e.reason}"})

    def get_pod_logs(self, pod_name: str, namespace: str = "okk-system",
                     since_minutes: int = 10, container: str | None = None) -> str:
        try:
            # If pod_name looks like a label selector, find pods first
            if "=" in pod_name:
                pods = self.k8s_core.list_namespaced_pod(
                    namespace=namespace, label_selector=pod_name,
                )
                if not pods.items:
                    return json.dumps({"error": f"No pods found matching {pod_name}"})
                pod_name = pods.items[0].metadata.name

            kwargs = {
                "name": pod_name,
                "namespace": namespace,
                "since_seconds": since_minutes * 60,
                "tail_lines": 200,
            }
            if container:
                kwargs["container"] = container

            logs = self.k8s_core.read_namespaced_pod_log(**kwargs)
            return logs if logs else "(no logs)"
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to get logs for {pod_name}: {e.reason}"})

    def get_pod_status(self, label_selector: str, namespace: str = "okk-system") -> str:
        try:
            pods = self.k8s_core.list_namespaced_pod(
                namespace=namespace, label_selector=label_selector,
            )
            result = []
            for pod in pods.items:
                conditions = []
                if pod.status.conditions:
                    conditions = [
                        {"type": c.type, "status": c.status, "reason": c.reason}
                        for c in pod.status.conditions
                    ]
                result.append({
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,
                    "conditions": conditions,
                    "restarts": sum(
                        cs.restart_count for cs in (pod.status.container_statuses or [])
                    ),
                })
            return json.dumps(result, indent=2)
        except k8s_client.ApiException as e:
            return json.dumps({"error": f"Failed to get pods: {e.reason}"})

    def get_chaos_status(self, namespace: str = "okk-system") -> str:
        chaos_types = [
            ("chaos-mesh.org", "v1alpha1", "podchaos"),
            ("chaos-mesh.org", "v1alpha1", "networkchaos"),
            ("chaos-mesh.org", "v1alpha1", "stresschaos"),
            ("chaos-mesh.org", "v1alpha1", "timechaos"),
        ]
        all_chaos = []
        for group, version, plural in chaos_types:
            try:
                result = self.k8s_custom.list_namespaced_custom_object(
                    group=group, version=version, namespace=namespace, plural=plural,
                )
                for item in result.get("items", []):
                    all_chaos.append({
                        "kind": item["kind"],
                        "name": item["metadata"]["name"],
                        "created": item["metadata"].get("creationTimestamp"),
                        "status": item.get("status", {}),
                    })
            except k8s_client.ApiException:
                pass  # Chaos Mesh CRDs may not be installed
        return json.dumps(all_chaos, indent=2)
