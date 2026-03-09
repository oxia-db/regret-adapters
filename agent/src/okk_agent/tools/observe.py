from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)


class ObserveTools:
    def __init__(self, config: Config, k8s_core: k8s_client.CoreV1Api):
        self.config = config
        self.k8s_core = k8s_core
        self._prometheus_url = config.prometheus_url
        self._coordinator_url = config.coordinator_url.rstrip("/")

    def query_metrics(self, query: str, range_minutes: int = 0) -> str:
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

    def get_testcase_status(self, name: str) -> str:
        try:
            url = f"{self._coordinator_url}/testcases/{urllib.parse.quote(name)}"
            with urllib.request.urlopen(url, timeout=10) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            return json.dumps({"error": f"Failed to get testcase {name}: {error_body}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def list_testcases(self) -> str:
        try:
            url = f"{self._coordinator_url}/testcases"
            with urllib.request.urlopen(url, timeout=10) as resp:
                return resp.read().decode()
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_pod_logs(self, pod_name: str, namespace: str = "okk-system",
                     since_minutes: int = 10, container: str | None = None) -> str:
        try:
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
        # Note: This still uses k8s_custom for Chaos Mesh CRDs
        from kubernetes import client as k8s_client
        k8s_custom = k8s_client.CustomObjectsApi()

        chaos_types = [
            ("chaos-mesh.org", "v1alpha1", "podchaos"),
            ("chaos-mesh.org", "v1alpha1", "networkchaos"),
            ("chaos-mesh.org", "v1alpha1", "stresschaos"),
            ("chaos-mesh.org", "v1alpha1", "timechaos"),
        ]
        all_chaos = []
        for group, version, plural in chaos_types:
            try:
                result = k8s_custom.list_namespaced_custom_object(
                    group=group, version=version, namespace=namespace, plural=plural,
                )
                for item in result.get("items", []):
                    all_chaos.append({
                        "kind": item["kind"],
                        "name": item["metadata"]["name"],
                        "created": item["metadata"].get("creationTimestamp"),
                        "status": item.get("status", {}),
                    })
            except Exception:
                pass
        return json.dumps(all_chaos, indent=2)
