from __future__ import annotations

import json
import logging

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)


class StateTools:
    """Persistent agent state backed by a Kubernetes ConfigMap."""

    def __init__(self, config: Config, k8s_core: k8s_client.CoreV1Api):
        self.config = config
        self.k8s_core = k8s_core
        self._cm_name = config.state_configmap
        self._namespace = config.namespace
        self._ensure_configmap()

    def _ensure_configmap(self):
        try:
            self.k8s_core.read_namespaced_config_map(self._cm_name, self._namespace)
        except k8s_client.ApiException as e:
            if e.status == 404:
                cm = k8s_client.V1ConfigMap(
                    metadata=k8s_client.V1ObjectMeta(name=self._cm_name),
                    data={},
                )
                self.k8s_core.create_namespaced_config_map(self._namespace, cm)
                logger.info("Created state ConfigMap %s", self._cm_name)
            else:
                raise

    def get_agent_state(self, key: str) -> str:
        try:
            cm = self.k8s_core.read_namespaced_config_map(self._cm_name, self._namespace)
            value = (cm.data or {}).get(key)
            if value is None:
                return json.dumps({"value": None})
            return value
        except k8s_client.ApiException as e:
            return json.dumps({"error": str(e)})

    def set_agent_state(self, key: str, value: str) -> str:
        try:
            cm = self.k8s_core.read_namespaced_config_map(self._cm_name, self._namespace)
            if cm.data is None:
                cm.data = {}
            cm.data[key] = value
            self.k8s_core.replace_namespaced_config_map(self._cm_name, self._namespace, cm)
            return json.dumps({"status": "updated", "key": key})
        except k8s_client.ApiException as e:
            return json.dumps({"error": str(e)})
