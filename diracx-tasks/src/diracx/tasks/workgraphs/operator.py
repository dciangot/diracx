"""Poll namespaced WorkGraphs and reconcile definition validation status only."""

from __future__ import annotations

import argparse
import logging
import math
import os
import time
from datetime import datetime, timezone

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import HTTPError

from .definition import DefinitionValidator, InvalidDefinition

GROUP = "workgraphs.diracx.io"
VERSION = "v1alpha1"
PLURAL = "workgraphs"
REQUEST_TIMEOUT = (5, 20)
LOG = logging.getLogger(__name__)


def desired_status(resource, validator, now=None):
    metadata = resource["metadata"]
    generation = metadata["generation"]
    summary = None
    try:
        summary = validator.validate(resource.get("spec"))
        valid, reason, message = (
            "True",
            "DefinitionValid",
            "Definition validated; execution is not implemented by this operator.",
        )
    except InvalidDefinition as error:
        valid, reason, message = "False", "InvalidDefinition", str(error)
    previous = next(
        (
            c
            for c in resource.get("status", {}).get("conditions", [])
            if c.get("type") == "Validated"
        ),
        {},
    )
    transition = previous.get("lastTransitionTime")
    if previous.get("status") != valid or not transition:
        transition = (
            (now or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")
        )
    condition = {
        "type": "Validated",
        "status": valid,
        "reason": reason,
        "message": message,
        "observedGeneration": generation,
        "lastTransitionTime": transition,
    }
    return {
        "observedGeneration": generation,
        "phase": "Defined" if valid == "True" else "Invalid",
        "conditions": [condition],
        "summary": summary,
    }


class WorkGraphOperator:
    """Use only namespaced list/get and optimistic-concurrency status patches."""

    def __init__(self, api, namespace, validator):
        self.api = api
        self.namespace = namespace
        self.validator = validator
        self.continuation = None

    def reconcile(self, resource):
        for _ in range(3):
            if resource["metadata"].get("deletionTimestamp"):
                return
            status = desired_status(resource, self.validator)
            old = resource.get("status", {})
            # Merge-patch removes a null summary instead of storing it.
            if all(old.get(key) == value for key, value in status.items()):
                return
            name = resource["metadata"]["name"]
            try:
                self.api.patch_namespaced_custom_object_status(
                    GROUP,
                    VERSION,
                    self.namespace,
                    PLURAL,
                    name,
                    {
                        "metadata": {
                            "resourceVersion": resource["metadata"]["resourceVersion"]
                        },
                        "status": status,
                    },
                    _content_type="application/merge-patch+json",
                    _request_timeout=REQUEST_TIMEOUT,
                )
                return
            except ApiException as error:
                if error.status == 404:
                    return
                if error.status != 409:
                    raise
                try:
                    resource = self.api.get_namespaced_custom_object(
                        GROUP,
                        VERSION,
                        self.namespace,
                        PLURAL,
                        name,
                        _request_timeout=REQUEST_TIMEOUT,
                    )
                except ApiException as error:
                    if error.status == 404:
                        return
                    raise
        LOG.warning("Status conflict retry limit reached; retrying next poll")

    def poll(self):
        """Bound each pagination pass and isolate transient per-resource failures."""
        try:
            for _ in range(100):
                page = self.api.list_namespaced_custom_object(
                    GROUP,
                    VERSION,
                    self.namespace,
                    PLURAL,
                    limit=100,
                    _continue=self.continuation,
                    _request_timeout=REQUEST_TIMEOUT,
                )
                for resource in page["items"]:
                    try:
                        self.reconcile(resource)
                    except (ApiException, HTTPError, OSError):
                        LOG.warning(
                            "WorkGraph status update failed; retrying next poll"
                        )
                self.continuation = page.get("metadata", {}).get("continue") or None
                if not self.continuation:
                    return
            LOG.info("Pagination limit reached; continuing next poll")
        except ApiException as error:
            if error.status == 410:
                self.continuation = None
            LOG.warning("WorkGraph list failed; retrying next poll")
        except (HTTPError, OSError):
            LOG.warning("WorkGraph list failed; retrying next poll")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--namespace", default=os.environ.get("POD_NAMESPACE", "default")
    )
    parser.add_argument(
        "--kubeconfig", help="Explicit local kubeconfig; otherwise use in-cluster auth"
    )
    parser.add_argument(
        "--interval", type=float, default=30, help="Polling interval in seconds (>=1)"
    )
    args = parser.parse_args()
    if not args.namespace:
        parser.error("--namespace must be nonempty")
    if not math.isfinite(args.interval) or args.interval < 1:
        parser.error("--interval must be finite and at least one second")
    logging.basicConfig(level=logging.INFO)
    if args.kubeconfig:
        config.load_kube_config(config_file=args.kubeconfig)
    else:
        config.load_incluster_config()
    validator = DefinitionValidator()
    configuration = client.Configuration.get_default_copy()
    configuration.retries = 0
    with client.ApiClient(configuration) as api_client:
        operator = WorkGraphOperator(
            client.CustomObjectsApi(api_client), args.namespace, validator
        )
        try:
            while True:
                operator.poll()
                time.sleep(args.interval)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
