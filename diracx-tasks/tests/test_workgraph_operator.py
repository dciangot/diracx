# Optional extras must be checked before importing the operator.
# ruff: noqa: E402

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import yaml

pytest.importorskip("kubernetes", minversion="37.0.1")
pytest.importorskip("jsonschema")

from jsonschema import Draft202012Validator
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import HTTPError

from diracx.tasks.workgraphs.definition import (
    DefinitionValidator,
    InvalidDefinition,
    load_schema,
)
from diracx.tasks.workgraphs.operator import (
    GROUP,
    PLURAL,
    REQUEST_TIMEOUT,
    VERSION,
    WorkGraphOperator,
    desired_status,
    main,
)

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy/workgraph-operator"


@pytest.fixture
def validator():
    return DefinitionValidator(load_schema(ROOT / "docs/schemas/dirac-1.0.json"))


@pytest.fixture
def resource():
    result = yaml.safe_load((DEPLOY / "example.yaml").read_text())
    result["metadata"].update(generation=1, resourceVersion="1", namespace="test")
    return result


@pytest.fixture
def doc(resource):
    return resource["spec"]["cwl"]


def test_example_and_no_mutation(validator, resource):
    original = copy.deepcopy(resource)
    assert validator.validate(resource["spec"]) == {
        "steps": 1,
        "dataManagement": 0,
        "dependencies": 0,
    }
    assert resource == original


def test_source_and_editable_schema_loader():
    assert load_schema() == load_schema(ROOT / "docs/schemas/dirac-1.0.json")
    DefinitionValidator()


@pytest.mark.parametrize(
    "path", sorted((ROOT / "docs/assets/examples").glob("*.cwl")), ids=lambda p: p.stem
)
def test_all_repository_examples(validator, path):
    assert validator.validate({"cwl": yaml.safe_load(path.read_text())})["steps"] > 0


def test_list_forms(validator, resource, doc):
    doc["hints"] = [{"class": "dirac:Workgraph", "schema_version": "1.0"}]
    for key in ["inputs", "outputs", "steps"]:
        doc[key] = [{"id": name, **value} for name, value in doc[key].items()]
    step = doc["steps"][0]
    step["hints"] = [{"class": "dirac:Transformation"}]
    step["in"] = [{"id": "seed", "source": "#events"}]
    step["out"] = [{"id": "simulated"}]
    doc["outputs"][0]["outputSource"] = ["#simulate/simulated"]
    assert validator.validate(resource["spec"])["steps"] == 1


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (None, "spec.cwl"),
        ({}, "spec.cwl"),
        ({"cwl": []}, "spec.cwl"),
    ],
)
def test_bad_spec(validator, value, message):
    with pytest.raises(InvalidDefinition, match=message):
        validator.validate(value)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("class", "CommandLineTool", "CWL v1.2"),
        ("cwlVersion", "v1.0", "CWL v1.2"),
        ("$namespaces", None, "namespace"),
        ("hints", {}, "requires dirac:Workgraph"),
        ("hints", "wrong", "hints must"),
        (
            "hints",
            [{"class": "dirac:Workgraph"}, {"class": "dirac:Workgraph"}],
            "Duplicate hint",
        ),
        ("hints", [{"bad": "hint"}], "require a class"),
        ("hints", {"dirac:Workgraph": [], "dirac:Unknown": {}}, "Hint values"),
        ("hints", {"dirac:Unknown": {}}, "Unknown Dirac"),
        ("inputs", [], "Unknown or unsupported source"),
        ("inputs", {"events": {}}, "CWL type"),
        ("steps", {}, "at least one"),
        ("steps", [None], "require an id"),
        ("steps", {"bad/name": {}}, "Identifiers"),
        ("outputs", {"datasets": {"type": "File[]"}}, "Sources must"),
    ],
)
def test_invalid_document_shapes(validator, resource, doc, field, value, message):
    doc[field] = value
    with pytest.raises(InvalidDefinition, match=message):
        validator.validate(resource["spec"])


@pytest.mark.parametrize(
    ("hint", "value"),
    [
        ("unknown_field", True),
        ("schema_version", "2.0"),
        ("type", 123),
        ("target", {"output": "datasets", "files": 0}),
        ("output_sandbox", [12]),
        ("hooks", {"Active": {"hook": 5}}),
        ("hooks", {"InvalidState": {"hook": "H"}}),
        ("approving", [{"action": "A", "unknown": True}]),
        ("data_management", [{"operation": "copy", "files": "events"}]),
        ("data_management", [{"operation": "remove", "files": "events", "packer": {}}]),
    ],
)
def test_complete_workgraph_schema(validator, resource, doc, hint, value):
    doc["hints"]["dirac:Workgraph"][hint] = value
    with pytest.raises(InvalidDefinition, match="published schema"):
        validator.validate(resource["spec"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("packer", {"args": {}}),
        ("after", [7]),
        ("initial_state", "Running"),
        ("actions", {"Cleaning": [{"action": 1}]}),
        ("requirements_template", []),
        ("driving_input", 1),
    ],
)
def test_complete_transformation_schema(validator, resource, doc, field, value):
    doc["steps"]["simulate"]["hints"]["dirac:Transformation"][field] = value
    with pytest.raises(InvalidDefinition, match="published schema"):
        validator.validate(resource["spec"])


@pytest.mark.parametrize("feeder", [{}, {"name": 2}, {"name": "Seeds", "args": []}])
def test_feeder_schema(validator, resource, doc, feeder):
    doc["inputs"]["events"]["dirac:Feeder"] = feeder
    with pytest.raises(InvalidDefinition, match="published schema"):
        validator.validate(resource["spec"])


def test_inline_hints_validated_but_arbitrary_args_not_interpreted(
    validator, resource, doc
):
    hint = doc["steps"]["simulate"]["hints"]["dirac:Transformation"]
    hint["packer"] = {"name": "Plugin", "args": {"hints": "data", "requirements": 42}}
    assert validator.validate(resource["spec"])
    doc["steps"]["simulate"]["run"] = {
        "class": "CommandLineTool",
        "hints": {"dirac:Job": {"requirements_template": []}},
    }
    with pytest.raises(InvalidDefinition, match="published schema"):
        validator.validate(resource["spec"])


@pytest.mark.parametrize(
    "requirements", [{"dirac:Workgraph": {}}, [{"class": "dirac:Job"}]]
)
def test_dirac_requirements_rejected(validator, resource, doc, requirements):
    doc["requirements"] = requirements
    with pytest.raises(InvalidDefinition, match="belong in hints"):
        validator.validate(resource["spec"])


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("hints", {}, "Every step requires"),
        ("hints", {"dirac:Transformation": {}, "dirac:Workgraph": {}}, "misplaced"),
        ("run", None, "requires a run"),
        ("run", {"class": "NotCWL"}, "requires a run"),
        ("out", "port", "out must be a list"),
        ("out", ["simulated", "simulated"], "duplicate output"),
        ("out", ["simulated", "unused"], "must be consumed"),
        ("in", {"seed": {"source": "unknown"}}, "Unknown or unsupported"),
        ("in", {"seed": {"source": "simulate/missing"}}, "Unknown or unsupported"),
        (
            "in",
            {"seed": {"source": "https://example.org/port"}},
            "Unknown or unsupported",
        ),
        ("in", {"seed": {"source": []}}, "Sources must"),
        ("in", {"seed": {}}, "requires source"),
    ],
)
def test_step_validation(validator, resource, doc, field, value, message):
    doc["steps"]["simulate"][field] = value
    with pytest.raises(InvalidDefinition, match=message):
        validator.validate(resource["spec"])


def test_duplicate_and_conflicting_ids(validator, resource, doc):
    step = doc["steps"]["simulate"]
    doc["steps"] = [{"id": "same", **step}, {"id": "same", **step}]
    with pytest.raises(InvalidDefinition, match="duplicate ids"):
        validator.validate(resource["spec"])
    doc["steps"] = {"simulate": {"id": "different", **step}}
    with pytest.raises(InvalidDefinition, match="conflicting id"):
        validator.validate(resource["spec"])


def test_driving_input_and_target(validator, resource, doc):
    transformation = doc["steps"]["simulate"]["hints"]["dirac:Transformation"]
    transformation["driving_input"] = "missing"
    with pytest.raises(InvalidDefinition, match="driving_input"):
        validator.validate(resource["spec"])
    transformation["driving_input"] = "seed"
    doc["hints"]["dirac:Workgraph"]["target"]["output"] = "missing"
    with pytest.raises(InvalidDefinition, match="target"):
        validator.validate(resource["spec"])


def test_inputs_are_parameters_not_authorization(validator, resource):
    resource["spec"]["inputs"] = {"events": []}
    assert validator.validate(resource["spec"])
    resource["spec"]["inputs"] = {"undeclared": "value"}
    with pytest.raises(InvalidDefinition, match="undeclared"):
        validator.validate(resource["spec"])
    resource["spec"]["inputs"] = []
    with pytest.raises(InvalidDefinition, match="spec.inputs"):
        validator.validate(resource["spec"])


def test_defaults_expressions_and_multi_sources_are_not_executed(
    validator, resource, doc
):
    doc["inputs"]["count"] = "int"
    doc["steps"]["simulate"]["in"] = {
        "seed": {"source": ["events", "count"]},
        "constant": {"default": "value"},
        "expression": {"valueFrom": "$(this_is_never_evaluated())"},
    }
    assert validator.validate(resource["spec"])


def test_data_management(validator, resource, doc):
    workgraph = doc["hints"]["dirac:Workgraph"]
    workgraph["data_management"] = [
        {"id": "stage", "operation": "replicate", "files": "events"},
        {
            "id": "clean",
            "operation": "remove",
            "files": "events",
            "after": ["simulate"],
        },
        {"operation": "archive", "files": "datasets"},
    ]
    doc["steps"]["simulate"]["hints"]["dirac:Transformation"]["after"] = ["stage"]
    assert validator.validate(resource["spec"]) == {
        "steps": 1,
        "dataManagement": 3,
        "dependencies": 3,
    }
    workgraph["data_management"].append(
        {"id": "stage", "operation": "remove", "files": "events"}
    )
    with pytest.raises(InvalidDefinition, match="ids must be unique"):
        validator.validate(resource["spec"])


@pytest.mark.parametrize(
    ("files", "message"),
    [("missing", "Unknown or unsupported"), ("events", "requires dirac:Feeder")],
)
def test_bad_data_sources(validator, resource, doc, files, message):
    del doc["inputs"]["events"]["dirac:Feeder"]
    doc["hints"]["dirac:Workgraph"]["data_management"] = [
        {"operation": "remove", "files": files}
    ]
    with pytest.raises(InvalidDefinition, match=message):
        validator.validate(resource["spec"])


@pytest.mark.parametrize("mode", ["dataflow", "after", "data-management"])
def test_cycles(validator, resource, doc, mode):
    transformation = doc["steps"]["simulate"]["hints"]["dirac:Transformation"]
    if mode == "dataflow":
        doc["steps"]["simulate"]["in"]["seed"] = "simulate/simulated"
    elif mode == "after":
        transformation["after"] = ["simulate"]
    else:
        transformation["after"] = ["replicate"]
        doc["hints"]["dirac:Workgraph"]["data_management"] = [
            {"id": "replicate", "operation": "replicate", "files": "datasets"}
        ]
    with pytest.raises(InvalidDefinition, match="cycle"):
        validator.validate(resource["spec"])


def test_unknown_after(validator, resource, doc):
    doc["steps"]["simulate"]["hints"]["dirac:Transformation"]["after"] = ["missing"]
    with pytest.raises(InvalidDefinition, match="after must name"):
        validator.validate(resource["spec"])


def test_data_management_can_consume_an_otherwise_orphan_output(
    validator, resource, doc
):
    doc["steps"]["simulate"]["out"].append("intermediate")
    doc["hints"]["dirac:Workgraph"]["data_management"] = [
        {"operation": "remove", "files": "simulate/intermediate"}
    ]
    assert validator.validate(resource["spec"])["dependencies"] == 1


def test_status_transitions_and_generation(validator, resource):
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = datetime(2026, 1, 2, tzinfo=timezone.utc)
    status = desired_status(resource, validator, first)
    assert status["phase"] == "Defined"
    assert status["conditions"][0]["status"] == "True"
    resource["status"] = status
    resource["metadata"]["generation"] = 2
    updated = desired_status(resource, validator, later)
    assert updated["observedGeneration"] == 2
    assert updated["conditions"][0]["observedGeneration"] == 2
    assert updated["conditions"][0]["lastTransitionTime"] == "2026-01-01T00:00:00Z"
    resource["spec"]["cwl"]["class"] = "bad"
    invalid = desired_status(resource, validator, later)
    assert invalid["summary"] is None
    assert invalid["phase"] == "Invalid"
    assert invalid["conditions"][0]["reason"] == "InvalidDefinition"
    assert invalid["conditions"][0]["lastTransitionTime"] == "2026-01-02T00:00:00Z"
    resource["status"] = invalid
    assert desired_status(resource, validator, later) == invalid
    resource["spec"]["cwl"]["class"] = "Workflow"
    assert desired_status(resource, validator, later)["phase"] == "Defined"


def test_schema_error_does_not_echo_values(validator, resource):
    secret = "SENSITIVE_DO_NOT_ECHO"
    resource["spec"]["cwl"]["hints"]["dirac:Workgraph"]["schema_version"] = secret
    assert secret not in json.dumps(desired_status(resource, validator))


def test_reconcile_patch_and_noop(validator, resource):
    api = Mock()
    operator = WorkGraphOperator(api, "test", validator)
    operator.reconcile(resource)
    args, kwargs = api.patch_namespaced_custom_object_status.call_args
    assert args[:5] == (GROUP, VERSION, "test", PLURAL, resource["metadata"]["name"])
    assert args[5]["metadata"] == {"resourceVersion": "1"}
    assert kwargs == {
        "_content_type": "application/merge-patch+json",
        "_request_timeout": REQUEST_TIMEOUT,
    }
    resource["status"] = args[5]["status"]
    operator.reconcile(resource)
    assert api.patch_namespaced_custom_object_status.call_count == 1
    resource["metadata"]["generation"] = 2
    operator.reconcile(resource)
    assert api.patch_namespaced_custom_object_status.call_count == 2


def test_invalid_reconcile_clears_old_summary(validator, resource):
    resource["status"] = desired_status(resource, validator)
    resource["spec"]["cwl"]["class"] = "bad"
    api = Mock()
    operator = WorkGraphOperator(api, "test", validator)
    operator.reconcile(resource)
    body = api.patch_namespaced_custom_object_status.call_args.args[5]
    assert body["status"]["summary"] is None
    resource["status"] = {k: v for k, v in body["status"].items() if v is not None}
    operator.reconcile(resource)
    assert api.patch_namespaced_custom_object_status.call_count == 1


def test_terminating_skipped(validator, resource):
    api = Mock()
    resource["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    WorkGraphOperator(api, "test", validator).reconcile(resource)
    api.patch_namespaced_custom_object_status.assert_not_called()


def test_conflict_reads_latest_and_revalidates(validator, resource):
    api = Mock()
    latest = copy.deepcopy(resource)
    latest["metadata"].update(resourceVersion="2", generation=2)
    latest["spec"]["cwl"]["class"] = "bad"
    api.patch_namespaced_custom_object_status.side_effect = [
        ApiException(status=409),
        {},
    ]
    api.get_namespaced_custom_object.return_value = latest
    WorkGraphOperator(api, "test", validator).reconcile(resource)
    body = api.patch_namespaced_custom_object_status.call_args.args[5]
    assert body["metadata"]["resourceVersion"] == "2"
    assert body["status"]["phase"] == "Invalid"
    assert body["status"]["observedGeneration"] == 2


@pytest.mark.parametrize("read_deleted", [True, False])
def test_not_found_is_benign(validator, resource, read_deleted):
    api = Mock()
    api.patch_namespaced_custom_object_status.side_effect = ApiException(
        status=409 if read_deleted else 404
    )
    api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    WorkGraphOperator(api, "test", validator).reconcile(resource)
    assert api.patch_namespaced_custom_object_status.call_count == 1


def test_conflicts_bounded(validator, resource):
    api = Mock()
    api.patch_namespaced_custom_object_status.side_effect = ApiException(status=409)
    api.get_namespaced_custom_object.return_value = resource
    WorkGraphOperator(api, "test", validator).reconcile(resource)
    assert api.patch_namespaced_custom_object_status.call_count == 3
    assert api.get_namespaced_custom_object.call_count == 3


def test_pagination_and_per_resource_failures(validator, resource):
    api = Mock()
    api.list_namespaced_custom_object.side_effect = [
        {"items": [resource, resource], "metadata": {"continue": "page2"}},
        {"items": [resource], "metadata": {}},
    ]
    api.patch_namespaced_custom_object_status.side_effect = [
        ApiException(status=503, reason="sensitive error body"),
        {},
        {},
    ]
    WorkGraphOperator(api, "test", validator).poll()
    assert api.patch_namespaced_custom_object_status.call_count == 3
    assert api.list_namespaced_custom_object.call_args.kwargs["_continue"] == "page2"
    assert api.list_namespaced_custom_object.call_args.kwargs["limit"] == 100


@pytest.mark.parametrize(
    "error", [ApiException(status=503), HTTPError("transport"), OSError()]
)
def test_transient_list_failure_recovers(validator, resource, error):
    api = Mock()
    api.list_namespaced_custom_object.side_effect = [
        error,
        {"items": [resource], "metadata": {}},
    ]
    operator = WorkGraphOperator(api, "test", validator)
    operator.poll()
    operator.poll()
    api.patch_namespaced_custom_object_status.assert_called_once()


def test_list_pass_is_bounded(validator):
    api = Mock()
    api.list_namespaced_custom_object.return_value = {
        "items": [],
        "metadata": {"continue": "next"},
    }
    operator = WorkGraphOperator(api, "test", validator)
    operator.poll()
    assert api.list_namespaced_custom_object.call_count == 100
    assert operator.continuation == "next"
    api.list_namespaced_custom_object.return_value = {"items": [], "metadata": {}}
    operator.poll()
    assert api.list_namespaced_custom_object.call_args.kwargs["_continue"] == "next"
    assert operator.continuation is None


def test_expired_pagination_restarts(validator):
    api = Mock()
    api.list_namespaced_custom_object.side_effect = ApiException(status=410)
    operator = WorkGraphOperator(api, "test", validator)
    operator.continuation = "expired"
    operator.poll()
    assert operator.continuation is None


def test_real_client_serializes_status_merge_patch(validator, resource):
    with client.ApiClient() as api_client:
        api_client._call_with_legacy_options = Mock(return_value={})
        WorkGraphOperator(
            client.CustomObjectsApi(api_client), "test", validator
        ).reconcile(resource)
        method, path, headers, body = (
            api_client._call_with_legacy_options.call_args.args[0][:4]
        )
        assert method == "PATCH"
        assert (
            path == f"/apis/{GROUP}/{VERSION}/namespaces/test/{PLURAL}/"
            f"{resource['metadata']['name']}/status"
        )
        assert headers["Content-Type"] == "application/merge-patch+json"
        assert body["metadata"] == {"resourceVersion": "1"}
        assert set(body) == {"metadata", "status"}
        assert body["status"]["phase"] == "Defined"
        assert body["status"]["observedGeneration"] == 1
        assert body["status"]["conditions"][0]["status"] == "True"
        assert (
            api_client._call_with_legacy_options.call_args.args[-2] == REQUEST_TIMEOUT
        )


def test_graph_size_bounded(validator, resource, doc):
    step = doc["steps"]["simulate"]
    step["out"] = []
    doc["outputs"] = {}
    doc["hints"]["dirac:Workgraph"].pop("target")
    doc["steps"] = {f"step-{n}": copy.deepcopy(step) for n in range(1001)}
    with pytest.raises(InvalidDefinition, match="1000"):
        validator.validate(resource["spec"])


def test_cli_cluster_auth_does_not_fallback(monkeypatch):
    monkeypatch.setattr("sys.argv", ["operator"])
    load_cluster, load_local = Mock(side_effect=RuntimeError("no cluster")), Mock()
    monkeypatch.setattr(
        "diracx.tasks.workgraphs.operator.config.load_incluster_config", load_cluster
    )
    monkeypatch.setattr(
        "diracx.tasks.workgraphs.operator.config.load_kube_config", load_local
    )
    with pytest.raises(RuntimeError, match="no cluster"):
        main()
    load_cluster.assert_called_once()
    load_local.assert_not_called()


def test_cli_explicit_kubeconfig_and_namespace_env(monkeypatch, validator):
    monkeypatch.setattr("sys.argv", ["operator", "--kubeconfig", "/explicit/config"])
    monkeypatch.setenv("POD_NAMESPACE", "managed")
    module = "diracx.tasks.workgraphs.operator"
    load_local, load_cluster = Mock(), Mock()
    monkeypatch.setattr(f"{module}.config.load_kube_config", load_local)
    monkeypatch.setattr(f"{module}.config.load_incluster_config", load_cluster)
    monkeypatch.setattr(f"{module}.DefinitionValidator", Mock(return_value=validator))
    context = MagicMock()
    api_factory = Mock(return_value=context)
    monkeypatch.setattr(f"{module}.client.ApiClient", api_factory)
    operator = Mock()
    operator.poll.side_effect = KeyboardInterrupt()
    factory = Mock(return_value=operator)
    monkeypatch.setattr(f"{module}.WorkGraphOperator", factory)
    main()
    load_local.assert_called_once_with(config_file="/explicit/config")
    load_cluster.assert_not_called()
    assert factory.call_args.args[1] == "managed"
    assert api_factory.call_args.args[0].retries == 0


@pytest.mark.parametrize("interval", ["0", "-1", "nan", "inf"])
def test_cli_rejects_invalid_intervals(monkeypatch, interval):
    monkeypatch.setattr("sys.argv", ["operator", "--interval", interval])
    with pytest.raises(SystemExit):
        main()


def test_manifest_consistency_and_security(validator):
    crd = yaml.safe_load((DEPLOY / "crd.yaml").read_text())
    assert crd["spec"]["scope"] == "Namespaced"
    assert crd["spec"]["group"] == GROUP
    assert crd["spec"]["names"]["plural"] == PLURAL
    version = crd["spec"]["versions"][0]
    assert version["name"] == VERSION
    assert version["subresources"] == {"status": {}}
    schema = version["schema"]["openAPIV3Schema"]
    assert schema["properties"]["spec"]["properties"]["cwl"][
        "x-kubernetes-preserve-unknown-fields"
    ]
    conditions = schema["properties"]["status"]["properties"]["conditions"]
    assert conditions["x-kubernetes-list-type"] == "map"
    assert conditions["x-kubernetes-list-map-keys"] == ["type"]
    example = yaml.safe_load((DEPLOY / "example.yaml").read_text())
    example["metadata"]["generation"] = 1
    example["status"] = desired_status(example, validator)
    Draft202012Validator(schema).validate(example)
    example["spec"]["cwl"]["class"] = "invalid"
    example["status"] = {
        k: v for k, v in desired_status(example, validator).items() if v is not None
    }
    Draft202012Validator(schema).validate(example)
    resources = list(yaml.safe_load_all((DEPLOY / "operator.yaml").read_text()))
    by_kind = {r["kind"]: r for r in resources}
    assert set(by_kind) == {"ServiceAccount", "Role", "RoleBinding", "Deployment"}
    assert by_kind["Role"]["rules"] == [
        {"apiGroups": [GROUP], "resources": [PLURAL], "verbs": ["get", "list"]},
        {"apiGroups": [GROUP], "resources": [f"{PLURAL}/status"], "verbs": ["patch"]},
    ]
    account = by_kind["ServiceAccount"]["metadata"]["name"]
    binding = by_kind["RoleBinding"]
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": account}]
    assert binding["roleRef"]["name"] == by_kind["Role"]["metadata"]["name"]
    pod = by_kind["Deployment"]["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == account
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["runAsUser"] > 0
    assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    container = pod["containers"][0]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }
    assert set(container["resources"]) == {"requests", "limits"}
    assert (
        container["env"][0]["valueFrom"]["fieldRef"]["fieldPath"]
        == "metadata.namespace"
    )
    assert validator.validate(
        yaml.safe_load((DEPLOY / "example.yaml").read_text())["spec"]
    )
