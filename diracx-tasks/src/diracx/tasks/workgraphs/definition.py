"""Validate Dirac hints and the supported outer CWL workgraph structure offline."""

from __future__ import annotations

import json
import re
from graphlib import CycleError, TopologicalSorter
from importlib.resources import files
from pathlib import Path

from jsonschema import Draft202012Validator

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}\Z")
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"


class InvalidDefinition(ValueError):  # noqa: N818
    """An invalid or unsupported definition, with a message safe for status."""


def load_schema(path: Path | None = None) -> dict:
    """Read the packaged schema, the editable checkout's schema, or an explicit path."""
    resource = path if path is not None else files(__package__) / "dirac-1.0.json"
    if path is None and not resource.is_file():
        # Editable packages import from src, outside the wheel's bundled resources.
        resource = Path(__file__).resolve().parents[5] / "docs/schemas/dirac-1.0.json"
    return json.loads(resource.read_text(encoding="utf-8"))


def identifier(value) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise InvalidDefinition(
            "Identifiers must be local names of at most 128 characters"
        )
    return value


def entries(value, label: str, shorthand: str | None = None) -> dict:
    """Normalize CWL mapping/list syntax, rejecting duplicate or conflicting ids."""
    if isinstance(value, dict):
        items = []
        for key, item in value.items():
            if not isinstance(item, dict) and shorthand:
                item = {shorthand: item}
            if not isinstance(item, dict):
                raise InvalidDefinition(f"{label} entries must be objects")
            if "id" in item and item["id"] != key:
                raise InvalidDefinition(f"{label} contains a conflicting id")
            items.append((key, item))
    elif isinstance(value, list):
        items = []
        for item in value:
            if not isinstance(item, dict) or "id" not in item:
                raise InvalidDefinition(f"{label} list entries require an id")
            items.append((item["id"], item))
    else:
        raise InvalidDefinition(f"{label} must be a mapping or list")
    result = {}
    for key, item in items:
        key = identifier(key)
        if key in result:
            raise InvalidDefinition(f"{label} contains duplicate ids")
        result[key] = item
    return result


def hints(value) -> dict:
    if isinstance(value, dict):
        result = value
    elif isinstance(value, list):
        result = {}
        for item in value:
            if not isinstance(item, dict) or not isinstance(item.get("class"), str):
                raise InvalidDefinition("Hint list entries require a class")
            name = item["class"]
            if name in result:
                raise InvalidDefinition("Duplicate hint classes are not allowed")
            result[name] = {k: v for k, v in item.items() if k != "class"}
    else:
        raise InvalidDefinition("hints must be a mapping or list")
    for name, value in result.items():
        if not isinstance(value, dict):
            raise InvalidDefinition("Hint values must be objects")
        if name.startswith("dirac:") and name not in {
            "dirac:Workgraph",
            "dirac:Transformation",
            "dirac:Job",
        }:
            raise InvalidDefinition("Unknown Dirac hint class")
    return result


class DefinitionValidator:
    """Check the complete hint schema and a deliberately limited CWL DAG subset."""

    def __init__(self, schema: dict | None = None):
        schema = load_schema() if schema is None else schema
        Draft202012Validator.check_schema(schema)
        self.hint_validator = Draft202012Validator(schema)
        self.feeder_validator = Draft202012Validator(
            {**schema, "$ref": "#/$defs/feeder"}
        )

    @staticmethod
    def check_schema(validator, value, label="Dirac hints"):
        # Do not echo jsonschema's error message: it includes submitted values.
        error = next(validator.iter_errors(value), None)
        if error is not None:
            location = "/".join(map(str, error.absolute_schema_path))
            raise InvalidDefinition(
                f"{label} violate the published schema at {location}"
            )

    def check_hints(self, document):
        """Validate hints even in inline processes, without resolving run references."""

        def check_block(item):
            if "hints" in item:
                self.check_schema(self.hint_validator, hints(item["hints"]))
            if "requirements" not in item:
                return
            requirements = item["requirements"]
            if isinstance(requirements, dict):
                classes = requirements.keys()
            elif isinstance(requirements, list):
                if any(not isinstance(v, dict) for v in requirements):
                    raise InvalidDefinition("requirements list entries must be objects")
                classes = [v.get("class") for v in requirements]
            else:
                raise InvalidDefinition("requirements must be a mapping or list")
            if any(isinstance(c, str) and c.startswith("dirac:") for c in classes):
                raise InvalidDefinition(
                    "Dirac extensions belong in hints, not requirements"
                )

        pending = [document]
        while pending:
            process = pending.pop()
            check_block(process)
            for parameter in entries(
                process.get("inputs", {}), "Process inputs", "type"
            ).values():
                if "dirac:Feeder" in parameter:
                    self.check_schema(
                        self.feeder_validator,
                        parameter["dirac:Feeder"],
                        "dirac:Feeder fields",
                    )
            if process.get("class") == "Workflow":
                for step in entries(
                    process.get("steps", {}), "Workflow steps"
                ).values():
                    check_block(step)
                    if isinstance(step.get("run"), dict):
                        pending.append(step["run"])

    def validate(self, spec) -> dict:
        """Return a summary; never fetch references, evaluate expressions or run tools."""
        try:
            return self._validate(spec)
        except RecursionError:
            raise InvalidDefinition(
                "Definition nesting exceeds the supported depth"
            ) from None

    def _validate(self, spec) -> dict:
        if not isinstance(spec, dict) or not isinstance(spec.get("cwl"), dict):
            raise InvalidDefinition("spec.cwl must be a CWL document object")
        supplied = spec.get("inputs", {})
        if not isinstance(supplied, dict):
            raise InvalidDefinition("spec.inputs must be an object")
        doc = spec["cwl"]
        if doc.get("class") != "Workflow" or doc.get("cwlVersion") != "v1.2":
            raise InvalidDefinition("Only a CWL v1.2 Workflow is supported")
        namespace = doc.get("$namespaces", {})
        if not isinstance(namespace, dict) or namespace.get("dirac") != DIRAC_NAMESPACE:
            raise InvalidDefinition(
                "Bind the dirac namespace to https://diracgrid.org/cwl#"
            )
        self.check_hints(doc)
        top = hints(doc.get("hints", {}))
        if "dirac:Workgraph" not in top:
            raise InvalidDefinition("Workflow requires dirac:Workgraph")
        if {"dirac:Transformation", "dirac:Job"} & top.keys():
            raise InvalidDefinition("Workgraph has a misplaced Dirac hint")
        workgraph = top["dirac:Workgraph"]
        inputs = entries(doc.get("inputs", {}), "Workflow inputs", "type")
        outputs = entries(doc.get("outputs", {}), "Workflow outputs", "type")
        steps = entries(doc.get("steps", {}), "Workflow steps")
        if not steps:
            raise InvalidDefinition("Workgraph must contain at least one step")
        if supplied.keys() - inputs.keys():
            raise InvalidDefinition(
                "spec.inputs contains undeclared workflow parameters"
            )
        for parameter in [*inputs.values(), *outputs.values()]:
            if (
                not isinstance(parameter.get("type"), (str, dict, list))
                or not parameter["type"]
            ):
                raise InvalidDefinition(
                    "Workflow parameters require a nonempty CWL type"
                )
        ports = {}
        transformations = {}
        step_inputs = {}
        for name, step in steps.items():
            step_hints = hints(step.get("hints", {}))
            if "dirac:Transformation" not in step_hints:
                raise InvalidDefinition("Every step requires dirac:Transformation")
            if {"dirac:Workgraph", "dirac:Job"} & step_hints.keys():
                raise InvalidDefinition("Step has a misplaced Dirac hint")
            transformations[name] = step_hints["dirac:Transformation"]
            run = step.get("run")
            if not (
                (isinstance(run, str) and run.strip())
                or (
                    isinstance(run, dict)
                    and run.get("class")
                    in ("CommandLineTool", "Workflow", "ExpressionTool")
                )
            ):
                raise InvalidDefinition(
                    "Each step requires a run reference or inline CWL process"
                )
            out = step.get("out")
            if not isinstance(out, list):
                raise InvalidDefinition("Step out must be a list")
            names = [identifier(p.get("id") if isinstance(p, dict) else p) for p in out]
            if len(names) != len(set(names)):
                raise InvalidDefinition("Step contains duplicate output ids")
            ports[name] = set(names)
            step_inputs[name] = entries(step.get("in", {}), "Step inputs", "source")
            driving = transformations[name].get("driving_input")
            if driving is not None and driving not in step_inputs[name]:
                raise InvalidDefinition("driving_input must name a declared step input")
        data = {}
        for index, item in enumerate(workgraph.get("data_management", [])):
            name = identifier(item.get("id", f"data-{index + 1}"))
            if name in steps or name in data:
                raise InvalidDefinition("Transformation ids must be unique")
            data[name] = item
        graph = {name: set() for name in [*steps, *data]}
        if len(graph) > 1000:
            raise InvalidDefinition("At most 1000 transformations are supported")
        used = set()

        def sources(value):
            values = value if isinstance(value, list) else [value]
            if not values or not all(isinstance(v, str) and v for v in values):
                raise InvalidDefinition(
                    "Sources must be nonempty local identifiers or lists"
                )
            return values

        def resolve(value, *, feeder_only=False):
            if not isinstance(value, str):
                raise InvalidDefinition("Source must be a local identifier")
            value = value.removeprefix("#")
            parts = value.split("/")
            if len(parts) == 1 and value in inputs:
                if feeder_only and "dirac:Feeder" not in inputs[value]:
                    raise InvalidDefinition(
                        "Data management input requires dirac:Feeder"
                    )
                return set()
            if len(parts) == 2 and parts[0] in ports and parts[1] in ports[parts[0]]:
                used.add(value)
                return {parts[0]}
            raise InvalidDefinition(
                "Unknown or unsupported source; use input or step/output"
            )

        for name, bindings in step_inputs.items():
            for binding in bindings.values():
                if "source" not in binding:
                    if "default" not in binding and "valueFrom" not in binding:
                        raise InvalidDefinition(
                            "Step input requires source, default or valueFrom"
                        )
                    continue
                for source in sources(binding["source"]):
                    graph[name].update(resolve(source))
        output_dependencies = {}
        for name, output in outputs.items():
            deps = set()
            for source in sources(output.get("outputSource")):
                deps.update(resolve(source))
            output_dependencies[name] = deps
        target = workgraph.get("target")
        if target is not None and target["output"] not in outputs:
            raise InvalidDefinition("Workgraph target must name a workflow output")
        for name, item in data.items():
            source = item["files"].removeprefix("#")
            if source in outputs:
                graph[name].update(output_dependencies[source])
            else:
                graph[name].update(resolve(source, feeder_only=True))
        for name, item in {**transformations, **data}.items():
            for dependency in item.get("after", []):
                if dependency not in graph:
                    raise InvalidDefinition("after must name a declared transformation")
                graph[name].add(dependency)
        if any(
            f"{name}/{port}" not in used
            for name, names in ports.items()
            for port in names
        ):
            raise InvalidDefinition("Every step output must be consumed or declared")
        try:
            tuple(TopologicalSorter(graph).static_order())
        except CycleError:
            raise InvalidDefinition(
                "Dependencies contain a cycle (dataflow or after)"
            ) from None
        return {
            "steps": len(steps),
            "dataManagement": len(data),
            "dependencies": sum(map(len, graph.values())),
        }
