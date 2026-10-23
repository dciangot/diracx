"""Include the authoritative hint schema in wheels and self-contained sdists."""

# ruff: noqa: INP001

from __future__ import annotations

from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Resolve the schema both in the monorepo and in an unpacked sdist."""

    def initialize(self, version, build_data):
        """Package the existing schema without maintaining a source-tree copy."""
        root = Path(self.root)
        relative = Path("docs/schemas/dirac-1.0.json")
        schema = root / relative
        if not schema.is_file():
            schema = root.parent / relative
        if not schema.is_file():
            raise FileNotFoundError("The authoritative Dirac hint schema is missing")
        destination = (
            "diracx/tasks/workgraphs/dirac-1.0.json"
            if self.target_name == "wheel"
            else relative.as_posix()
        )
        build_data["force_include"][str(schema)] = destination
