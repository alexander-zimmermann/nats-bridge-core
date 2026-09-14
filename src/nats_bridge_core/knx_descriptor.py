"""KNX descriptor: which published field goes on which datapoint, with which DPT.

A sidecar bridge ships `knx.yaml` at its package root. The descriptor describes the
product family — subjects, fields, DPTs, writer behaviour — never the house: group
addresses and device names are bound in lares. Schema and loader live here so every
bridge validates the same format in CI and the mapping generator reads it through one
loader at the deployed tag.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import jsonschema
import yaml

DESCRIPTOR_RESOURCE = "knx.yaml"


class DescriptorError(ValueError):
    """The descriptor is missing, unreadable or does not match the schema."""


@dataclass(frozen=True, slots=True)
class FieldDescriptor:
    # Payload field name; the writer reads it at `$.<name>`
    name: str
    # Last segment(s) of the group-address name, e.g. `Programm-Phase`; candidates in order
    datapoints: tuple[str, ...]
    # `main.sub` with a three-digit sub, as in the writer rules and the catalog
    dpt: str
    seed_on_start: bool = False
    min_delta: float | None = None
    min_delta_pct: float | None = None

    @property
    def payload_path(self) -> str:
        return f"$.{self.name}"


@dataclass(frozen=True, slots=True)
class SubjectDescriptor:
    # Subject suffix after the device segment: `state`, `environment`, `availability`
    suffix: str
    fields: Mapping[str, FieldDescriptor]


@dataclass(frozen=True, slots=True)
class Descriptor:
    subjects: Mapping[str, SubjectDescriptor]


def load(path: Path) -> Descriptor:
    """Load and validate a descriptor file."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise DescriptorError(f"{path}: {exc.strerror}") from exc
    return _parse(text, source=str(path))


def load_package(package: str) -> Descriptor:
    """Load the `knx.yaml` a bridge ships at its package root."""
    source = f"{package}:{DESCRIPTOR_RESOURCE}"
    try:
        text = resources.files(package).joinpath(DESCRIPTOR_RESOURCE).read_text(encoding="utf-8")
    except ModuleNotFoundError as exc:
        raise DescriptorError(f"{source}: {exc}") from exc
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise DescriptorError(f"{source}: no {DESCRIPTOR_RESOURCE} at the package root") from exc
    return _parse(text, source=source)


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate and non-string keys instead of keeping the last one."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Hashable, Any]:
        seen: set[str] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found non-string key {key!r} (quote YAML 1.1 words like on/off/yes/no)",
                    key_node.start_mark,
                )
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


@functools.cache
def _validator() -> jsonschema.Draft202012Validator:
    schema = resources.files("nats_bridge_core").joinpath("_schemas", "knx-descriptor.schema.json")
    return jsonschema.Draft202012Validator(json.loads(schema.read_text(encoding="utf-8")))


def _candidates(datapoint: str | list[str]) -> tuple[str, ...]:
    return (datapoint,) if isinstance(datapoint, str) else tuple(datapoint)


def _parse(text: str, *, source: str) -> Descriptor:
    try:
        data: Any = yaml.load(text, Loader=_StrictLoader)
    except yaml.YAMLError as exc:
        raise DescriptorError(f"{source}: {exc}") from exc
    if not isinstance(data, dict):
        raise DescriptorError(
            f"{source}: expected a mapping at the top level, got {type(data).__name__}"
        )

    # Every error at once, so a bridge author fixes all typos in one CI round
    errors = sorted(_validator().iter_errors(data), key=lambda e: [str(p) for p in e.absolute_path])
    if errors:
        lines = [
            f"{'.'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}" for e in errors
        ]
        raise DescriptorError(f"{source}: " + "; ".join(lines))

    subjects = {
        suffix: SubjectDescriptor(
            suffix=suffix,
            fields={
                name: FieldDescriptor(
                    name=name,
                    datapoints=_candidates(raw["datapoint"]),
                    dpt=raw["dpt"],
                    seed_on_start=raw.get("seed_on_start", False),
                    min_delta=raw.get("min_delta"),
                    min_delta_pct=raw.get("min_delta_pct"),
                )
                for name, raw in subject["fields"].items()
            },
        )
        for suffix, subject in data["subjects"].items()
    }
    return Descriptor(subjects=subjects)
