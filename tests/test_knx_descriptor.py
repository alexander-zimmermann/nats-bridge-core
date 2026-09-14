"""KNX descriptor loader: the typed model from a valid knx.yaml, and rejection of drift."""

from __future__ import annotations

from pathlib import Path

import pytest

from nats_bridge_core import knx_descriptor
from nats_bridge_core.knx_descriptor import DescriptorError, FieldDescriptor

VALID = """
subjects:
  state:
    fields:
      status:
        datapoint: Status
        dpt: "5.010"
        seed_on_start: true
        min_delta: 0
      remaining_minutes:
        datapoint: Restzeit
        dpt: "7.006"
        min_delta: 1
        min_delta_pct: 5
  environment:
    fields:
      temperature_c:
        datapoint: Raum.Ist-Temperatur
        dpt: "9.001"
"""


def write_descriptor(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "knx.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_valid_descriptor_loads_into_typed_model(tmp_path: Path) -> None:
    descriptor = knx_descriptor.load(write_descriptor(tmp_path, VALID))

    assert list(descriptor.subjects) == ["state", "environment"]
    state = descriptor.subjects["state"]
    assert state.suffix == "state"
    assert list(state.fields) == ["status", "remaining_minutes"]
    assert state.fields["status"] == FieldDescriptor(
        name="status", datapoints=("Status",), dpt="5.010", seed_on_start=True, min_delta=0
    )
    assert state.fields["remaining_minutes"] == FieldDescriptor(
        name="remaining_minutes",
        datapoints=("Restzeit",),
        dpt="7.006",
        min_delta=1,
        min_delta_pct=5,
    )


def test_datapoint_list_keeps_candidates_in_order(tmp_path: Path) -> None:
    text = VALID.replace("datapoint: Status\n", "datapoint: [Hinweis-Fertig, Hinweis]\n")

    descriptor = knx_descriptor.load(write_descriptor(tmp_path, text))

    assert descriptor.subjects["state"].fields["status"].datapoints == ("Hinweis-Fertig", "Hinweis")


def test_behaviour_keys_default_to_off(tmp_path: Path) -> None:
    descriptor = knx_descriptor.load(write_descriptor(tmp_path, VALID))

    field = descriptor.subjects["environment"].fields["temperature_c"]
    assert field.seed_on_start is False
    assert field.min_delta is None
    assert field.min_delta_pct is None


def test_payload_path_is_dollar_dot_field_name(tmp_path: Path) -> None:
    descriptor = knx_descriptor.load(write_descriptor(tmp_path, VALID))

    assert descriptor.subjects["state"].fields["status"].payload_path == "$.status"


def test_load_from_package_resource() -> None:
    descriptor = knx_descriptor.load_package("knx_descriptor_fixture")

    assert list(descriptor.subjects) == ["state", "environment"]
    assert descriptor.subjects["state"].fields["filter_life"].datapoints == ("Filter.Restlaufzeit",)
    assert descriptor.subjects["environment"].fields["pm25"].dpt == "9.030"


def test_package_without_descriptor_names_the_resource() -> None:
    with pytest.raises(DescriptorError, match="nats_bridge_core.*knx.yaml"):
        knx_descriptor.load_package("nats_bridge_core")


def test_unknown_package_is_a_descriptor_error() -> None:
    with pytest.raises(DescriptorError, match="no_such_bridge"):
        knx_descriptor.load_package("no_such_bridge")


@pytest.mark.parametrize(
    ("text", "names"),
    [
        pytest.param(
            VALID.replace('dpt: "7.006"\n', 'dpt: "7.006"\n        ga: 1/2/3\n'),
            ["remaining_minutes", "ga"],
            id="unknown-field-key",
        ),
        pytest.param(
            VALID.replace("  environment:\n", "  environment:\n    device: backofen\n"),
            ["environment", "device"],
            id="unknown-subject-key",
        ),
        pytest.param(
            VALID + "version: 1\n",
            ["version"],
            id="unknown-top-level-key",
        ),
        pytest.param(
            VALID.replace("        datapoint: Status\n", ""),
            ["status", "datapoint"],
            id="missing-datapoint",
        ),
        pytest.param(
            VALID.replace('        dpt: "9.001"\n', ""),
            ["temperature_c", "dpt"],
            id="missing-dpt",
        ),
        pytest.param(
            VALID.replace('dpt: "5.010"', 'dpt: "5.10"'),
            ["status", "dpt"],
            id="dpt-sub-not-three-digits",
        ),
        pytest.param(
            VALID.replace('dpt: "5.010"', "dpt: 5.010"),
            ["status", "dpt"],
            id="dpt-not-a-string",
        ),
        pytest.param(
            VALID.replace("min_delta: 1\n", "min_delta: -1\n"),
            ["remaining_minutes", "min_delta"],
            id="negative-min-delta",
        ),
        pytest.param(
            VALID.replace("min_delta_pct: 5\n", "min_delta_pct: -5\n"),
            ["remaining_minutes", "min_delta_pct"],
            id="negative-min-delta-pct",
        ),
        pytest.param(
            VALID.replace("seed_on_start: true", "seed_on_start: yes please"),
            ["status", "seed_on_start"],
            id="seed-on-start-not-bool",
        ),
        pytest.param(
            VALID.replace("      status:\n", "      status text:\n"),
            ["status text"],
            id="field-name-not-a-json-identifier",
        ),
        pytest.param(
            VALID.replace("        datapoint: Status\n", "        datapoint: ''\n"),
            ["status", "datapoint"],
            id="empty-datapoint",
        ),
        pytest.param(
            VALID.replace("datapoint: Status\n", "datapoint: []\n"),
            ["status", "datapoint"],
            id="empty-datapoint-list",
        ),
        pytest.param(
            VALID.replace("datapoint: Status\n", "datapoint: [Hinweis, Hinweis]\n"),
            ["status", "datapoint"],
            id="duplicate-datapoint-candidates",
        ),
        pytest.param(
            VALID.replace("datapoint: Status\n", "datapoint: [Hinweis, Hin weis]\n"),
            ["status", "datapoint"],
            id="datapoint-candidate-with-whitespace",
        ),
        pytest.param(
            VALID.split("  environment:\n")[0] + "  environment:\n    fields: {}\n",
            ["environment", "fields"],
            id="subject-without-fields",
        ),
        pytest.param(
            "subjects: {}\n",
            ["subjects"],
            id="no-subjects",
        ),
        pytest.param(
            VALID.replace("  environment:\n", "  environment.raw:\n"),
            ["environment.raw"],
            id="subject-suffix-not-a-single-token",
        ),
    ],
)
def test_rejects_drift_and_names_the_offender(tmp_path: Path, text: str, names: list[str]) -> None:
    path = write_descriptor(tmp_path, text)

    with pytest.raises(DescriptorError) as excinfo:
        knx_descriptor.load(path)

    message = str(excinfo.value)
    assert str(path) in message
    for name in names:
        assert name in message


def test_reports_every_error_at_once(tmp_path: Path) -> None:
    text = VALID.replace('dpt: "5.010"', 'dpt: "5.10"').replace("        datapoint: Restzeit\n", "")

    with pytest.raises(DescriptorError) as excinfo:
        knx_descriptor.load(write_descriptor(tmp_path, text))

    message = str(excinfo.value)
    assert "subjects.state.fields.status.dpt" in message
    assert "subjects.state.fields.remaining_minutes: 'datapoint' is a required property" in message


def test_rejects_duplicate_field_key(tmp_path: Path) -> None:
    text = VALID.replace(
        "  environment:\n",
        '      status:\n        datapoint: Status-2\n        dpt: "5.010"\n  environment:\n',
    )

    with pytest.raises(DescriptorError, match="duplicate key 'status'"):
        knx_descriptor.load(write_descriptor(tmp_path, text))


def test_rejects_yaml_1_1_word_as_field_key(tmp_path: Path) -> None:
    text = VALID.replace("      status:\n", "      on:\n")

    with pytest.raises(DescriptorError, match="non-string key True"):
        knx_descriptor.load(write_descriptor(tmp_path, text))


@pytest.mark.parametrize("text", ["", "- a\n- b\n", "just a string\n"])
def test_rejects_non_mapping_document(tmp_path: Path, text: str) -> None:
    with pytest.raises(DescriptorError, match="mapping"):
        knx_descriptor.load(write_descriptor(tmp_path, text))


def test_rejects_unparseable_yaml(tmp_path: Path) -> None:
    with pytest.raises(DescriptorError, match="knx.yaml"):
        knx_descriptor.load(write_descriptor(tmp_path, "subjects: [unclosed\n"))
