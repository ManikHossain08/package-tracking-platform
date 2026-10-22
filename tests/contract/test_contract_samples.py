"""TS-C-01: contract samples behave as documented (spec 02, contracts/README.md)."""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
SAMPLES = CONTRACTS / "samples"


def _validator(schema_file: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS / schema_file).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


EVENT = _validator("package-event.v1.schema.json")
DLQ = _validator("dlq-record.v1.schema.json")
EXPECTED = {
    k: v for k, v in json.loads((SAMPLES / "invalid" / "expected.json").read_text()).items()
    if not k.startswith("_")
}
INVALID_FILES = sorted(p.name for p in (SAMPLES / "invalid").iterdir() if p.name != "expected.json")


@pytest.mark.req("FR-VAL-01", "FR-ING-01")
@pytest.mark.parametrize("path", sorted((SAMPLES / "valid").iterdir()), ids=lambda p: p.name)
def test_valid_samples_pass(path: Path) -> None:
    errors = [e.message for e in EVENT.iter_errors(json.loads(path.read_text()))]
    assert errors == []


@pytest.mark.req("FR-VAL-01")
@pytest.mark.parametrize("name", INVALID_FILES)
def test_invalid_samples_are_rejected(name: str) -> None:
    raw = (SAMPLES / "invalid" / name).read_text()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        assert EXPECTED[name] == "INVALID_JSON"
        return
    assert list(EVENT.iter_errors(payload)), f"{name} unexpectedly passed the schema"


@pytest.mark.req("FR-VAL-01")
def test_every_invalid_sample_has_exactly_one_expected_code() -> None:
    assert sorted(EXPECTED) == INVALID_FILES


@pytest.mark.req("FR-DLQ-01")
def test_expected_codes_are_valid_dlq_error_codes() -> None:
    allowed = set(DLQ.schema["$defs"]["errorCode"]["enum"])
    assert set(EXPECTED.values()) <= allowed


@pytest.mark.req("FR-DLQ-01")
@pytest.mark.parametrize("path", sorted((SAMPLES / "dlq").iterdir()), ids=lambda p: p.name)
def test_dlq_samples_pass(path: Path) -> None:
    errors = [e.message for e in DLQ.iter_errors(json.loads(path.read_text()))]
    assert errors == []


@pytest.mark.req("NFR-04")
def test_v1_accepts_unknown_fields_for_forward_compatibility() -> None:
    event = json.loads((SAMPLES / "valid" / "delivered_v1_1_extra_field.json").read_text())
    assert "proof_of_delivery_type" in event
    assert EVENT.is_valid(event)
