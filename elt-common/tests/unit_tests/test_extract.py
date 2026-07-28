import datetime as dt
from pathlib import Path

import pytest

from elt_common.extract import BaseExtract, Watermark, create_extract_obj
from elt_common.sources.sqldatabase import SqlDatabaseExtract
from elt_common.pipeline_types import ELTJobManifest

_value_type_error_msg = "'value' must be a string, number, or ISO format datetime"


@pytest.mark.parametrize(
    "serialized,expected_error_message",
    [
        ("", "Expecting value"),
        ("{}", "'column' was missing"),
        ('{"column": "no_value"}', "'value' was missing"),
        ('{"value": "no_column"}', "'column' was missing"),
        ("{'column': 'single_quotes', 'value': 123}", "double quotes"),
        ('{"column": 123, "value": "non_string_column"}', "Watermark 'column' must be a string"),
        ('{"column": "null_value", "value": null}', "'value' was missing"),
        ('{"column": null, "value": "null_column"}', "'column' was missing"),
        ('{"column": "bool_value", "value": true}', _value_type_error_msg),
        ('{"column": "array_value", "value": [1]}', _value_type_error_msg),
        ('{"column": "object_value", "value": {"a": 123}}', _value_type_error_msg),
    ],
)
def test_deserialize_watermark_bad_values_value_error(serialized, expected_error_message):
    with pytest.raises(ValueError, match=expected_error_message):
        Watermark.deserialize(serialized)


@pytest.mark.parametrize(
    "serialized,expected",
    [
        ('{"column": "string_value", "value": "123"}', Watermark("string_value", "123")),
        ('{"column": "int_value", "value": 123}', Watermark("int_value", 123)),
        ('{"column": "float_value", "value": 123.123}', Watermark("float_value", 123.123)),
        (
            '{"column": "datetime_value", "value": "2020-01-01T00:00:00"}',
            Watermark("datetime_value", dt.datetime(2020, 1, 1)),
        ),
    ],
)
def test_deserialize_watermark_good_values(serialized, expected):
    assert Watermark.deserialize(serialized) == expected


def make_error_manifest(filename):
    this_dir = Path(__file__).parent
    return ELTJobManifest(
        warehouse_name="warehouse",
        name=filename,
        domain="whatever",
        is_ingest_job=True,
        ingest_job_dir=this_dir / "create_extract_obj_fakes" / "errors",
    )


@pytest.mark.parametrize(
    "filename,expected_error,expected_error_message",
    [
        ("doesnt_exist", RuntimeError, "No extraction class definition file"),
        ("not_python", RuntimeError, "No extraction class definition file"),
        ("blank", AttributeError, "doesn't include an 'Extract' class"),
        ("non_class", TypeError, "'non_class' is not a class"),
        ("non_sub_class", TypeError, "'non_sub_class' doesn't subclass"),
        ("doesnt_implement_method", TypeError, "Can't instantiate abstract class"),
    ],
)
def test_create_extract_obj_errors(filename, expected_error, expected_error_message):
    job = make_error_manifest(filename)
    with pytest.raises(expected_error, match=expected_error_message):
        create_extract_obj(job)


def make_manifest(filename):
    this_dir = Path(__file__).parent
    return ELTJobManifest(
        warehouse_name="test_warehouse",
        name=filename,
        domain="whatever",
        is_ingest_job=True,
        ingest_job_dir=this_dir / "create_extract_obj_fakes",
    )


def test_create_extract_obj():
    job = make_manifest("three_empty_tables")

    extract_obj = create_extract_obj(job)

    assert isinstance(extract_obj, BaseExtract)
    for _, props in extract_obj.extract_resource_properties():
        yielded = [d for d in props.extractor(None)]
        assert len(yielded) == 1
        assert yielded[0] == []


def test_create_extract_obj_custom_config(monkeypatch):
    job = make_manifest("custom_config")
    monkeypatch.setenv("CUSTOM_CONFIG__REQUIRED_STR", "required")

    extract_obj = create_extract_obj(job)

    assert isinstance(extract_obj, BaseExtract)
    assert getattr(extract_obj.config, "required_str") == "required"


def test_create_extract_obj_sql_extract(monkeypatch):
    job = make_manifest("sql_extract")
    monkeypatch.setenv("SQL_EXTRACT__DRIVERNAME", "sqlite")
    monkeypatch.setenv("SQL_EXTRACT__DATABASE", "not_real")
    monkeypatch.setenv("SQL_EXTRACT__CHUNK_SIZE", "100")

    extract_obj = create_extract_obj(job)

    assert isinstance(extract_obj, BaseExtract)
    assert isinstance(extract_obj, SqlDatabaseExtract)
    assert extract_obj._chunk_size == 100
