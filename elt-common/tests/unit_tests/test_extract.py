import pytest

from elt_common.extract import Watermark


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
        ('{"column": "bool_value", "value": true}', "'value' must be a string or number"),
        ('{"column": "array_value", "value": [1]}', "'value' must be a string or number"),
        ('{"column": "object_value", "value": {"a": 123}}', "'value' must be a string or number"),
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
    ],
)
def test_deserialize_watermark_good_values(serialized, expected):
    assert Watermark.deserialize(serialized) == expected
