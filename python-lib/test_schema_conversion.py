import pytest

from schema_conversion import SchemaConversion
from mock_dss_generator import MockDataset
import pdb


def test_prepare_row_to_dss():
    dataset = MockDataset(include_geo=True, rows_number=1)
    dss_schema = dataset.generate_schema()
    dss_row = dataset.generate_row()
    print(dss_row)
    dss_storage_types = [column["type"] for column in dss_schema["columns"]]
    schema_converter = SchemaConversion()
    schema_converter.set_dss_storage_types(dss_storage_types)
    print(schema_converter.dss_storage_types)
    output_row = schema_converter.prepare_row_to_hyper(dss_row)
    print(output_row)
    return True


if __name__ == "__main__":
    test_prepare_row_to_dss()