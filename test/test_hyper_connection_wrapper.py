"""
    Test for the Hyper Connection wrapper.

    This file should be very
"""

import logging
import os

from schema_conversion import SchemaConversionDSSHyper
from hyper_connection_wrapper import write_geo_table
from hyper_connection_wrapper import write_regular_table
from mock_dss_generator import MockDataset
from tableauhyperapi import TableDefinition

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


def test_write_regular_table():
    """
        Test the write of a regular hyper table.
    :return:
    """

    path_to_hyper = './data/test_write_regular_table.hyper'
    if os.path.exists(path_to_hyper):
        os.remove(path_to_hyper)

    dataset = MockDataset(include_geo=False, rows_number=100)

    schema_converter = SchemaConversionDSSHyper(debug=True)

    regular_schema = dataset.schema
    regular_table = TableDefinition('myRegularTable', schema_converter.convert_schema_dss_to_hyper(regular_schema))

    schema_converter.set_dss_type_array(regular_schema)

    row = schema_converter.enforce_hyper_type_on_row(dataset.data[0])
    write_regular_table(path_to_hyper, regular_table, regular_schema, [row])

    return True

def test_write_geo_table():
    """
        Test the write of the geo table.
    :return:
    """

    path_to_hyper = './data/test_write_geo_table.hyper'
    if os.path.exists(path_to_hyper):
        os.remove(path_to_hyper)

    dataset = MockDataset(include_geo=True, rows_number=100)

    schema_converter = SchemaConversionDSSHyper(debug=True)

    geo_schema = dataset.schema
    geo_table = TableDefinition("myGeoTable", schema_converter.convert_schema_dss_to_hyper(geo_schema))

    regular_schema = schema_converter.dss_schema_geo_to_text(geo_schema)
    regular_table = TableDefinition("myRegularTable", schema_converter.convert_schema_dss_to_hyper(regular_schema))

    schema_converter.set_dss_type_array(geo_schema)

    row = schema_converter.enforce_hyper_type_on_row(dataset.data[0])
    write_geo_table(path_to_hyper, regular_table, geo_table, "mySchema", [row])

    return True

if __name__ == "__main__":
    print(test_write_regular_table())
    print(test_write_geo_table())