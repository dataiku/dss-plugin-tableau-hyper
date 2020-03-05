"""
    Contains only the SchemaConversionDSSHyper class for conversion of the schemas
"""

from export_type_conversion import TypeConversionDSSHyper
from tableauhyperapi import TableDefinition

import copy
import logging
import pdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


def is_geo(dss_schema):
    """
        Check if the dss schema contains a geopoint column in input.

    :param dss_schema: A dss schema
        expects the following input format: [{"name": "type"}] and returns the Tableau schema
    :return: the corresponding Tableau Schema
    """
    for typed_column in dss_schema['columns']:
        name, dss_type = typed_column['name'], typed_column['type']
        if dss_type == 'geopoint':
            return True
    return False


def dss_schema_geo_to_text(dss_schema):
    """
        Transform the input schema into a non geotable (regular table) in order to create a temporary table.

    :param dss_schema: The input schema in dss
    :return: The same dss schema as input but with all its geopoint columns as text columnss
    """
    mirror_schema = copy.deepcopy(dss_schema)
    for typed_column in mirror_schema['columns']:
        name, dss_type = typed_column['name'], typed_column['type']
        if dss_type == 'geopoint':
            typed_column['type'] = 'string'
    return mirror_schema


class SchemaConversionDSSHyper:
    """

        Convert the schema of a DSS table into a valid Tableau Schema.
        TODO: Add a DSS schema and a Hyper schema to show the difference

        Test location:
            ./test/test_schema_conversion.py

    """

    def __init__(self):
        self.type_converter = TypeConversionDSSHyper()
        self.dss_types_array = []
        self.table = None

    def convert_schema_dss_to_hyper(self, dss_schema):
        """
            Convert the input dss schema to a hyper file TableDefinition object.
            Handle the conversion between columns types or storage types between DSS and Tableau.

        :param dss_schema: The dss schema
        :return: The hyper table TableDefinition object
        """
        hyper_columns = []
        for typed_column in dss_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            hyper_type = self.type_converter.translate(dss_type)
            hyper_columns.append(TableDefinition.Column(name, hyper_type))
        return hyper_columns

    def set_dss_type_array(self, dss_schema):
        """
            Store a view of the columns types for later types conversions on row values

        :param dss_schema:
        :return: Does not return a value, only a setter
        """
        self.dss_types_array = []
        for typed_column in dss_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            self.dss_types_array.append(dss_type)

    def enforce_hyper_type_on_row(self, row):
        """
            Do the types conversion on the values of the row.

        :param row: The dss row (Should be an iterable)
        :return:
        """
        # Convert the values of the dataset row
        output_row = [self.type_converter.set_type(value, dss_type) for value, dss_type in zip(row, self.dss_types_array)]
        return output_row


if __name__ == "__main__":
    # Show the use of the SchemaConversionDSSHyper class
    schema_converter = SchemaConversionDSSHyper()
    dss_schema = {'columns': [
                    {'name': 'order_date', 'type': 'string'},
                    {'name': 'pages_visited', 'type': 'bigint'},
                    {'name': 'tshirt_quantity', 'type': 'unknown_format'}],
        'userModified': True}
    hyper_columns = schema_converter.convert_schema_dss_to_hyper(dss_schema)
    for column in hyper_columns:
        print(str(column.type))
    pdb.set_trace()