"""
TODO: Remove
1. Create mock dataset and load true datasets and schemas in Tableau Hyper
2. Test the schema conversion and the type enforcement in both ways
3. Validate on the
"""


from tableauhyperapi import TableDefinition
from type_conversion import TypeConversion

import copy
import logging
import pdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


def dss_is_geo(dss_schema):
    """
        Check if the input dss_schema contains a geopoint column in the input.

    :param dss_schema: [{"name": foo, "type": bar}, ...]
    :return: bool{The dss_schema contains a geopoint column}
    """
    if "columns" not in dss_schema:
        logger.warning("Input object has not the expected format")
        raise TypeError
    for column in dss_schema['columns']:
        if column['type'] == 'geopoint':
            return True
    return False


def geo_to_text(dss_schema):
    """
        Transform the input dss schema into a non geotable (regular table) in order to create a temporary table.

    :param dss_schema: The input schema in dss
    :return: The same dss schema as input but with all its geopoint columns as text columnss
    """
    mirror_schema = copy.deepcopy(dss_schema)
    for typed_column in mirror_schema['columns']:
        name, dss_type = typed_column['name'], typed_column['type']
        if dss_type == 'geopoint':
            typed_column['type'] = 'string'
    return mirror_schema


class SchemaConversion:
    """

        Convert the schema of a DSS table into a valid Tableau Schema.
        TODO: Add a DSS schema and a Hyper schema to show the difference
    """

    def __init__(self):
        self.type_converter = TypeConversion()
        # will store the schema types of the future dataset (dss or tableau)
        self.dss_storage_types = []
        self.hyper_storage_types = []

    def dss_schema_to_hyper_table(self, dss_schema):
        """
            Return the schema of the hyper files under the form of the hyper columns.
            Return the sequence of Tableau Column.

        :param dss_schema:
        :return:
        """
        hyper_columns = []
        for dss_column in dss_schema['columns']:
            column_name, column_type = dss_column['name'], dss_column['type']
            hyper_type = self.type_converter.dss_type_to_hyper(column_type)
            hyper_columns.append(TableDefinition.Column(column_name, hyper_type))
        return hyper_columns

    def hyper_table_to_dss_schema(self, hyper_table):
        """
            Convert the hyper table to the dss schema.
            TODO: Check with isolated working examples

        :param hyper_table: Should come from connection.catalog.get_table_definition(table_name)
        :return:
        """
        dss_columns = []
        for column_ in hyper_table.columns:
            dss_columns.append({"name": column_.name, "type": column_.type})
        return dss_columns

    def set_dss_storage_types(self, dss_storage_types):
        """
            Expect a list of types identifiers.
        :param dss_storage_types:
        :return:
        """
        self.dss_storage_types = dss_storage_types

    def set_hyper_storage_types(self, hyper_storage_types):
        self.hyper_storage_types = hyper_storage_types

    def prepare_row_to_dss(self, hyper_row):
        # TODO: Check conventions, should we have
        if not self.hyper_storage_types:
            logger.warning("Premature type casting on the input hyper row, set the types to cast to before.")
            return False
        dss_row = [self.type_converter.hyper_value_to_dss(value_, type_) for
                            value_, type_ in zip(hyper_row, self.hyper_storage_types)]
        return dss_row

    def prepare_row_to_hyper(self, dss_row):
        if not self.dss_storage_types:
            logger.warning("Premature type casting on the input dss row, set the types to cast to before.")
            return False
        hyper_row = [self.type_converter.dss_value_to_hyper(value_, type_) for value_, type_ in zip(dss_row, self.dss_storage_types)]
        return hyper_row
