"""
    Schema Conversion:

    Main class and helpers functions for the conversion from a dss schema to a hyper schema
    and the other way around.

    Concepts:
        dss_columns
        hyper_columns
        dss_storage_types
        hyper_storage_types
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
    Return True if the schema `dss_schema` of the input dss dataset contains a geopoint column.

    :param dss_schema: The schema of a dss dataset
        :example: [{"columns": [{"name": "customer_id", "type": "bigint"}, ...]}, ...]

    :return: contains_geo
    """
    contains_geo = False
    for column in dss_schema['columns']:
        if column['type'] == 'geopoint':
            contains_geo = True
            return contains_geo
    return contains_geo


def geo_to_text(dss_schema):
    """
    Transform the input dss schema `dss_schema` in order to set the `geopoint` storage type column to `string`.

    This function is used when writing geo-data in a hyper file. A temporary table must be created with
    no geopoint in it.

    :param dss_schema: The schema of a dss dataset
        :example: [{"columns": [{"name": "customer_id", "type": "bigint"}, ...]}, ...]

    :return: regular_schema : The schema after the change of type of the `geopoint` type to `string`
    """
    regular_schema = copy.deepcopy(dss_schema)
    for typed_column in regular_schema['columns']:
        name, dss_type = typed_column['name'], typed_column['type']
        if dss_type == 'geopoint':
            typed_column['type'] = 'string'
    return regular_schema


class SchemaConversion:
    """
        Main class for conversion of the schema from DSS to Tableau Hyper. (Both ways are supported)
        This class exhibits some basic conversion and interaction with schemas, in dss or tableau.
    """

    def __init__(self):
        # handle the conversion between the types of dss and tableau hyper
        self.type_converter = TypeConversion()
        # store the ordered storage types of the dss dataset
        self.dss_storage_types = []
        # store the ordered storage types (tags) of the hyper dataset
        self.hyper_storage_types = []

    def dss_columns_to_hyper_columns(self, dss_columns):
        """
        Convert the columns of the dss dataset to hyper columns

        :param dss_columns: The columns the dss dataset
            :example: [{"name": "customer_id", "type": "bigint"}, ...]

        :return: hyper_columns: The hyper columns of the Hyper Table
        """
        hyper_columns = []
        for dss_column in dss_columns:
            dss_column_name, dss_column_type = dss_column['name'], dss_column['type']
            hyper_type = self.type_converter.dss_type_to_hyper(dss_column_type)
            hyper_columns.append(TableDefinition.Column(dss_column_name, hyper_type))
        return hyper_columns

    def hyper_columns_to_dss_columns(self, hyper_columns):
        """
        Convert the hyper columns of the Hyper Table to dss columns

        :param hyper_columns:
        Should support the following methods:
            >>> column = hyper_columns[0]
            >>> print(column.type.tag)

        :return: The dss_columns in a schema format, list of dict
            :example [{"name": "customer_name", "type": "string"}, ...]
        """
        dss_columns = []
        for hyper_column in hyper_columns:
            hyper_name, hyper_type_tag = hyper_column.name, hyper_column.type.tag
            dss_type = self.type_converter.hyper_type_to_dss(hyper_type_tag)
            dss_columns.append({"name": hyper_name, "type": hyper_type_tag})
        return dss_columns

    def set_dss_storage_types(self, dss_storage_types):
        """
        Store the storage types in the format the dss dataset

        :param dss_storage_types:
            :example ['bigint', 'double', ...]
        """
        self.dss_storage_types = dss_storage_types

    def set_hyper_storage_types(self, hyper_storage_types):
        """
        Store the storage types in the format of Hyper Table

        :param hyper_storage_types: Array containing the tags of the types in the Hyper file
        >>> from tableauhyperapi import SqlType
        >>> from tableauhyperapi import TypeTag
        >>> assert SqlType.double().tag == TypeTag.DOUBLE

        """
        self.hyper_storage_types = hyper_storage_types

    def prepare_row_to_dss(self, hyper_row):
        """
        Enforce the types of the values for the compliance to the defined types in the DSS dataset

        :param hyper_row:

        :return:
        """
        dss_row = [self.type_converter.hyper_value_to_dss(value_, type_) for value_, type_ in zip(hyper_row, self.hyper_storage_types)]
        return dss_row

    def prepare_row_to_hyper(self, dss_row):
        """
        Convert every values of the dss row for compliance to the Hyper table

        :param dss_row:

        :return:
        """
        hyper_row = [self.type_converter.dss_value_to_hyper(value_, type_) for value_, type_ in zip(dss_row, self.dss_storage_types)]
        return hyper_row
