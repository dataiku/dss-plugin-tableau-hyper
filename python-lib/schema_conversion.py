"""
Helper functions and class definition for conversion between schema in DSS and Tableau Hyper

"""

from tableauhyperapi import TableDefinition
from type_conversion import TypeConversion

import copy
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def dss_is_geo(dss_schema):
    """
    Check if the input dataset contains a geopoint (DSS storage type) column
    If so, specific processing will be applied later on

    :param dss_schema: schema of a dss dataset
    >>> dss_schema = {"columns": [{"name": "customer_id", "type": "bigint"}]}
    :return Boolean{input dataset contains at least one geopoint column}

    """
    for column in dss_schema['columns']:
        if column['type'] == 'geopoint':
            return True
    return False


def geo_to_text(dss_schema):
    """
    Transform the input dss schema `dss_schema` in order to set the `geopoint` storage type column to `string`.

    This function is used when writing geo-data in a hyper file. A temporary table must be created with
    no geopoint column in it.

    :param dss_schema: The schema of a dss dataset
    :example: {"columns": [{"name": "customer_id", "type": "bigint"}, ...]}, ...]}
    >>> dss_schema = {"columns": [{"name": "customer_id", "type": "bigint"}]}
    :return: regular_schema : The schema after the change of type of the `geopoint` type to `string`
    """
    regular_schema = copy.deepcopy(dss_schema)
    for typed_column in regular_schema['columns']:
        name, dss_type = typed_column['name'], typed_column['type']
        if dss_type == 'geopoint':
            typed_column['type'] = 'string'
    return regular_schema


class SchemaConversion:

    def __init__(self):
        """
        Class handling the conversion of the schema between Tableau Hyper and DSS
        """
        self.type_converter = TypeConversion()
        self.dss_storage_types = []
        self.hyper_storage_types = []

    def dss_columns_to_hyper_columns(self, dss_columns):
        """
        Convert the columns of the DSS dataset to Tableau Hyper columns

        :param dss_columns: columns of the DSS dataset
        :example: [{"name": "customer_id", "type": "bigint"}, ...]
        >>> dss_columns = [{"name": "customer_id", "type": "bigint"}]
        :return: hyper_columns: tableau hyper columns object
        """
        hyper_columns = []
        for dss_column in dss_columns:
            dss_column_name, dss_column_type = dss_column['name'], dss_column['type']
            hyper_type = self.type_converter.dss_type_to_hyper(dss_column_type)
            hyper_columns.append(TableDefinition.Column(dss_column_name, hyper_type))
        return hyper_columns

    def hyper_columns_to_dss_columns(self, hyper_columns):
        """
        Convert Tableau Hyper columns to DSS columns

        :param hyper_columns:
        >>> column = hyper_columns[0]
        >>> print(column.type.tag)
        :return: The dss_columns in a schema format, list of dict
        :example [{"name": "customer_name", "type": "string"}, ...]
        """
        dss_columns = []
        for hyper_column in hyper_columns:
            hyper_name, hyper_type_tag = hyper_column.name.unescaped, hyper_column.type.tag
            dss_type = self.type_converter.hyper_type_to_dss(hyper_type_tag)
            dss_columns.append({"name": hyper_name, "type": dss_type})
        return dss_columns

    def set_dss_storage_types(self, dss_storage_types):
        """
        Store DSS storage types

        :param dss_storage_types:
        :example
        >>> ['bigint', 'double']
        """
        self.dss_storage_types = dss_storage_types

    def set_hyper_storage_types(self, hyper_storage_types):
        """
        Store Tableau Hyper storage types

        :param hyper_storage_types: array containing the type tags of the Tableau Hyper columns
        >>> from tableauhyperapi import SqlType
        >>> from tableauhyperapi import TypeTag
        >>> assert SqlType.double().tag == TypeTag.DOUBLE
        """
        self.hyper_storage_types = hyper_storage_types

    def prepare_row_to_dss(self, hyper_row):
        """
        Transform value with respect to specified type in DSS dataset

        :param hyper_row: row of values coming from Tableau Hyper
        :return dss_row: dss compliant row
        """
        dss_row = [self.type_converter.hyper_value_to_dss(value_, type_) for value_, type_ in zip(hyper_row, self.hyper_storage_types)]
        return dss_row

    def prepare_row_to_hyper(self, dss_row):
        """
        Transform value with respect to specified type in Tableau Hyper dataset

        :param dss_row: row of values coming from DSS dataset
        :return hyper_row: tableau hyper compliant row
        """
        hyper_row = [self.type_converter.dss_value_to_hyper(value_, type_) for value_, type_ in zip(dss_row, self.dss_storage_types)]
        return hyper_row
