from export_type_conversion import TypeConversionDSSHyper
from tableauhyperapi import TableDefinition

import copy
import logging
import pdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class SchemaConversionDSSHyper:
    """
        Convert the schema of a DSS table into a valid Tableau Schema.
        A DSS schema is of the following format:
                dss_schema = {'columns': [ {'name': 'order_date', 'type': 'string'}, ...], 'userModified': True}
    """

    def __init__(self, debug=False):
        self.type_converter = TypeConversionDSSHyper()
        self.dss_types_array = []
        self.table = None
        self.debug = debug

    def is_geo(self, dss_schema):
        """
        Check if the dss schema includes a geo column.
        :param dss_schema:
        :return:
        """
        for typed_column in dss_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            if dss_type == 'geopoint':
                return True
        return False

    def dss_schema_geo_to_text(self, dss_schema):
        """
        Due to the specific load when handling a geo column, it is useful to be able to
        create an intermediary dss schema with all the geopoint columns converted to text.
        :param dss_schema:
        :return: a dss schema with geopoint columns as text columns
        """
        mirror_schema = copy.deepcopy(dss_schema)
        for typed_column in mirror_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            if dss_type == 'geopoint':
                typed_column['type'] = 'string'
        return mirror_schema

    def convert_schema_dss_to_hyper(self, dss_schema):
        """
        Conversion from the DSS Schema to the Hyper Schema. Both are data oriented table with
        specific predefinition of the columns and their types.
        :param dss_schema:
        :return: A schema into the Hyper Format
        """
        hyper_schema = []
        for typed_column in dss_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            hyper_type = self.type_converter.translate(dss_type)
            hyper_schema.append(TableDefinition.Column(name, hyper_type))
        return hyper_schema

    def set_dss_type_array(self, dss_schema):
        self.dss_types_array = []
        for typed_column in dss_schema['columns']:
            name, dss_type = typed_column['name'], typed_column['type']
            self.dss_types_array.append(dss_type) # Store a view of the types for later row conversion

    def enforce_hyper_type_on_row(self, row):
        """
            Cast the values on a single row.
        :param row:
        :return:
        """
        output_row = [self.type_converter.set_type(value, dss_type) for value, dss_type in zip(row, self.dss_types_array)]
        if self.debug:
            logger.info("Enforce types on row:\n {} \n With dss types:\n  {} \n Output: \n {} \n".format(row, self.dss_types_array, output_row))
        return output_row

if __name__ == "__main__":
    schema_converter = SchemaConversionDSSHyper()
    dss_schema = {'columns': [
                    {'name': 'order_date', 'type': 'string'}, {'name': 'pages_visited', 'type': 'int'},
                    {'name': 'order_id', 'type': 'string'}, {'name': 'customer_id', 'type': 'string'},
                    {'name': 'tshirt_category', 'type': 'string'}, {'name': 'tshirt_price', 'type': 'float'},
                    {'name': 'tshirt_quantity', 'type': 'int'}],
               'userModified': True}
    hyper_schema = schema_converter.convert_schema_dss_to_hyper(dss_schema)