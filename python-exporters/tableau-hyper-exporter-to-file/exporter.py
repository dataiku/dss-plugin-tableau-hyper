"""
Enables the export of a DSS dataset to Hyper file, file format of Tableau Software.
It will handle conversion between storage types from DSS to Hyper.
"""

import logging

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
    Plugin component (Exporter):
    Export a dataset in DSS to a Hyper file, format of Tableau Software.
    Rely mostly on a wrapper located at ./tableau-hyper-export/python-lib/tableau_table_writer.py
    """

    def __init__(self, config, plugin_config):
        """
        :param config:
        :param plugin_config:
        """
        self.config = config
        self.plugin_config = plugin_config

        self.table_name = self.config.get('table_name')
        if self.table_name is None:
            raise ValueError('The table name parameter is not defined and is mandatory.')

        self.schema_name = self.config.get('schema_name')
        if self.schema_name is None:
            raise ValueError('The schema name parameter is not defined and is mandatory.')

        # Init custom wrapper for writing Tableau hyper file
        self.writer = TableauTableWriter(schema_name=self.schema_name, table_name=self.table_name)

        # To be filled later
        self.output_file = None

    def open(self, schema):
        """
        Leave empty.
        :param schema:
        :return:
        """
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        Called when opening the output export file.

        :param schema: dss dataset schema
        :param destination_file_path: path of the export output file
        """
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

    def write_row(self, row):
        """
        Receive one row from DSS, iterate on the DSS file.

        :param row: <tuple>
        """
        self.writer.write_row(row)
        self.writer.row_index += 1
        return True

    def close(self):
        """
        Called when closing the export file
        """
        self.writer.close()
        return True
