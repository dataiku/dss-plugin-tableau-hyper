from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper

import joblib
import logging
import tempfile
import os

import tableauserverclient as TSC

from export_schema_conversion import SchemaConversionDSSHyper

from hyper_connection_wrapper import write_regular_table
from hyper_connection_wrapper import write_geo_table
from mock_dss_generator import MockDataset

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

from tableau_table_writer import TableauTableWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
        DSS exporter class.
        Export a DSS dataset to a Hyper file.
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        self.writer = TableauTableWriter()

        if 'table_name' not in self.config:
            self.config['table_name'] = 'my_dss_table'
        if 'schema_name' not in self.config:
            self.config['schema_name'] = 'my_dss_schema'

    def open(self, schema):
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exportsers with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_type_array(schema)
        self.writer.create_schema(schema)

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        self.writer.write_row(row)
        self.writer.row_index += 1
        return True

    def close(self):
        """
            Called when closing the table.
        """
        self.writer.close()
        return True