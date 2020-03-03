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

        # Batch upload and write in Hyper
        self.row_index = 0
        self.data = []
        self.batch_size = 2000

        # Initialised empty values
        self.schema = None
        self.output_file = None
        self.is_geo_table = None
        self.hyper_table = None
        self.regular_hyper_table = None

        # Handle parameters
        if 'table_name' not in self.config:
            self.config['table_name'] = 'my_dss_table'
        if 'schema_name' not in self.config:
            self.config['schema_name'] = 'my_dss_schema'

        # Class components
        self.schema_converter = SchemaConversionDSSHyper()

    def open(self, schema):
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exportsers with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        logger.info("Schema of output file:\n {}".format(schema))
        self.schema = schema
        self.schema_converter.set_dss_type_array(self.schema)
        self.output_file = destination_file_path
        self.is_geo_table = self.schema_converter.is_geo(self.schema)

        if self.is_geo_table:
            logger.info("Detected geotable through the schema.")
        else:
            logger.info("Detected regular table through the schema.")

        logger.info("Creating Hyper Table with name: {}".format(self.config['table_name']))
        self.hyper_table = TableDefinition(TableName('schema', self.config['table_name']), self.schema_converter.convert_schema_dss_to_hyper(self.schema))
        if self.is_geo_table:
            dss_regular_schema = self.schema_converter.dss_schema_geo_to_text(self.schema)
            self.regular_hyper_table = TableDefinition(TableName('schema', self.config['table_name']+"_geo"), self.schema_converter.convert_schema_dss_to_hyper(dss_regular_schema))

        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                connection.catalog.create_schema('schema')

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        # Process the row
        typed_row = self.schema_converter.enforce_hyper_type_on_row(row)
        self.data.append(typed_row)
        self.row_index += 1
        if self.row_index % self.batch_size == 0:
            if self.is_geo_table:
                write_geo_table(self.output_file, self.regular_hyper_table, self.hyper_table, self.schema, self.data)
            else:
                write_regular_table(self.output_file, self.hyper_table, self.schema, self.data)
            self.data = []
        return True

    def close(self):
        """
            Called when closing the table.
        """
        if self.data:
            if self.is_geo_table:
                write_geo_table(self.output_file, self.regular_hyper_table, self.hyper_table, self.schema, self.data)
            else:
                write_regular_table(self.output_file, self.hyper_table, self.schema, self.data)
            self.data = []
        return True