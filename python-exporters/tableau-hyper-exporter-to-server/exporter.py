"""
    Contains only the class TableauHyperExporter.
"""

import logging
import tempfile

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauServerExporter(Exporter):
    """
        Plugin component for export to a Tableau Server. Will create a temporary hyper file before sending.
        Derive from the TableauTableWriter.
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        # Instantiate the Tableau custom writer
        self.writer = TableauTableWriter()
        # Retrieve the hyper table configuration
        if 'table_name' not in self.config:
            self.config['table_name'] = 'my_dss_table'
        if 'schema_name' not in self.config:
            self.config['schema_name'] = 'my_dss_schema'
        # TODO: Should we checked the configuration for the table and schema ?
        # TODO: Should check the parameter set of the plugin
        # Will be filled via DSS
        self.output_file = None

    def open(self, schema):
        # Leave method empty here
        return None

    def open_to_file(self, schema, destination_file_path):
        """
            Initial actions for the opening of the output file.

        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        logger.info("Creating a temporary file")
        self.output_file = tempfile.TemporaryFile().name
        logger.info("Created a temporary file in location: {}".format(self.output_file))
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
        # TODO: Make sure that the temporary file is deleted
        return True
