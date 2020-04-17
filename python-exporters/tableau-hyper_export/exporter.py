"""
    Contains only the class TableauHyperExporter.
"""

import logging

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
        Plugin component (Exporter) to export a dataset in dss to a hyper file format. Based on the TableauTableWriter
        wrapper for the read/write to hyper file Tableau APIs.

        Test location:
            - (DSS flow) dku17: Should be tested on different scenarios
            - (Mock execution) local: Can be tested on mock run locally
    """
    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config

        schema_name = self.config.get("schema_name")
        if schema_name is None:
            schema_name = "dss_schema"

        table_name = self.config.get("table_name")
        if table_name is None:
            table_name = "dss_table"

        logger.info("Detected schema_name: {}".format(schema_name))
        logger.info("Detected table_name: {}".format(table_name))

        # Instantiate the Tableau custom writer
        self.writer = TableauTableWriter(schema_name=schema_name, table_name=table_name)

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
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

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
