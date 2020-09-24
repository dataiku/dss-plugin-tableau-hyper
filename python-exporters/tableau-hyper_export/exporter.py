import logging

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter
from custom_exceptions import InvalidPluginParameter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
    Exporter to Tableau Hyper file
    """
    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        Exporter.__init__(self, config, plugin_config)

        schema_name = self.config.get("schema_name", "Extract")
        table_name = self.config.get("table_name", "Extract")
        if schema_name == '':
            raise InvalidPluginParameter('schema_name', schema_name)
        if table_name == '':
            raise InvalidPluginParameter('table_name', table_name)

        logger.info("Detected schema_name: {}".format(schema_name))
        logger.info("Detected table_name: {}".format(table_name))

        # Instantiate the Tableau custom writer
        self.writer = TableauTableWriter(schema_name=schema_name, table_name=table_name)

        self.output_file = None

    def open(self, schema):
        # Leave method empty here
        pass

    def open_to_file(self, schema, destination_file_path):
        """
        Initial actions for the opening of the output file.

        :param schema: the column names and types of the data
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        logger.info("Writing the output hyper file at the following location: {}".format(self.output_file))
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

    def write_row(self, row):
        """
        Handle one row of data to export

        :param row: a tuple with N strings matching the schema passed to open
        """
        self.writer.write_row(row)

    def close(self):
        """
        Called when closing the table.
        """
        self.writer.close()
