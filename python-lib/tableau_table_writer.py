"""
Wrapper of the Tableau Hyper exporter

The exporters (to file and to server) as Tableau Hyper files rely on this class.
"""

import logging
import os

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

from schema_conversion import dss_is_geo
from schema_conversion import geo_to_text
from schema_conversion import SchemaConversion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


class TableauTableWriter(object):
    """
    Wrapper class for writing a Tableau Hyper file from a DSS dataset.
    """

    def __init__(self, schema_name, table_name):
        """
        :param schema_name: name of the target schema
        :param table_name: name of the target table
        """
        self.row_index = 0
        self.data = []
        self.batch_size = 2000

        self.schema_name = schema_name
        self.table_name = table_name

        self.output_file = None
        self.is_geo_table = False

        self.schema_converter = SchemaConversion()

        # Tableau Hyper related objects
        self.hyper = None
        self.connection = None
        self.tmp_table_definition = None
        self.output_table_definition = None
        self.tmp_table_inserter = None
        self.output_table_inserter = None

    def create_schema(self, schema_dss, destination_file_path):
        """
        Read the Tableau Hyper file an.

        :param schema_dss: DSS schema from the DSS dataset to export
            example: [{"columns": [{"name": "customer_id", "type": "bigint"}, ...]}, ...]

        :param destination_file_path:
        :return:
        """
        # Read the destination file of the dss
        self.output_file = destination_file_path
        logger.info("Writing the Tableau Hyper file to the following location: {}".format(destination_file_path))
        logger.info("The dataset to export has the following schema: {}".format(schema_dss))

        dss_columns = schema_dss['columns']
        dss_storage_types = [column_descriptor['type'] for column_descriptor in dss_columns]
        self.schema_converter.set_dss_storage_types(dss_storage_types)

        self.is_geo_table = dss_is_geo(schema_dss)
        logger.info("The input dataset contains a geo column: {}".format(self.is_geo_table))

        if not self.schema_name or not self.table_name:
            logger.warning("Did not received the table or schema name.")
            raise ValueError("No valid schema or table name received.")

        logger.info("Received target schema {} and table {}".format(self.schema_name, self.table_name))

        # Create the Tableau Hyper schema from the DSS schema
        self.output_table_definition = TableDefinition(
                        TableName(self.schema_name, self.table_name),
                        self.schema_converter.dss_columns_to_hyper_columns(dss_columns)
        )

        # Open connection to file
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.connection = Connection(self.hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE)
        assert self.connection is not None
        self.connection.catalog.create_schema(self.schema_name)
        self.connection.catalog.create_table(self.output_table_definition)

        # Handle the geo case
        if self.is_geo_table:
            logger.info("Detected geo column. Creating a temporary table...")
            dss_tmp_schema = geo_to_text(schema_dss)
            dss_tmp_columns = dss_tmp_schema['columns']
            self.tmp_table_definition = TableDefinition(
                        TableName(self.schema_name, "tmp_"+self.table_name),
                        self.schema_converter.dss_columns_to_hyper_columns(dss_tmp_columns))
            self.connection.catalog.create_table(self.tmp_table_definition)
            logger.info("Created temporary table")

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open method
        """
        try:
            hyper_compliant_row = self.schema_converter.prepare_row_to_hyper(row)
            self.data.append(hyper_compliant_row)
            self.row_index += 1

            if self.row_index % self.batch_size == 0:
                logger.info("Writing {} lines to hyper file".format(len(self.data)))
                self.update_table() # send data to hyper file, flush buffer
                self.data = []
        except Exception as err:
            logger.warning("Failed to perform writing on following row:\n{}".format(row))
            raise err
        return True

    def update_table(self):
        """
        Perform an update of the Tableau Hyper file with new data
        """
        # if there is a geo table, create an intermediate and temporary table
        if self.is_geo_table:

            self.tmp_table_inserter = Inserter(self.connection, self.tmp_table_definition)
            self.tmp_table_inserter.add_rows(self.data)
            self.tmp_table_inserter.execute()
            self.tmp_table_inserter.close()

            self.connection.execute_command(
                command=f"INSERT INTO {self.output_table_definition.table_name} SELECT * FROM {self.tmp_table_definition.table_name};")
            self.connection.execute_command(command=f"TRUNCATE TABLE {self.tmp_table_definition.table_name};")

        else:

            if self.connection is None:
                logger.warning("Connection to Tableau Hyper file is undefined")
            self.output_table_inserter = Inserter(self.connection, self.output_table_definition)
            self.output_table_inserter.add_rows(self.data)
            self.output_table_inserter.execute()
            self.output_table_inserter.close()

        return True

    def close(self):
        """
        Release the Tableau Hyper connections
        """
        logger.info("Closing export ...")
        if self.data:
            logger.info("Performing final data update...")
            self.update_table()
            self.data = []
        logger.info("Closing Tableau Hyper connections...")
        if self.is_geo_table:
            self.connection.execute_command(command=f"DROP TABLE {self.tmp_table_definition.table_name};")
        self.hyper.close()
        self.connection.close()
        logger.info("Closed export")
        return True
