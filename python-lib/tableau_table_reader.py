"""
Wrapper of the Tableau Hyper formatter

The formatter (import a Tableau Hyper file as DSS dataset) relies on this class.
"""

import logging
import os
import tempfile

from schema_conversion import SchemaConversion
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import TableName
from tableauhyperapi import HyperException

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def build_query(columns):
    """
    Build the SQL-like query to Tableau Hyper file

    :param: columns : A Tableau Hyper Table Definition Object (Exposed through the following methods)
    >>> table_definition = connection.catalog.get_table_definition(table_name)
    >>> columns = table_definition.columns
    >>> for column in columns:
    >>>     print(column.type, column.name.unescaped)

    :return: the core of the SQL query with columns specifications
    :example:
    >>> "host_id, host_name, neighbourhood, geopoint:: text"
    """
    query_columns = ''
    for column in columns:
        query_columns += column.name.unescaped
        if str(column.type) == 'GEOGRAPHY':
            query_columns += ':: text'
        query_columns += ', '
    return query_columns[:-2]


class TableauTableReader(object):

    def __init__(self, schema_name, table_name):
        """
        Wrapper for the Tableau Hyper formatter

        :param schema_name : name of the schema as stored in the Tableau Hyper file
        :param table_name : name of the table as stored in the Tableau Hyper file
        """

        self.table_name = table_name
        self.schema_name = schema_name

        self.hyper_table = None
        self.hyper_columns = None
        self.hyper_storage_types = None
        self.dss_columns = None
        self.dss_storage_types = None

        self.rows = []
        self.row_index = 0

        self.path_to_hyper = None

        self.hyper = None
        self.connection = None

        self.schema_converter = SchemaConversion()

        # Handle batch querying
        self.offset = 0
        self.limit = 10000
        self.end_read = False

    def create_tmp_hyper(self):
        """
        Create a temporary file to store the streaming buffer
        :return: self.path_to_hyper: path to the temporary file
        """
        self.path_to_hyper = tempfile.NamedTemporaryFile(prefix='output', suffix=".hyper", dir=os.getcwd()).name
        logger.info("Create a temporary hyper file at location: {}".format(self.path_to_hyper))
        return self.path_to_hyper

    def read_buffer(self, stream):
        """
        Read and store the full stream
        :param stream: stream coming from the Tableau Hyper file
        :return:
        """
        line = True
        with open(self.path_to_hyper, "ab") as f:
            while line:
                line = stream.read(1024)
                f.write(line)
        logger.info("Stored the full stream as bytes")

    def open_connection(self):
        """
        Open the connection to the Tableau Hyper file and the database
        """
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.connection = Connection(self.hyper.endpoint, self.path_to_hyper)
        logger.info("Opened the connection to Tableau Hyper file")

    def read_hyper_columns(self):
        """
        Read from the Tableau Hyper file the columns and schema of the table

        :return: self.hyper_storage_types
        """
        logger.info("Trying to read Tableau Hyper table {}.{} ...".format(self.schema_name, self.table_name))
        hyper_table = TableName(self.schema_name, self.table_name)
        self.hyper_table = hyper_table
        try:
            table_def = self.connection.catalog.get_table_definition(hyper_table)
        except HyperException as e:
            logger.warning("The target table does not exists in this hyper file. Requested table: {}.{}"
                           .format(self.table_name, self.schema_name))
            raise Exception("Table does not exist: {}.{}".format(self.schema_name, self.table_name))

        self.hyper_columns = table_def.columns
        self.hyper_storage_types = [column.type.tag for column in self.hyper_columns]

        self.dss_columns = self.schema_converter.hyper_columns_to_dss_columns(self.hyper_columns)
        self.dss_storage_types = [column['type'] for column in self.dss_columns]

        self.schema_converter.set_dss_storage_types(self.dss_storage_types)
        self.schema_converter.set_hyper_storage_types(self.hyper_storage_types)

    def fetch_rows(self, offset, limit):
        """
        Retrieve all the rows from the Tableau Hyper file, convert values on the fly
        """
        sql_hyper_query = f'SELECT {build_query(self.hyper_columns)} FROM {self.hyper_table} OFFSET {offset} LIMIT {limit}'
        result = self.connection.execute_query(sql_hyper_query)
        for row in result:
            self.rows.append(row)

    def close_connection(self):
        """
        Close the connection to the Tableau Hyper File
        """
        self.connection.close()
        self.hyper.close()

    def read_schema(self):
        """
        Access
        """
        logger.info("Send to dss during read_schema: {}".format(self.dss_columns))
        return self.dss_columns

    def read_row(self):
        """
        Read one row from the stored data
        """
        if self.end_read:
            return None
        if len(self.rows) == 0:
            self.fetch_rows(self.offset, self.limit)
            self.offset += self.limit
        if len(self.rows) == 0:
            self.close_connection()
            self.end_read = True
            return None
        else:
            hyper_row = self.rows.pop()
            dss_row = self.schema_converter.prepare_row_to_dss(hyper_row)
            row = {}
            for column, value in zip(self.dss_columns, dss_row):
                row[column["name"]] = value
            self.row_index += 1
            return row