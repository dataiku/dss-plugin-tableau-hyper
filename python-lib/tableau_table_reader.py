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
        query_columns += str(column.name)
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

    def clean_tmp_hyper_files(self):
        """
        :return:
        """
        hyper_to_clean = [x for x in os.listdir(".") if x.startswith("tmp_hyper_file")]
        for file_name in hyper_to_clean:
            try:
                os.remove(file_name)
            except Exception as err:
                logger.warning(err)
                pass

    def create_tmp_hyper_file(self, unique=False):
        """
        Create a temporary file to store the streaming buffer
        :return: self.path_to_hyper: path to the temporary file
        """
        if unique:
            self.path_to_hyper = "tmp_hyper_file.hyper" # default name
            if os.path.exists(self.path_to_hyper):
                logger.info("Existing temporary hyper storage file detected: {}\nWill be destroyed...".format(self.path_to_hyper))
                os.remove(self.path_to_hyper)
        else:
            self.path_to_hyper = tempfile.NamedTemporaryFile(suffix=".hyper", prefix="tmp_hyper_file_", delete=True, dir=".").name
            logger.info("Creating temporary file to store future buffer stream from Hyper: {} ".format(self.path_to_hyper))

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
            if os.path.exists(self.path_to_hyper):
                os.remove(self.path_to_hyper)
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
        logger.warning("SQL query ", sql_hyper_query)
        try:
            result = self.connection.execute_query(sql_hyper_query)
        except Exception as err:
            logger.fatal("Tried to execute query but was unsuccessful.")
            raise err
        for row in result:
            self.rows.append(row)

    def close_connection(self):
        """
        Close the connection to the Tableau Hyper File
        """
        self.connection.close()
        self.hyper.close()
        if os.path.exists(self.path_to_hyper):
            os.remove(self.path_to_hyper)

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
            os.remove(self.path_to_hyper)
            self.end_read = True
            logger.info("Finished reading rows from hyper file...")
            return None
        else:
            hyper_row = self.rows.pop()
            dss_row = self.schema_converter.prepare_row_to_dss(hyper_row)
            row = {}
            for column, value in zip(self.dss_columns, dss_row):
                row[column["name"]] = value
            self.row_index += 1
            return row