"""
A custom Python format is a subclass of Formatter, with the logic split into
OutputFormatter for outputting to a format, and FormatExtractor for reading
from a format

The parameters it expects are specified in the format.json file.

Note: the name of the class itself is not relevant.

"""

from typing import List

from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor

import base64
import datetime
import json
import logging
import os
import pandas
import tempfile

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class MyFormatter(Formatter):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        print("Plugin init config: {}".format(config))
        Formatter.__init__(self, config, plugin_config)

    def get_output_formatter(self, stream, schema):
        """
        Return a OutputFormatter for this format
        :param stream: the stream to write the formatted data to
        :param schema: the schema of the rows that will be formatted (never None)
        """
        return MyOutputFormatter(stream, schema)

    def get_format_extractor(self, stream, schema=None):
        """
        Return a FormatExtractor for this format
        :param stream: the stream to read the formatted data from
        :param schema: the schema of the rows that will be extracted. None when the extractor is used to detect the format.
        """
        print("Using the format extractor with schema: {}".format(schema))
        print("Plugin config MyFormatter: {}".format(self.config))
        return MyFormatExtractor(stream, schema, self.config.get("table_name", ""), self.config.get("schema_name", ""))


class MyOutputFormatter(OutputFormatter):
    """
    Writes a stream of rows to a stream in a format. The calls will be:

    * write_header()
    * write_row(row_1)
      ...
    * write_row(row_N)
    * write_footer()

    """

    def __init__(self, stream, schema):
        """
        Initialize the formatter
        :param stream: the stream to write the formatted data to
        """
        OutputFormatter.__init__(self, stream)
        self.schema = schema

    def write_header(self):
        """
        Write the header of the format (if any)
        """
        if self.schema is not None:
            self.stream.write(' ' + base64.b64encode(json.dumps(self.schema['columns'])) + '\n')

    def write_row(self, row):
        """
        Write a row in the format
        :param row: array of strings, with one value per column in the schema
        """
        clean_row = []
        for x in row:
            if isinstance(x, datetime.datetime):
                clean_row.append(x.isoformat())
            elif isinstance(x, pandas.Timestamp):
                clean_row.append(x.isoformat())
            else:
                clean_row.append(x)
        self.stream.write(base64.b64encode(json.dumps(clean_row)) + '\n')

    def write_footer(self):
        """
        Write the footer of the format (if any)
        """
        pass


class MyFormatExtractor(FormatExtractor):
    """
        Read the input format in DSS

    TODO: Remove the test path:
    /Users/thibaultdesfontaines/superstore_sample.hyper
    """

    def __init__(self, stream, schema, table_name = None, schema_name = None):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)

        self.columns = []
        self.schema = None
        self.row_index = 0
        self.rows = []

        self.hyper_table = None

        self.table_name = table_name
        self.schema_name = schema_name

        self.path_to_hyper = tempfile.NamedTemporaryFile(prefix='output', suffix=".hyper", dir=os.getcwd()).name
        print("Creating a temporary hyper file for temporary buffer storage")
        print("Name of the file: {}".format(self.path_to_hyper))

        print("Input table name: {}".format(self.table_name))
        print("Input schema name: {}".format(self.schema_name))

        # Store the lines from the stream in the temp hyper file
        lines = self.stream.readlines()
        for line in lines:
            with open(self.path_to_hyper, "ab") as f:
                f.write(line)

        print("Plugin execution path: {}".format(os.getcwd()))

        hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        connection = Connection(hyper.endpoint, self.path_to_hyper)

        tables_in_hyper = []
        schema_names = connection.catalog.get_schema_names()
        detected_target_table = False
        for schema_name_ in schema_names:
            for table_name_ in connection.catalog.get_table_names(schema_name_):
                if table_name_.name.unescaped == self.table_name and table_name_.schema_name.name.unescaped == self.schema_name:
                    detected_target_table = True
                tables_in_hyper.append(table_name_)

        print("Detected following tables previously written in Hyper file: {}".format(tables_in_hyper))
        print("The input target table is in the hyper file: {}".format(detected_target_table))

        self.hyper_table = TableName(self.schema_name, self.table_name)

        var = connection.catalog.get_table_definition(self.hyper_table)
        columns: List[str] = [column.name.unescaped for column in var.columns]
        self.columns = [{'name': name, 'type': 'string'} for name in columns]
        print("Read schema format: {}".format(self.columns))
        logger.info("Read schema format: {}".format(self.columns))

        result = connection.execute_query(f'SELECT * FROM {self.hyper_table}')
        for row in result:
            self.rows.append(row)

        connection.close()
        hyper.close()

    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return self.columns

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        if self.row_index == len(self.rows):
            return None
        line = self.rows[self.row_index]
        row = {}
        for column, value in zip(self.columns, line):
            row[column['name']] = value
        self.row_index += 1
        return row
