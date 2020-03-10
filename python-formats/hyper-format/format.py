# This file is the actual code for the custom Python format tttest_tf

# import the base class for the custom format
from typing import List

from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor

import json, base64, pandas, datetime
import logging
import os

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

"""
A custom Python format is a subclass of Formatter, with the logic split into
OutputFormatter for outputting to a format, and FormatExtractor for reading
from a format

The parameters it expects are specified in the format.json file.

Note: the name of the class itself is not relevant.
"""


class MyFormatter(Formatter):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        print("Config: {}".format(config))
        logger.info("Config: {}".format(config))
        print("Plugin Config: {}".format(plugin_config))
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
        return MyFormatExtractor(stream, schema)


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
    Reads a stream in a format to a stream of rows
    """

    def __init__(self, stream, schema):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)
        self.columns = []
        self.table_name = None
        self.schema = None
        self.path_to_hyper = ''
        self.row_index = 0
        self.rows = []

        self.path_to_hyper = "./stream_trace.hyper"

        if os.path.exists(self.path_to_hyper):
            os.remove(self.path_to_hyper)

        lines = self.stream.readlines()

        first = lines[0]

        print("Plugin execution path: {}".format(os.getcwd()))
        for line in lines:
            with open(self.path_to_hyper, "ab") as f:
                f.write(line)

        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, self.path_to_hyper) as connection:
                tables = []
                schema_names = connection.catalog.get_schema_names()
                for schema in schema_names:
                    for table in connection.catalog.get_table_names(schema):
                        tables.append(table)

        self.table_name = tables[0]

        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            print("The HyperProcess has started.")
            with Connection(hyper.endpoint, self.path_to_hyper) as connection:
                var = connection.catalog.get_table_definition(self.table_name)
            print("The connection to the Hyper extract file is closed.")
        print("The HyperProcess has shut down.")

        columns: List[str] = [column.name.unescaped for column in var.columns]
        self.columns = [{'name': name, 'type': 'string'} for name in columns]

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

        if not self.rows:
            with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                print("The HyperProcess has started.")
                with Connection(hyper.endpoint, self.path_to_hyper) as connection:
                    with connection.execute_query(f'SELECT * FROM {self.table_name}') as result:
                        for row in result:
                            self.rows.append(row)
                        print("The connection to the Hyper extract file is closed.")
            print("The HyperProcess has shut down.")

            print("Initial loading of columns: {}".format(self.columns))
            print("Number of rows to write: {}".format(len(self.rows)))

        if self.row_index == len(self.rows):
            logger.info("No more rows to read.")
            return None

        line = self.rows[self.row_index]
        logger.info("Reading line: {}".format(line))
        row = {}
        for column, value in zip(self.columns, line):
            row[column['name']] = value
        self.row_index += 1
        logger.info("Inserting the following row: {}".format(row))
        return row
