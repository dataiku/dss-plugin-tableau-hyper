"""
A custom Python format is a subclass of Formatter, with the logic split into
OutputFormatter for outputting to a format, and FormatExtractor for reading
from a format

The parameters it expects are specified in the format.json file.

Note: the name of the class itself is not relevant.

"""

from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor

import base64
import datetime
import json
import logging
import os
import pandas

from tableau_table_reader import TableauTableReader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


class MyFormatter(Formatter):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
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
        table_name = self.config['table_name']
        schema_name = self.config['schema_name']
        return MyFormatExtractor(stream, schema, table_name=table_name, schema_name=schema_name)


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
        for column_value in row:
            if isinstance(column_value, datetime.datetime):
                clean_row.append(column_value.isoformat())
            elif isinstance(column_value, pandas.Timestamp):
                clean_row.append(column_value.isoformat())
            else:
                clean_row.append(column_value)
        self.stream.write(base64.b64encode(json.dumps(clean_row)) + '\n')

    def write_footer(self):
        """
        Write the footer of the format (if any)
        """
        pass


class MyFormatExtractor(FormatExtractor):
    """
    Read the input format
    """

    def __init__(self, stream, schema, table_name=None, schema_name=None):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)
        self.tableau_reader = TableauTableReader(table_name=table_name, schema_name=schema_name)
        self.tableau_reader.create_tmp_hyper_file()
        self.tableau_reader.read_buffer(stream)
        self.tableau_reader.open_connection()
        self.tableau_reader.read_hyper_columns()

    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return self.tableau_reader.read_schema()

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        return self.tableau_reader.read_row()
