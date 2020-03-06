# This file is the actual code for the custom Python format tttest_tf

# import the base class for the custom format
from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor

import json, base64, pandas, datetime

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
        Formatter.__init__(self, config, plugin_config)  # pass the parameters to the base class

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
        self.columns = [c['name'] for c in schema['columns']] if schema is not None else None
        
    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        first = self.stream.readline()
        print(hekki)
        if len(first) > 0 and first[0] == ' ':
            columns = json.loads(base64.b64decode(first[1:-1]))
            self.columns = [c['name'] for c in columns]
            return columns
        else:
            return None
    
    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        if self.stream.closed:
            return None
        line = self.stream.readline()
        if len(line) == 0:
            return None
        # header line with the schema => skip
        if line[0] == ' ':
            return self.read_row()
        values = json.loads(base64.b64decode(line[:-1]))
        row = {}
        for i in range(0,len(values)):
            name = self.columns[i] if self.columns is not None and i < len(self.columns) else 'col_%i' % i
            row[name] = values[i]
        return row
        