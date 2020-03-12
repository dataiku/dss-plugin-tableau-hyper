"""
    A mock dataset object that creates rows that are in the output types of a DSS schema.
    The dataset object should be as close as possible to the original DSS object. Some types in this dataset
    are not handled. You should check documentation in the official documentation for full coverage.
"""

import datetime
import pdb
import random
import string


class MockDataset:
    """
        Emulator of the DSS dataset object when handled by the exporter class.
        If containing some geodata, should be indicated in order for Tableau to handle it.
    """

    def __init__(self, include_geo=False, rows_number=100):
        """
        Emulator of the DSS object dataset
        :param is_geo:
        """
        self.include_geo = include_geo # The dataset contains at least one geopoint column
        self.rows_number = rows_number
        self.schema = self.generate_schema()
        self.data = self.generate_rows(self.rows_number)

    def generate_schema(self):
        """
        Generate the schema returned by DSS when being handled by the exporter.
        Name of the column are generated randomly.
        :return: output_dss_schema
        """
        types_in_schema = ['date', 'string', 'double', 'bigint'] # ordered types list
        if self.include_geo:
            types_in_schema.append('geopoint')
        output_dss_schema = {'columns':
                                 [{'name': 'sample_' + type_, 'type': type_} for type_ in types_in_schema],
                             'userModified': True}
        return output_dss_schema

    def generate_row(self):
        row = [ self.generate_date(), self.generate_string(), self.generate_float(), self.generate_int()]
        if self.include_geo:
            row.append(self.generate_geopoint())
        return row

    def generate_rows(self, number_rows):
        data = [self.generate_row() for i in range(number_rows)]
        return data

    def generate_string(self, len_string=10):
        """
        string
        :return: A random sequence of string
        """
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_string))

    def generate_float(self):
        """
        double
        :return: A random float number
        """
        return float(random.random())

    def generate_int(self):
        return random.randint(-10000, 10000)

    def generate_date(self):
        timestamp = datetime.datetime.now().timestamp()
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        return dt_object

    def generate_geopoint(self):
        lat = round(-73.98 + random.random()*0.2, 2)
        long = round(40.75 + random.random()*0.2, 2)
        return "POINT({} {})".format(lat, long)

if __name__ == "__main__":

    # Basic usage
    dataset = MockDataset(include_geo=False)
    dataset.generate_row()
    dataset.generate_float()
    for row in dataset.data:
        print(row)