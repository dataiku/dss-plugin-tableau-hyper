"""
TODO: Write every format on Tableau Hyper File Format (Not necessarily on server)
TODO: Add every format in this code source
TODO: Keep all the test files in this test, in the test if necessary
"""

import numpy
import pdb
import logging

from tableauhyperapi import SqlType
from tableauhyperapi import TableDefinition

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

class TypeConversionDSSHyper:
    """
        The enforcer functions should enforce the right format of the data before being
        sent to the Tableau Exporter. After that, the data is ready to be sent to the Tableau Hyper.
        TODO: Add the input type field from DSS
        A sample line from DSS dataset:

        ('Midtown', 40.75362, -73.98376999999999, 'Entire home/apt', 225.0, 45.0, '2019-05-21', Timestamp('2019-05-21 00:00:00'), 0.38, 'POINT(-73.98377 40.75362)')

        Timestamp object is type:

        timestamp = datetime.datetime.now().timestamp()
        dt_object = datetime.datetime.fromtimestamp(timestamp)
    """

    def __init__(self):

        # TODO: Check every DSS type is contained once and only once (bijective mapping)
        self.mapping_dss_to_hyper = {
            'bigint': SqlType.int(),
            'double': SqlType.double(),
            'string': SqlType.text(),
            'date': SqlType.date(),
            'geopoint': SqlType.geography(),
            'bool': SqlType.bool()
        }

        self.mapping_hyper_type_enforcement = {
            'bigint': self.enforce_int,
            'double': self.enforce_double,
            'string': self.enforce_string,
            'date': self.enforce_date,
            'geopoint': self.enforce_geography,
            'bool': self.enforce_bool
        }

    def translate(self, dss_type):
        """
            Apply the straight mapping between types in DSS and Hyper.

        :param dss_type:
        :return:
        """
        if dss_type not in self.mapping_dss_to_hyper:
            # TODO: Log the type non soutenu
            return SqlType.text()
        else:
            return self.mapping_dss_to_hyper[dss_type]

    def set_type(self, value, dss_type):
        """
            Convert the value out of the streaming exporter to acceptable Hyper value wrt type.
            TODO: Check that the output value is ready to consume by Tableau Hyper

        :param value:
        :param dss_type:
        :return:
        """
        if dss_type not in self.mapping_hyper_type_enforcement:
            return str(value)
        return self.mapping_hyper_type_enforcement[dss_type](value)

    def enforce_int(self, value):
        if value is not None:
            return int(value)
        else:
            return None

    def enforce_double(self, value):
        if value is not None:
            return float(value)
        else:
            return None

    def enforce_string(self, value):
        if type(value) is not str:
            logger.info("Invalid format of value. Expected string: {}".format(value))
        if value is not None:
            return str(value)
        else:
            return ""

    def enforce_date(self, value):
        """

        :param value:   <class 'pandas._libs.tslibs.timestamps.Timestamp'>
        :return:        datetime.date(2019, 6, 13)
        """
        if value is not None:
            return value.date()
        else:
            return None

    def enforce_geography(self, value):
        """
        :param value: 'POINT(-73.98377 40.75362) -> point(-73.98377 40.75362)
        :return:
        """
        to_hyper_value = str(value).lower()
        if "point(" not in to_hyper_value:
            logger.info("Wrong geography value encountered before sending to hyper {}.".format(to_hyper_value))
            raise TypeError
        return to_hyper_value

    def enforce_bool(self, value):
        """
        Enforce boolean type
        :param value:
        :return:
        """
        if value is not None:
            return bool(value)
        else:
            return False

if __name__ == "__main__":
    type_converter = TypeConversionDSSHyper()