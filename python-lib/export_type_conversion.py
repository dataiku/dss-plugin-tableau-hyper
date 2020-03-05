"""
    Should contains only the class TypeConversionDSSHyper.

TODO: Write every format on Tableau Hyper File Format (Not necessarily on server)
TODO: Add every format in this code source
TODO: Keep all the test files in this test, in the test if necessary
"""

import logging

from tableauhyperapi import SqlType

from type_casting import enforce_float
from type_casting import enforce_int
from type_casting import enforce_string
from type_casting import enforce_bool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TypeConversionDSSHyper:
    """
        Support the conversion of storage types between DSS and Tableau Hyper.
    """

    def __init__(self):

        self.mapping_dss_to_hyper = {
            'bigint': SqlType.int(),
            'double': SqlType.double(),
            'string': SqlType.text(),
            'date': SqlType.date(),
            'geopoint': SqlType.geography(),
            'bool': SqlType.bool()
        }

        self.mapping_hyper_type_enforcement = {
            'bigint': enforce_int,
            'double': enforce_float,
            'string': enforce_string,
            'bool': enforce_bool,
            'date': self.enforce_date,
            'geopoint': self.enforce_geography
        }

    def translate(self, dss_type):
        """

            Map the types between DSS and Tableau Hyper

        :param dss_type:
        :return:
        """
        if dss_type not in self.mapping_dss_to_hyper:
            logger.info("The following type is not supported in the conversion: {}".format(dss_type))
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
        if value is not None:
            to_hyper_value = str(value).lower()
            return to_hyper_value
        else:
            return None


if __name__ == "__main__":
    type_converter = TypeConversionDSSHyper()
    print(type_converter.enforce_geography("POINT(90 -49)"))
    print(type_converter.enforce_geography("unexpected"))
