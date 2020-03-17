"""
TODO: Remove
Assess the class:
1. Locate the definition of the storage types in DSS (See the code source)
2. Create a dataset on dku17 with all the types available (?)
3. Store at least one row of the dataset from the plugin component
4. This row will be used for later tests in the tests files
5. Demo your class, play with the class you created potentially in a python notebook
6. Create tests on the class, use Shift+Command+T when selecting the class
"""

import logging

from tableauhyperapi import SqlType
from tableauhyperapi import TypeTag

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


def to_dss_date(hyper_date):
    """
        Expecting <class 'pandas._libs.tslibs.timestamps.Timestamp'> for hyper_date
    :param hyper_date:
    :return:
    """
    return hyper_date.date()


def to_dss_geometry(hyper_string):
    """
        Convert the geometry in Tableau to geometry in DSS
    :param hyper_string:
    :return:
    """
    return hyper_string.lower()


def to_hyper_date(dss_date):
    return dss_date.date()


def to_hyper_geography(dss_geopoint):
    return dss_geopoint.lower()


class TypeConversion(object):
    """
        Support the conversion of storage types between DSS and Tableau Hyper.
    """

    def __init__(self):
        """
            You can check the available storage types of DSS at:
            ./dip/dss-core/src/main/java/com/dataiku/dip/datasets/Type.java

            Check the available DSS types at:
            https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/sqltype.html
        """

        # TODO: Change to TypeTag as key in the mapping
        self.mapping_dss_to_hyper = {
            'array': None,
            'bigint': (SqlType.int(), int),
            'boolean': (SqlType.bool(), bool),
            'date': (SqlType.date(), to_hyper_date),
            'double': (SqlType.double(), float),
            'float': (SqlType.double(), float),
            'geometry': (SqlType.geography(), to_hyper_geography),
            'geopoint': (SqlType.geography(), to_hyper_geography),
            'int': (SqlType.int(), int),
            'map': None,
            'object': None,
            'smallint': (SqlType.geography(), int),
            'string': (SqlType.text(), str),
        }

        self.mapping_hyper_to_dss = {
            TypeTag.INT: ('bigint', int),
            TypeTag.BIG_INT: ('bigint', int),
            TypeTag.BOOL: ('bool', bool),
            TypeTag.DATE: ('date', to_dss_date),
            TypeTag.DOUBLE: ('double', float),
            TypeTag.GEOGRAPHY: ('geometry', to_dss_geometry),
            TypeTag.TEXT: ('string', str)
        }

    def dss_type_to_hyper(self, dss_type):
        """
            Convert an identifier (string) of a single dss storage type to the mapped hyper type.

        :param dss_type:
        :return:
        """
        if dss_type not in self.mapping_dss_to_hyper:
            logger.warning("Storage type not found for dss conversion to hyper: {}".format(dss_type))
            return SqlType.text()
        else:
            return self.mapping_dss_to_hyper[dss_type][0]

    def hyper_type_to_dss(self, hyper_type):
        """
            Convert an identifier (string) of a single hyper type to the mapped dss storage type.

        :param hyper_type:
        :return:
        """
        if hyper_type not in self.mapping_hyper_to_dss:
            logger.warning("Storage type not found for hyper conversion to dss: {}".format(hyper_type))
            return 'string'
        else:
            return self.mapping_hyper_to_dss[hyper_type][0]

    def dss_value_to_hyper(self, value, dss_type='string'):
        try:
            conversion_function = self.mapping_dss_to_hyper[dss_type][1]
        except:
            logger.warning("DSS type for value conversion: {}".format(dss_type))
            logger.warning("Mapping: {}".format(self.mapping_dss_to_hyper))
            return False
        try:
            output_value = conversion_function(value)
        except:
            logger.warning("Failed to convert value {} to type {}".format(value, dss_type))
            output_value = self.mapping_dss_to_hyper[dss_type][1]
            # TODO: Look for TypeError (message in the exception)
            raise TypeError
        return output_value

    def hyper_value_to_dss(self, value, hyper_type=SqlType.text().tag):
        # TODO: Try value is None, Check NULL dans SQL
        try:
            conversion_function = self.mapping_hyper_to_dss[hyper_type][1]
        except:
            logger.warning("Hyper type for value conversion: {}".format(hyper_type))
            logger.warning("Mapping: {}".format(self.mapping_dss_to_hyper))
            return False
        try:
            output_value = conversion_function(value)
        except:
            logger.warning("Failed to convert value {} to type {}".format(value, hyper_type))
            raise TypeError
        return output_value