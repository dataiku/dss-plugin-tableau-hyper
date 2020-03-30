import datetime
import logging
import numpy as np
import pandas as pd
import math

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
    intermediary_date = hyper_date.to_date()
    return datetime.datetime(intermediary_date.year, intermediary_date.month, intermediary_date.day)


def to_dss_geometry(hyper_string):
    """
        Convert the geometry in Tableau to geometry in DSS
    :param hyper_string:
    :return:
    """
    return hyper_string.upper()


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

        handle_nan = lambda f: lambda x: None if x is np.nan else f(x)
        handle_nat = lambda f: lambda x: None if issubclass(type(x), type(pd.NaT)) else f(x)

        self.mapping_dss_to_hyper = {
            'array': None,
            'bigint': (SqlType.int(), handle_nan(int)),
            'boolean': (SqlType.bool(), handle_nan(bool)),
            'date': (SqlType.date(), handle_nat(to_hyper_date)),
            'double': (SqlType.double(), handle_nan(float)),
            'float': (SqlType.double(), handle_nan(float)),
            'geometry': (SqlType.geography(), handle_nan(to_hyper_geography)),
            'geopoint': (SqlType.geography(), handle_nan(to_hyper_geography)),
            'int': (SqlType.int(), handle_nan(int)),
            'map': None,
            'object': None,
            'smallint': (SqlType.geography(), handle_nan(int)),
            'string': (SqlType.text(), handle_nan(str)),
        }

        self.mapping_hyper_to_dss = {
            TypeTag.INT: ('bigint', lambda x: None if math.isnan(x) else int(x)),
            TypeTag.BIG_INT: ('bigint', lambda x: None if math.isnan(x) else int(x)),
            TypeTag.BOOL: ('bool', lambda x: None if x is None else bool(x)),
            TypeTag.DATE: ('date', lambda x: None if x is None else to_dss_date(x)),
            TypeTag.DOUBLE: ('double', lambda x: None if math.isnan(x) else float(x)),
            TypeTag.GEOGRAPHY: ('geometry', lambda x: None if x is None else to_dss_geometry(x)),
            TypeTag.TEXT: ('string', lambda x: None if x is None else str(x))
        }

    def dss_type_to_hyper(self, dss_type):
        """
        Convert an identifier (string) of a single dss storage type to the mapped hyper type.

        :param dss_type: Storage type string identifier from DSS ("int", "bigint", "date"...)
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
        """
        Convert a value coming from a DSS dataset and stored using the
        `dss_type` storage type to the appropriate storage type in Hyper File.

        :param value: Value from DSS dataset
        :param dss_type: Storage type of the DSS dataset
        :return: output_value : Value compliant to Hyper File
        """
        try:
            conversion_function = self.mapping_dss_to_hyper[dss_type][1]
        except:
            logger.info("DSS type for value conversion: {}".format(dss_type))
            logger.info("Mapping: {}".format(self.mapping_dss_to_hyper))
            return False
        try:
            output_value = conversion_function(value)
        except:
            logger.warning("Failed to convert value {} to type {} with {}".format(value, dss_type, type(value)))
            output_value = None
        return output_value

    def hyper_value_to_dss(self, value, tag=SqlType.text().tag):
        """
        Convert the value `value` stored in a Hyper File under the storage type
        `hyper_type_tag` to the apropriate DSS type.

        >>> from tableauhyperapi import TypeTag, SqlType
        >>> assert TypeTag.INT == SqlType.int().tag

        :param value: Value from the Hyper Dataset
        :param hyper_type: Storage type under which the value is stored in Hyper
        :return: output_value : Value compliant to Hyper
        """
        try:
            conversion_function = self.mapping_hyper_to_dss[tag][1]
        except:
            logger.warning("Hyper type for value conversion: {}".format(tag))
            logger.warning("Mapping: {}".format(self.mapping_dss_to_hyper))
            return False
        try:
            output_value = conversion_function(value)
        except:
            logger.warning("Failed to convert value {} to type {}".format(value, tag))
            output_value = None
        return output_value
