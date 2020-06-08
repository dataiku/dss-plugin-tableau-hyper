import datetime
import logging
import math
import numpy as np
import pandas as pd
import pickle

from tableauhyperapi import SqlType
from tableauhyperapi import TypeTag

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def to_dss_date(hyper_date):
    """
    Convert Tableau Hyper date to DSS date
    :param hyper_date: <class 'pandas._libs.tslibs.timestamps.Timestamp'>
    :return: A date object readable by DSS
    """
    intermediary_date = hyper_date.to_date()
    return datetime.datetime(intermediary_date.year, intermediary_date.month, intermediary_date.day)


def to_dss_timestamp(hyper_timestamp):
    """
    Convert Tableau Hyper Timestamp to DSS date
    :param hyper_date: <class 'pandas._libs.tslibs.timestamps.Timestamp'>
    :return: A timestamp object readable by DSS
    """
    return datetime.datetime(
             hyper_timestamp.year,
             hyper_timestamp.month,
             hyper_timestamp.day,
             hyper_timestamp.hour,
             hyper_timestamp.minute,
             hyper_timestamp.second,
             hyper_timestamp.microsecond)


def to_dss_geopoint(hyper_string):
    """
    Format geopoint type from Tableau Hyper to DSS
    :param hyper_string: String type in Tableau
    :return: A pre-formatted string before export as geopoint
    """
    return hyper_string.upper()


def to_hyper_timestamp(dss_date):
    """
    Format a date object from DSS to Tableau Hyper
    :param dss_date: A DSS date value
    :return: Tableau Hyper date value
    """
    return dss_date


def to_hyper_geography(dss_geopoint):
    """
    Format a `geo` typed value from DSS to Tableau Hyper
    :param dss_geopoint: `geo` typed value
    :return: Tableau Hyper geo value
    """
    return dss_geopoint.lower()


class TypeConversion(object):

    def __init__(self):
        """
        Handler for conversion of storage types between DSS and Tableau Hyper

        DSS storage types:

        "string","date","geopoint","geometry","array","map","object","double",
        "boolean","float","bigint","int","smallint","tinyint"

        Tableau Hyper storage types:

        TypeTag.BOOL, TypeTag.BIG_INT, TypeTag.SMALL_INT, TypeTag.INT, TypeTag.NUMERIC,
        TypeTag.DOUBLE, TypeTag.OID, TypeTag.BYTES, TypeTag.TEXT, TypeTag.VARCHAR, TypeTag.CHAR,
        TypeTag.JSON, TypeTag.DATE, TypeTag.INTERVAL, TypeTag.TIME, TypeTag.TIMESTAMP,
        TypeTag.TIMESTAMP_TZ, TypeTag.GEOGRAPHY

        """

        # Define functions for explicit conversion of null values
        # Handle nan values, mostly for quantitative values
        handle_nan = lambda f: lambda x: None if x is np.nan else f(x)
        # Handle nat values, for dates
        handle_nat = lambda f: lambda x: None if issubclass(type(x), type(pd.NaT)) else f(x)

        # Mapping DSS to Tableau Hyper types
        self.mapping_dss_to_hyper = {
            'array': (SqlType.text(), handle_nan(str)),
            'bigint': (SqlType.int(), handle_nan(int)),
            'boolean': (SqlType.bool(), handle_nan(bool)),
            'date': (SqlType.timestamp(), handle_nat(to_hyper_timestamp)),
            'double': (SqlType.double(), handle_nan(float)),
            'float': (SqlType.double(), handle_nan(float)),
            'geometry': (SqlType.text(), handle_nan(str)),
            'geopoint': (SqlType.geography(), handle_nan(to_hyper_geography)),
            'int': (SqlType.int(), handle_nan(int)),
            'map': (SqlType.text(), handle_nan(str)),
            'object': (SqlType.text(), handle_nan(str)),
            'smallint': (SqlType.int(), handle_nan(int)),
            'string': (SqlType.text(), handle_nan(str)),
            'tinyint': (SqlType.int(), handle_nan(int)),
        }

        # Mapping Tableau Hyper to DSS types
        self.mapping_hyper_to_dss = {
            TypeTag.BIG_INT: ('bigint', lambda x: None if math.isnan(x) else int(x)),
            TypeTag.BYTES: ('string', lambda x: None if x is None else str(x)),
            TypeTag.BOOL: ('bool', lambda x: None if x is None else bool(x)),
            TypeTag.CHAR: ('string', lambda x: None if x is None else str(x)),
            TypeTag.DATE: ('date', lambda x: None if x is None else to_dss_date(x)),
            TypeTag.DOUBLE: ('double', lambda x: None if math.isnan(x) else float(x)),
            TypeTag.GEOGRAPHY: ('geopoint', lambda x: None if x is None else to_dss_geopoint(x)),
            TypeTag.INT: ('int', lambda x: None if math.isnan(x) else int(x)),
            TypeTag.INTERVAL: ('string', lambda x: None if x is None else str(x)),
            TypeTag.JSON: ('string', lambda x: None if x is None else str(x)),
            TypeTag.NUMERIC: ('double', lambda x: None if math.isnan(x) else float(x)),
            TypeTag.OID: ('string', lambda x: None if x is None else str(x)),
            TypeTag.SMALL_INT: ('int', lambda x: None if math.isnan(x) else int(x)),
            TypeTag.TEXT: ('string', lambda x: None if x is None else str(x)),
            TypeTag.TIME: ('string', lambda x: None if x is None else str(x)),
            TypeTag.TIMESTAMP: ('date', lambda x: None if x is None else to_dss_timestamp(x)),
            TypeTag.TIMESTAMP_TZ: ('string', lambda x: None if x is None else str(x)),
            TypeTag.VARCHAR: ('string', lambda x: None if x is None else str(x))
        }

    def dss_type_to_hyper(self, dss_type):
        """
        Convert an identifier (string) of a single dss storage type to the mapped hyper type.

        :param dss_type: Storage type string identifier from DSS
            examples:
            >>> "bigint"
        :return: The mapped Tableau Hyper Type
            examples:
            >>> SqlType.big_int()
        """
        if dss_type not in self.mapping_dss_to_hyper:
            logger.warning("Invalid DSS storage type {}".format(dss_type))
            raise ValueError("Invalid DSS storage type {}".format(dss_type))
        elif self.mapping_dss_to_hyper[dss_type] is None:
            logger.warning("DSS type not supported in conversion: {}".format(dss_type))
            logger.warning("Setting the target conversion type to `SqlType.text()`")
            return SqlType.text()
        else:
            return self.mapping_dss_to_hyper[dss_type][0]

    def hyper_type_to_dss(self, hyper_type):
        """
        Convert an identifier (string) of a single hyper type to the mapped dss storage type.

        :param hyper_type:
            examples:
            >>> TypeTag.BIG_INT
        :return: the mapped DSS storage type
            examples:
            >>> 'bigint'
        """
        if hyper_type not in self.mapping_hyper_to_dss:
            logger.warning("Invalid Hyper storage type {}".format(hyper_type))
            raise ValueError("Invalid Hyper storage type {}".format(hyper_type))
        elif self.mapping_hyper_to_dss[hyper_type] is None:
            logger.warning("Tableau Hyper type not supported in conversion: {}".format(hyper_type))
            logger.warning("Setting the target conversion type to `string`")
            return 'string'
        else:
            return self.mapping_hyper_to_dss[hyper_type][0]

    def dss_value_to_hyper(self, value, dss_type='string'):
        """
        Convert a `value` stored as the storage type `dss_type` in a DSS dataset
        to the mapped Tableau Hyper type.

        :param value: a value coming from a dss dataset
        :param dss_type: storage type of the value
            example:
            >>> 'bigint'
        :return: output_value : the value converted in the Tableau Hyper type, may affect its value or not
        """
        try: # retrieve the conversion function
            conversion_function = self.mapping_dss_to_hyper[dss_type][1]
        except Exception as err:
            logger.warning("Failed to retrieve the conversion function for type {}".format(dss_type))
            raise err
        try: # convert the dss value in the hyper storage type
            output_value = conversion_function(value)
        except Exception as err:
            logger.warning("Failed to convert value {} of type {} to type {}".format(value, type(value), dss_type))
            raise err
        return output_value

    def hyper_value_to_dss(self, value, tag=SqlType.text().tag):
        """
        Convert the value `value` stored in a Hyper File under the storage type
        `hyper_type_tag` to the apropriate DSS type.

        >>> from tableauhyperapi import TypeTag, SqlType
        >>> assert TypeTag.INT == SqlType.int().tag

        :param value: Value from the Hyper Dataset
        :param tag: Storage type under which the value is stored in Hyper
        :return: output_value : Value compliant to Hyper
        """
        try: # try to retrieve the conversion function
            conversion_function = self.mapping_hyper_to_dss[tag][1]
        except Exception as err:
            logger.warning("Failed to retrieve the conversion function {}: {}".format(tag, err))
            raise err
        try:
            output_value = conversion_function(value)
        except Exception as err:
            logger.warning("Failed to convert value {} of type {} stored as {}".format(value, type(value), tag))
            raise err
        return output_value
