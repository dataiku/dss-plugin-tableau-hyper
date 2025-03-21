import datetime
import logging
import math
import numpy as np
import pandas as pd

from tableauhyperapi import SqlType
from tableauhyperapi import TypeTag

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def to_dss_date_dateonly(hyper_date):
    """
    To convert Tableau Hyper date (just a date) to a DSS date
    (i.e. our legacy date time type which represents datetime with timestamp),
    but only populating the date components.
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

def to_dss_timestamp_tz(hyper_timestamp):
    return hyper_timestamp.to_datetime()

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
    if isinstance(dss_date, str):
        try:
            dss_date = pd.to_datetime(dss_date)
        except:
            return
    if pd.isna(dss_date):
        # Tableau does not handle NaT, so replacing them with None
        return
    return dss_date

def to_hyper_date(dss_dateonly):
    """
    Format a dateonly object from DSS to Tableau Hyper
    :param dss_dateonly: A DSS dateonly value
    :return: Tableau Hyper date value
    """
    return dss_dateonly

def to_hyper_geography(dss_geopoint):
    """
    Format a `geo` typed value from DSS to Tableau Hyper
    :param dss_geopoint: `geo` typed value
    :return: Tableau Hyper geo value
    """
    return dss_geopoint.lower()


class TypeConversion(object):

    def __init__(self, config):

        self.config = config 

        """
        Handler for conversion of storage types between DSS and Tableau Hyper
        """
        handle_null = lambda f: lambda x: None if pd.isna(x) else f(x)

        # Mapping DSS to Tableau Hyper types
        self.mapping_dss_to_hyper = {
            'array': (SqlType.text(), handle_null(str)),
            'bigint': (SqlType.big_int(), handle_null(int)),
            'boolean': (SqlType.bool(), handle_null(bool)),
            'dateonly': (SqlType.date(), handle_null(to_hyper_date)),
            'datetimenotz': (SqlType.timestamp(), handle_null(to_hyper_timestamp)),
            'double': (SqlType.double(), handle_null(float)),
            'float': (SqlType.double(), handle_null(float)),
            'geometry': (SqlType.text(), handle_null(str)),
            'geopoint': (SqlType.geography(), handle_null(to_hyper_geography)),
            'int': (SqlType.int(), handle_null(int)),
            'map': (SqlType.text(), handle_null(str)),
            'object': (SqlType.text(), handle_null(str)),
            'smallint': (SqlType.small_int(), handle_null(int)),
            'string': (SqlType.text(), handle_null(str)),
            'tinyint': (SqlType.small_int(), handle_null(int)),
        }

        # Legacy mode exported DSS "date" to Hyper "timestamp", which was wrong. Modern mode exports it as timestamp_tz
        if self.config is None or self.config.get("temporal_export_mode", "LEGACY") == "LEGACY":
            self.mapping_dss_to_hyper['date'] = (SqlType.timestamp(), handle_null(to_hyper_timestamp))
        else:
            self.mapping_dss_to_hyper['date'] = (SqlType.timestamp_tz(), handle_null(to_hyper_timestamp))

        # Mapping Tableau Hyper to DSS types
        self.mapping_hyper_to_dss = {
            TypeTag.BIG_INT: ('bigint', handle_null(int)),
            TypeTag.BYTES: ('string', handle_null(str)),
            TypeTag.BOOL: ('boolean', handle_null(bool)),
            TypeTag.CHAR: ('string', handle_null(str)),
            TypeTag.DOUBLE: ('double', handle_null(float)),
            TypeTag.GEOGRAPHY: ('geopoint', handle_null(to_dss_geopoint)),
            TypeTag.INT: ('int', handle_null(int)),
            TypeTag.INTERVAL: ('string', handle_null(str)),
            TypeTag.JSON: ('string', handle_null(str)),
            TypeTag.NUMERIC: ('double', handle_null(float)),
            TypeTag.OID: ('string', handle_null(str)),
            TypeTag.SMALL_INT: ('smallint', handle_null(int)),
            TypeTag.TEXT: ('string', handle_null(str)),
            TypeTag.VARCHAR: ('string', handle_null(str))
        }

        if self.config is None or self.config.get("temporal_import_mode", "LEGACY") == "LEGACY":
            self.mapping_hyper_to_dss[TypeTag.DATE] = ('date', handle_null(to_dss_date_dateonly))
            self.mapping_hyper_to_dss[TypeTag.TIME] = ('string', handle_null(str))
            self.mapping_hyper_to_dss[TypeTag.TIMESTAMP] = ('date', handle_null(to_dss_timestamp))
            self.mapping_hyper_to_dss[TypeTag.TIMESTAMP_TZ] = ('string', handle_null(str))
        else:
            self.mapping_hyper_to_dss[TypeTag.DATE] = ('dateonly', handle_null(str))
            self.mapping_hyper_to_dss[TypeTag.TIME] = ('string', handle_null(str))
            self.mapping_hyper_to_dss[TypeTag.TIMESTAMP] = ('datetimenotz', handle_null(to_dss_timestamp))
            self.mapping_hyper_to_dss[TypeTag.TIMESTAMP_TZ] = ('date', handle_null(to_dss_timestamp_tz))

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
        try:
            (tag, function) = self.mapping_dss_to_hyper.get(dss_type, (None, None))
            if tag is None:
                logger.warning("Invalid DSS storage type {}".format(dss_type))
                raise ValueError("Invalid DSS storage type {}".format(dss_type))
            else:
                return tag
        except Exception as err:
            raise err

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
        try:
            (dss_type, function) = self.mapping_hyper_to_dss.get(hyper_type, (None, None))
            if dss_type is None:
                logger.warning("Invalid DSS storage type {}".format(hyper_type))
                raise ValueError("Invalid DSS storage type {}".format(hyper_type))
            else:
                return dss_type
        except Exception as err:
            raise err

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
        try:  # retrieve the conversion function
            conversion_function = self.mapping_dss_to_hyper[dss_type][1]
        except Exception as err:
            logger.warning("Failed to retrieve the conversion function for type {}".format(dss_type))
            raise err
        try:  # convert the dss value in the hyper storage type
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
        try:  # try to retrieve the conversion function
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
