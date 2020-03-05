"""
    Type conversion between Hyper and DSS in order to have a read capability in the plugin.
"""

from tableauhyperapi import SqlType

from type_casting import enforce_bool
from type_casting import enforce_float
from type_casting import enforce_int
from type_casting import enforce_string


class TypeConversionHyperDSS:
    """
        Convert the types available in Hyper format into a compliant dss types.
        Based on the following consideration for Hyper Type naming convention:
        >>> assert(str(SqlType.text())) == 'TEXT'
        >>> assert(str(SqlType.bool())) == 'BOOL'
    """
    def __init__(self):
        # TODO: Check the exhaustive list of types from Tableau
        self.mapping_hyper_to_dss = {
            'INT': 'bigint',
            'DOUBLE': 'double'
        }

        self.mapping_dss_compliant = {
            'INT': enforce_int,
            'FLOAT': enforce_float,
            'BOOL': enforce_bool,
            'STRING': enforce_string
        }

if __name__ == "__main__":
    type_converter = TypeConversionHyperDSS()