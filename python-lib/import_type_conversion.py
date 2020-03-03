"""
    Type conversion between Hyper and DSS in order to have a read capability in the plugin.
"""

from tableauhyperapi import SqlType

class TypeConversionHyperDSS:
    """
        Conversion between the hyper format and the dss schema format.
        Through the text object of the hyper Sql.Type()

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
        self.mapping_hyper_type_enforcement = {
            'INT': self.enforce_int,
            'DOUBLE': self.enforce_double,
            'STRING': self.enforce_string
        }

    def __str__(self):
        return "Type Converter from Hyper format to DSS."

if __name__ == "__main__":
    type_converter = TypeConversionHyperDSS()