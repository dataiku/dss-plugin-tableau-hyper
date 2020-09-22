import datetime
from type_conversion import TypeConversion
from schema_conversion import SchemaConversion
from tableauhyperapi import TypeTag, HyperProcess, Connection, Telemetry, TableName
from unittest import TestCase


class TestTypeConversion(TestCase):

    def test_dss_type_to_hyper(self):
        type_converter = TypeConversion()
        hyper_type = type_converter.dss_type_to_hyper('bigint')
        assert str(hyper_type.tag) == 'TypeTag.INT'

    def test_hyper_type_to_dss(self):
        type_converter = TypeConversion()
        dss_type = type_converter.hyper_type_to_dss(TypeTag.INT)
        assert dss_type == 'int'

    def test_dss_value_to_hyper(self):
        type_converter = TypeConversion()
        hyper_value = type_converter.dss_value_to_hyper("POINT(-90 89)", 'geopoint')
        assert hyper_value == 'point(-90 89)'

    def test_hyper_value_to_dss(self):
        type_converter = TypeConversion()
        dss_value = type_converter.hyper_value_to_dss("point(-90 80)", TypeTag.GEOGRAPHY)
        assert dss_value == "POINT(-90 80)"
