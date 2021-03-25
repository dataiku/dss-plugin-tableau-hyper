import datetime
from type_conversion import TypeConversion
from schema_conversion import SchemaConversion
from tableauhyperapi import TypeTag, HyperProcess, Connection, Telemetry, TableName
from unittest import TestCase


class TestTypeConversion(TestCase):

    def test_to_dss_date(self):
        schema_converter = SchemaConversion()
        path_to_hyper = "data/superstore_sample.hyper"
        hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        connection = Connection(hyper.endpoint, path_to_hyper)
        hyper_table = TableName('public', 'Orders')
        hyper_table_def = connection.catalog.get_table_definition(hyper_table)
        result = connection.execute_query(f'SELECT * FROM {hyper_table}')
        for row in result:
            pass
        sample_date = row[2].to_date()
        dss_date = datetime.datetime(sample_date.year, sample_date.month, sample_date.day)
        connection.close()
        hyper.close()
        dss_columns = schema_converter.hyper_columns_to_dss_columns(hyper_table_def.columns)
        return True

    def test_dss_type_to_hyper(self):
        type_converter = TypeConversion()
        hyper_type = type_converter.dss_type_to_hyper('bigint')
        assert str(hyper_type.tag) == 'TypeTag.BIG_INT'

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
        return True
