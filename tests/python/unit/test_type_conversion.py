import datetime
from type_conversion import TypeConversion
from schema_conversion import SchemaConversion
from tableauhyperapi import TypeTag, HyperProcess, Connection, Telemetry, TableName
from unittest import TestCase
import pandas as pd

from tableau_server_utils import get_hyper_process


class TestTypeConversion(TestCase):

    def test_to_dss_date(self):
        schema_converter = SchemaConversion(None)
        path_to_hyper = "data/superstore_sample.hyper"
        hyper = get_hyper_process()
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
        type_converter = TypeConversion(None)
        hyper_type = type_converter.dss_type_to_hyper('bigint')
        assert str(hyper_type.tag) == 'TypeTag.BIG_INT'

    def test_hyper_type_to_dss(self):
        type_converter = TypeConversion(None)
        dss_type = type_converter.hyper_type_to_dss(TypeTag.INT)
        assert dss_type == 'int'

    def test_dss_value_to_hyper(self):
        type_converter = TypeConversion(None)
        hyper_value = type_converter.dss_value_to_hyper("POINT(-90 89)", 'geopoint')
        assert hyper_value == 'point(-90 89)'

    def test_hyper_value_to_dss(self):
        type_converter = TypeConversion(None)
        dss_value = type_converter.hyper_value_to_dss("point(-90 80)", TypeTag.GEOGRAPHY)
        assert dss_value == "POINT(-90 80)"
        return True

    def test_dss_geometry_to_hyper_with_export_geometry_as_string_legacy_false(self):
        type_converter = TypeConversion(None, False)
        hyper_value = type_converter.dss_value_to_hyper("MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))", 'geometry')
        assert hyper_value == "multipolygon(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))"
        
    def test_dss_geometry_to_hyper_with_export_geometry_as_string_legacy_true(self):
        type_converter = TypeConversion(None, True)
        hyper_value = type_converter.dss_value_to_hyper("MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))", 'geometry')
        assert hyper_value == "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))"

    def test_hyper_geometry_to_dss(self):
        type_converter = TypeConversion(None)
        dss_value = type_converter.hyper_value_to_dss("multipolygon(((0 0,4 0,4 4,0 4,0 0)))", TypeTag.GEOGRAPHY)
        assert dss_value == "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0)))"

    def test_dss_geometry_type_mapping_with_export_geometry_as_string_legacy_false(self):
        type_converter = TypeConversion(None, False)
        hyper_type = type_converter.dss_type_to_hyper('geometry')
        assert str(hyper_type.tag) == 'TypeTag.GEOGRAPHY'
        
    def test_dss_geometry_type_mapping_with_export_geometry_as_string_legacy_true(self):
        type_converter = TypeConversion(None, True)
        hyper_type = type_converter.dss_type_to_hyper('geometry')
        assert str(hyper_type.tag) == 'TypeTag.TEXT'
        
    def test_dss_geometry_type_mapping_default(self):
        type_converter = TypeConversion(None)
        hyper_type = type_converter.dss_type_to_hyper('geometry')
        assert str(hyper_type.tag) == 'TypeTag.GEOGRAPHY'

    def test_dss_date_formats_to_hyper(self):
        type_converter = TypeConversion(None)
        
        # Test dateonly format
        hyper_value = type_converter.dss_value_to_hyper("2025-01-31", 'dateonly')
        assert hyper_value == "2025-01-31"
        
        # Test date with timezone formats
        hyper_value = type_converter.dss_value_to_hyper("2025-01-31T01:02:03+0200", 'date')
        expected_value = pd.to_datetime("2025-01-31T01:02:03+0200")
        assert hyper_value == expected_value
        
        hyper_value = type_converter.dss_value_to_hyper("2013-05-30T15:16:13.764+0200", 'date')
        expected_value = pd.to_datetime("2013-05-30T15:16:13.764+0200")
        assert hyper_value == expected_value
        
        hyper_value = type_converter.dss_value_to_hyper("2013-05-30T15:16:13.764Z", 'date')
        expected_value = pd.to_datetime("2013-05-30T15:16:13.764Z")
        assert hyper_value == expected_value
        
        # Test datetimenotz format - also returns Timestamp objects
        hyper_value = type_converter.dss_value_to_hyper("2025-01-31 01:02:03", 'datetimenotz')
        expected_value = pd.to_datetime("2025-01-31 01:02:03")
        assert hyper_value == expected_value

    def test_date_type_mappings(self):
        type_converter = TypeConversion(None)
        
        date_hyper_type = type_converter.dss_type_to_hyper('date')
        dateonly_hyper_type = type_converter.dss_type_to_hyper('dateonly')
        datetimenotz_hyper_type = type_converter.dss_type_to_hyper('datetimenotz')
        
        assert str(date_hyper_type.tag) == 'TypeTag.TIMESTAMP'
        assert str(dateonly_hyper_type.tag) == 'TypeTag.DATE'
        assert str(datetimenotz_hyper_type.tag) == 'TypeTag.TIMESTAMP'
