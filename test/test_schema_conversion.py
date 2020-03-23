from unittest import TestCase

from schema_conversion import dss_is_geo, geo_to_text
from schema_conversion import SchemaConversion

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
    TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

import pdb


class Test(TestCase):

    def test_dss_is_geo(self):
        dss_schema = {"columns": [
            {"name": "customer_id", "type": "bigint"},
            {"name": "location", "type": "geopoint"}]}
        is_geo = dss_is_geo(dss_schema)
        assert is_geo

        dss_schema = {"columns": [{"name": "customer_id", "type": "bigint"}]}
        is_geo = dss_is_geo(dss_schema)
        assert not is_geo


class Test(TestCase):

    def test_geo_to_text(self):
        dss_schema = {"columns": [
            {"name": "customer_id", "type": "bigint"},
            {"name": "location", "type": "geopoint"}]}
        regular_schema = geo_to_text(dss_schema)
        assert regular_schema['columns'][1]['type'] == 'string'


class TestSchemaConversion(TestCase):

    def test_dss_columns_to_hyper_columns(self):
        schema_converter = SchemaConversion()
        dss_columns = [
            {"name": "customer_id", "type": "bigint"},
            {"name": "location", "type": "geopoint"},
            {"name": "price", "type": "double"}

        ]
        hyper_columns = schema_converter.dss_columns_to_hyper_columns(dss_columns)
        columns_tags = [str(column_.type.tag) for column_ in hyper_columns]
        assert columns_tags == ['TypeTag.INT', 'TypeTag.GEOGRAPHY', 'TypeTag.DOUBLE']

    def test_hyper_columns_to_dss_columns(self):
        schema_converter = SchemaConversion()
        path_to_hyper = "./data/superstore_sample.hyper"
        hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        connection = Connection(hyper.endpoint, path_to_hyper)
        hyper_table = connection.catalog.get_table_definition(TableName('public', 'Customer'))
        connection.close()
        hyper.close()
        dss_columns = schema_converter.hyper_columns_to_dss_columns(hyper_table.columns)
        return True

    def test_set_dss_storage_types(self):
        return True

    def test_set_hyper_storage_types(self):
        return True

    def test_prepare_row_to_dss(self):
        return True

    def test_prepare_row_to_hyper(self):
        return True
