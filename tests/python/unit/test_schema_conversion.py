from schema_conversion import dss_is_geo, geo_to_text
from schema_conversion import SchemaConversion
from unittest import TestCase

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
    TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName


class TestSchemaConversion(TestCase):

    def test_dss_is_geo(self):
        dss_schema = {"columns": [
            {"name": "customer_id", "type": "bigint"},
            {"name": "location", "type": "geopoint"}]}
        is_geo = dss_is_geo(dss_schema)
        assert is_geo

        dss_schema = {"columns": [{"name": "customer_id", "type": "bigint"}]}
        is_geo = dss_is_geo(dss_schema)
        assert not is_geo

    def test_geo_to_text(self):
        dss_schema = {"columns": [
            {"name": "customer_id", "type": "bigint"},
            {"name": "location", "type": "geopoint"}
        ]}
        regular_schema = geo_to_text(dss_schema)
        assert regular_schema['columns'][1]['type'] == 'string'

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
