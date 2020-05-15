# -*- coding: utf-8 -*-
from tableausdk import *
from tableausdk.HyperExtract import *
import tableauserverclient as TSC

TABLEAU_FORMAT = 'hyper'


# Because of a bug in Tableau, we need to load Tableau before pandas.
# See http://community.tableau.com/thread/156790 for more info

import dataiku
import os
from dataiku.customrecipe import *
from datetime import datetime

import numpy as np
import pandas as pd

typeMap = {
    'tinyint': Type.INTEGER,
    'smallint':Type.INTEGER,
    'int':     Type.INTEGER,
    'bigint':  Type.INTEGER,
    'float':   Type.DOUBLE,
    'double':  Type.DOUBLE,
    'boolean': Type.BOOLEAN,
    'string':  Type.UNICODE_STRING,
    'date':    Type.DATETIME,
    'array':   Type.UNICODE_STRING,
    'map':     Type.UNICODE_STRING,
    'object':  Type.UNICODE_STRING
    }


def set_int(row, col, val):
    if np.isnan(val):
        return
    row.setInteger(col, int(val))


def set_long(row, col, val):
    if np.isnan(val):
        return
    row.setLongInteger(col, int(val))


def set_double(row, col, val):
    if isinstance(val, float) and np.isnan(val):
        return
    row.setDouble(col, float(val))


def set_str(row, col, val):
    if isinstance(val, float):
        if np.isnan(val):
            return
        else:
            row.setString(col, unicode(val)),
    elif isinstance(val, str):
        row.setString(col, val.decode("utf8"))
    else:
        row.setString(col, unicode(val))


def set_date(row, col, val):
    if pd.isnull(val):
        return
        # For some weird reason, the fractional second must be given in 1/10000 second
    row.setDateTime(col, val.year, val.month, val.day, val.hour, val.minute, val.second, val.microsecond / 100)

fieldSetterMap = {
    'boolean':  lambda row, col, val: row.setBoolean(col, val),
    'tinyint':  set_int,
    'smallint': set_int,
    'int':      set_int,
    'bigint':   set_long,
    'float':    set_double,
    'double':   set_double,
    'date':     set_date,
    'string':   set_str,
    'array':    set_str,
    'map':      set_str,
    'object':   set_str
}


class TableauExport(object):
    @classmethod
    def get_inferred_tableau_format(clf):
        return TABLEAU_FORMAT
    
    @staticmethod
    def make_table_definition(schema):
        table_def = TableDefinition()
        table_def.setDefaultCollation(Collation.EN_GB)
        for col in schema:
            table_def.addColumn(col['name'], typeMap.get(col["type"],Type.UNICODE_STRING))
        return table_def

    def __init__(self, extract_file_path, input_schema):
        self.extract_file_path = extract_file_path
        self.output_path = os.path.dirname(self.extract_file_path)
        self.input_schema = input_schema
        self.format_type = TABLEAU_FORMAT

        self.errors = 0
        self.nrows = 0

        print("Writing Extract {} file: {}".format(self.format_type, self.extract_file_path))
        ExtractAPI.initialize()
        self.extract = Extract(self.extract_file_path)
        assert(not self.extract.hasTable('Extract'))
        self.table_def = TableauExport.make_table_definition(self.input_schema)
        self.table = self.extract.addTable('Extract', self.table_def)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close();

    def close(self):
        self.extract.close()
        ExtractAPI.cleanup()
        if self.errors > 0:
            print "Encountered %d errors" % self.errors

    def insert_array_row(self, input_row):
        # Work around bug in DSS 3.1.0 API for single-column datasets
        if len(input_row) == 0 and len(self.input_schema) == 1:
            input_row = [u'']
        nb_cols = len(self.input_schema)
        output_row = Row(self.table_def)
        for col_no in range(nb_cols):
            col = self.input_schema[col_no]
            data = input_row[col_no]
            try:
                fieldSetterMap[col['type']](output_row, col_no, data)
            except Exception, e:
                self.errors += 1
                if self.errors < 100 or (self.errors < 10000 and self.errors % 100 == 0) or (self.errors % 1000 ==0):
                    try:
                        print "[err #%s] Failed setting: col=%s type=%s val=%s  err=%s" % (self.errors, col["name"],  col["type"], data, e)
                    except Exception, e2:
                        print "[err #%s] Failed setting: col=%s type=%s val=NON_PRINTED  err=%s" % (self.errors, col["name"],  col["type"],e)
                #raise
        self.table.insert(output_row)
        self.nrows += 1
        if self.nrows % 5000 == 0:
            print("%s: Exported %d rows" % (self.format_type, self.nrows))


def upload_tde_file(tde_file, config):
    print "Start upload to Tableau server"
    ServerAPI.initialize()
    server_connection = ServerConnection()
    proxyUsername = config.get("proxy_username",'')
    proxyPassword = config.get("proxy_password",'')
    if proxyUsername != '':
        server_connection.setProxyCredentials(proxyUsername, proxyPassword)

    print "Connecting to server"
    server_connection.connect(
        config['server_url'],
        config['username'],
        config.get('password',''),
        config.get('site_id','') );

    project = config.get('project','default')
    if project == '':
        project = 'default'
    output_table = config.get('output_table','DSS_extract')
    if output_table == '':
        output_table = 'DSS_extract'

    print "Publishing extract"
    server_connection.publishExtract(
        tde_file,
        project,
        output_table,
        True ); # overwrite

    print "Disconnecting ..."
    server_connection.disconnect();
    server_connection.close();
    ServerAPI.cleanup();
    print "End upload to Tableau server"


def get_project_from_name(server, project_name_utf8):
    # We cannot use Tableau API Filtering as it has some limitation in special characters. Indeed, ",", "&", ":" and
    # all characters that usually appear in url break the request. The only solution, for the moment, is to iterate
    # through all the projets.
    page_nb = 1
    while True:
        all_project_items, pag_it = server.projects.get(req_options=TSC.RequestOptions(pagenumber=page_nb))
        filtered_projects = list(
            filter(lambda x: x.name.encode('utf-8') == project_name_utf8, all_project_items)
        )
        if filtered_projects:
            return filtered_projects[0]
        if pag_it.page_number == (pag_it.total_available // pag_it.page_size) + 1:
            raise ValueError('Project {} does not exist on server'.format(project_name_utf8))
        page_nb += 1

def get_datasource_from_name(server, output_table_utf8):
    page_nb = 1
    while True:
        all_datasources, pag_it = server.datasources.get(req_options=TSC.RequestOptions(pagenumber=page_nb))
        filtered_datasources = list(
            filter(lambda x: x.name.encode('utf-8') == output_table_utf8, all_datasources)
        )
        if filtered_datasources:
            datasource = filtered_datasources[0]
            print('WARN: Found existing table {} with id {}, will be overwritten'.format(
                datasource.name.encode('utf-8'), datasource.id)
            )
            return datasource
        if pag_it.page_number == (pag_it.total_available // pag_it.page_size) + 1: return
        page_nb += 1