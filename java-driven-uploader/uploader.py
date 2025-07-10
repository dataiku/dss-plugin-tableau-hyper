import json
import logging
import os, os.path as osp
from tableau_server_utils import get_project_from_name, get_full_list_of_projects, get_tableau_server_connection
import tempfile
from custom_exceptions import InvalidPluginParameter
import tableauserverclient as tsc
from contextlib import contextmanager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')

def remove_empty_keys(dictionary):
    key_to_remove = []
    for key in dictionary:
        if dictionary[key] is None or dictionary[key] == '':
            key_to_remove.append(key)
    for key in key_to_remove:
        del dictionary[key]

def check_null_values(variable_value, variable_name):
    if (variable_value is None) or (variable_value == ''):
        raise InvalidPluginParameter(variable_name=variable_name, variable_value=variable_value)

def load_configuration():
    with open("uploader_config.json", "rb") as f:
        uploader_config = json.load(f)
    return uploader_config["config"], uploader_config["hyperFilePath"]

def create_server_connection(config):
    server_name, username, password, site_name, ignore_ssl, auth_type = get_tableau_server_connection(config)
    
    logger.info("Detected following user input configuration:\n"
                "     username: {},\n"
                "   server_url: {},\n"
                "    site_name: {}".format(username, server_name, site_name))
    
    if auth_type == "pta-preset":
        tableau_auth = tsc.PersonalAccessTokenAuth(username, password, site_id=site_name)
    else:
        tableau_auth = tsc.TableauAuth(username, password, site_id=site_name)
    
    server = tsc.Server(server_name)
    if ignore_ssl:
        server.add_http_options({'verify': False})
        logger.info("HTTP client is ignoring SSL check.")
    
    return server, tableau_auth

@contextmanager
def tableau_server_session(server, tableau_auth):
    try:
        with server.auth.sign_in(tableau_auth):
            yield server
    finally:
        pass

def get_target_project(server, config):
    project_id = config.get('project_id')
    project_name = config.get('project', 'default')
    
    if project_id:
        logger.info("Using project ID: %s", project_id)
        return project_id
    else:
        exists, project = get_project_from_name(server, project_name.encode("utf-8"))
        if not exists:
            project_names = get_full_list_of_projects(server)
            logger.warning("Target project {} seems to be unexisting on server, projects accessible on server are:"
                          "{}".format(project_name, project_names))
            raise ValueError('The project {} does not exist on server.'.format(project_name))
        
        logger.info("Using project: %s (ID: %s)", project_name, project.id)
        return project.id

def log_hyper_configuration(config):
    project_name = config.get('project', 'default')
    schema_name = 'Extract'
    table_name = 'Extract'
    
    logger.info("Detected following Tableau Hyper file configuration:\n"
                "       project_name: {},\n"
                "        schema_name: {},\n"
                "         table_name: {},\n".format(
                project_name, schema_name, table_name))

def publish_datasource(server, project_id, hyper_file_path):
    tableau_datasource = tsc.DatasourceItem(project_id)
    logger.info("Publishing to project %s", project_id)
    server.datasources.publish(tableau_datasource, hyper_file_path, 'Overwrite')

def upload():
    config, hyper_file_path = load_configuration()
    server, tableau_auth = create_server_connection(config)
    log_hyper_configuration(config)
    
    with tableau_server_session(server, tableau_auth) as authenticated_server:
        project_id = get_target_project(authenticated_server, config)
        publish_datasource(authenticated_server, project_id, hyper_file_path)

if __name__ == "__main__":
    upload()