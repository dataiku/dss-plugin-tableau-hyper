import json
import logging
import os, os.path as osp

from tableau_server_utils import get_project_from_name, get_full_list_of_projects, get_tableau_server_connection
import tempfile
from custom_exceptions import InvalidPluginParameter

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def remove_empty_keys(dictionary):
    """
    Remove empty keys from dictionary
    :param dictionary:
    """
    key_to_remove = []
    for key in dictionary:
        if dictionary[key] is None or dictionary[key] == '':
            key_to_remove.append(key)
    for key in key_to_remove:
        del dictionary[key]


def check_null_values(variable_value, variable_name):
    """
    Throw exception if input variable is not defined
    :param variable_value: value of the variable
    :param variable_name: name of the input variable
    """
    if (variable_value is None) or (variable_value == ''):
        raise InvalidPluginParameter(variable_name=variable_name, variable_value=variable_value)

def upload():
    with open("uploader_config.json", "rb") as f:
        uploader_config = json.load(f)
        
    config = uploader_config["config"]
    
    server_name, username, password, site_name, ignore_ssl, auth_type = get_tableau_server_connection(config)
        
    logger.info("Detected following user input configuration:\n"
                    "     username: {},\n"
                    "   server_url: {},\n"
                    "    site_name: {}".format(username, server_name, site_name))

    # Retrieve Tableau Hyper and Server/Online locations and table configurations
    
    project_id = None
    retrieve_project_list = config.get('retrieve_project_list', False)
    if retrieve_project_list:
        project_id = config.get('project_id')
    project_name = config.get('project', 'default')
    schema_name = 'Extract'
    table_name = 'Extract'

    logger.info("Detected following Tableau Hyper file configuration:\n"
                    "       project_name: {},\n"
                    "        schema_name: {},\n"
                    "         table_name: {},\n".format(
                project_name, schema_name, table_name))

    # Open connection to Tableau Server
    if auth_type == "pta-preset":
        tableau_auth = tsc.PersonalAccessTokenAuth(username, password, site_id=site_name)
    else:
        tableau_auth = tsc.TableauAuth(username, password, site_id=site_name)
    server = tsc.Server(server_name)
    if ignore_ssl:
        server.add_http_options({'verify': False})
        logger.info("HTTP client is ignoring SSL check.")

    # Retrieve target project from Tableau Server/Online
    with server.auth.sign_in(tableau_auth):
            if project_id:
                tableau_datasource = tsc.DatasourceItem(project_id) # Create new datasource
                logger.info("Publishing to project %s" % project_id)
            else:
                exists, project = get_project_from_name(server, project_name.encode("utf-8"))
                if not exists:
                    project_names = get_full_list_of_projects(server)
                    logger.warning("Target project {} seems to be unexisting on server, projects accessible on server are:"
                                "{}".format(project_name, project_names))
                    raise ValueError('The project {} does not exist on server.'.format(project_name))
                tableau_datasource = tsc.DatasourceItem(project.id) # Create new datasource
                logger.info("Publishing to project %s" % project.id)

            server.datasources.publish(tableau_datasource, uploader_config["hyperFilePath"], 'Overwrite')
            
if __name__ == "__main__":
    upload()