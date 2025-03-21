"""
Gives functions for interaction with Tableau Server through the Tableau Server Client API
"""

import logging
import os
from tableauhyperapi import HyperProcess, Telemetry
import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')

def get_hyper_process_parameters():
    # totally disable logging (cf/ https://tableau.github.io/hyper-db/docs/hyper-api/hyper_process/#log_config)
    return {"log_config": ""}

def get_hyper_process():
    return HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                  parameters=get_hyper_process_parameters())


def get_project_from_name(server, project_name):
    """
    Retrieve the target project from Tableau Server
    :param server: tableau server
    :param project_name: target project name
    :return: couple (Boolean{target project exists on server}, project)
    """
    logger.info(f"Searching for project {project_name}")
    pages_in_total = compute_total_pages_for_projects(server)

    page_nb = 1

    while page_nb <= pages_in_total:
        all_project_items, _pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        filtered_projects = list(
            filter(lambda x: x.name.encode('utf-8') == project_name, all_project_items)
        )
        if filtered_projects:
            if len(filtered_projects) > 1:
                logger.warning('Detected multiple projects associated with the target name {}'.format(project_name))
            return True, filtered_projects[0]
        page_nb += 1

    logger.info('Project {} does not exist on server'.format(project_name))
    return False, None


def get_full_list_of_projects(server):
    """
    Return the full list of projects on Tableau Server
    :param server:
    :return:
    """
    pages_in_total = compute_total_pages_for_projects(server)
    
    all_projects = set()
    page_nb = 1

    while page_nb <= pages_in_total:

        all_project_items, _pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        project_names = [x.name.encode('utf-8') for x in all_project_items]
        for name in project_names:
            all_projects.add(name)

        page_nb += 1

    return all_projects


def get_dict_of_projects_paths(server):
    """
    Computes a dictionary of all the available projects from a Tableau server 
    """
    pages_in_total = compute_total_pages_for_projects(server)
    
    all_projects = {}
    page_nb = 1

    while page_nb <= pages_in_total:

        all_project_items, _pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        for all_project_item in all_project_items:
            all_projects[all_project_item.id] = {"name": all_project_item.name, "parent": all_project_item.parent_id}
        page_nb += 1

    return all_projects


def build_directory_structure(all_projects):
    file_system_structure = {}
    for project_id in all_projects:
        project = all_projects.get(project_id)
        project_full_path = get_project_full_path(project, all_projects)
        file_system_structure[project_full_path] = project_id
    return file_system_structure


def get_project_full_path(project, all_projects):
    parent_project_id = project.get("parent")
    if not parent_project_id:
        return project.get("name")
    else:
        parent_project = all_projects.get(parent_project_id)
        root = get_project_full_path(parent_project, all_projects)
        return root + " / " + project.get("name")


def get_legacy_tableau_server_connection(config):
    server_url = username = password = site_id = None
    use_preset = config.get('usePreset', False)
    if use_preset:
        configuration = config.get('tableau_server_connection', {})
    else:
        configuration = config
    server_url = configuration.get('server_url', None)
    username = configuration.get('username', None)
    password = configuration.get('password', None)
    site_id = configuration.get('site_id', '')

    ssl_cert_path = configuration.get('ssl_cert_path', None)
    ignore_ssl = configuration.get('ignore_ssl', False)

    setup_ssl(ignore_ssl, ssl_cert_path)

    return server_url, username, password, site_id, ignore_ssl


def get_tableau_server_connection(config):
    server_url = username = password = site_id = None
    auth_type = config.get("auth_type", None)

    if auth_type == "legacy-login":
        configuration = config
    elif auth_type == "legacy-preset":
        configuration = config.get('tableau_server_connection', {})
    elif auth_type == "basic-preset":
        configuration = config.get('tableau_server_personal_connection', {})
        tableau_personal_basic = configuration.get("tableau_personal_basic", {})
        username = tableau_personal_basic.get("user")
        password = tableau_personal_basic.get("password")
    elif auth_type == "pta-preset":
        configuration = config.get('tableau_server_pta_connection', {})
        personal_access_token = configuration.get("personal_access_token", {})
        username = personal_access_token.get("user")
        password = personal_access_token.get("password")
    else: # the auth_type selector was never used, so old conf file, use legacy mode
        server_url, username, password, site_id, ignore_ssl = get_legacy_tableau_server_connection(config)
        return server_url, username, password, site_id, ignore_ssl, auth_type

    server_url = configuration.get('server_url', None)
    username = configuration.get('username', username)
    password = configuration.get('password', password)
    site_id = configuration.get('site_id', '')

    ssl_cert_path = configuration.get('ssl_cert_path', None)
    ignore_ssl = configuration.get('ignore_ssl', False)

    setup_ssl(ignore_ssl, ssl_cert_path)

    return server_url, username, password, site_id, ignore_ssl, auth_type


def setup_ssl(ignore_ssl, ssl_cert_path):
    if not ignore_ssl and ssl_cert_path:
        if not os.path.isfile(ssl_cert_path):
            raise ValueError('SSL certificate file {} does not exist'.format(ssl_cert_path))
        else:
            # default variables handled by python requests to validate cert (used by underlying tableauserverclient)
            os.environ['REQUESTS_CA_BUNDLE'] = ssl_cert_path
            os.environ['CURL_CA_BUNDLE'] = ssl_cert_path


def compute_total_pages_for_projects(server) -> int:
    """
    Computes the number of pages necessary to query all the projects available on a Tableau server

    :param server: tableau server
    :return: Total number of pages
    """
    _project_items, pag_it = server.projects.get(
        req_options=tsc.RequestOptions(pagenumber=1)
    )

    partial_page = 1 if pag_it.total_available % pag_it.page_size > 0 else 0
    pages_in_total = (pag_it.total_available // pag_it.page_size) + partial_page

    logger.info(
        f"{pag_it.total_available} projects, divided in {pages_in_total} pages ({partial_page} partial page) with a page size of {pag_it.page_size}"
    )

    return pages_in_total
