"""
Gives functions for interaction with Tableau Server through the Tableau Server Client API
"""

import logging

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Hyper Plugin | %(levelname)s - %(message)s')


def get_project_from_name(server, project_name):
    # We cannot use Tableau API Filtering as it has some limitation in special characters. Indeed, ",", "&", ":" and
    # all characters that usually appear in url break the request. The only solution, for the moment, is to iterate
    # through all the projets.
    page_nb = 1
    while True:
        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        filtered_projects = list(
            filter(lambda x: x.name == project_name, all_project_items)
        )
        if filtered_projects:
            return True, filtered_projects[0]
        if pag_it.page_number == (pag_it.total_available // pag_it.page_size) + 1:
            logger.info('Project {} does not exist on server'.format(project_name))
            return False, None
        page_nb += 1


def get_datasource_from_name(server, output_table_utf8):
    page_nb = 1
    while True:
        all_datasources, pag_it = server.datasources.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
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


def get_full_list_of_projects(server):
    page_nb = 1
    while True:
        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        project_names = [x.name for x in all_project_items]
        if pag_it.page_number == (pag_it.total_available // pag_it.page_size) + 1:
            return project_names
        page_nb += 1