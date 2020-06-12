"""
Gives functions for interaction with Tableau Server through the Tableau Server Client API
"""

import logging

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def get_project_from_name(server, project_name):
    """
    Retrieve the target project from Tableau Server
    :param server: tableau server
    :param project_name: target project name
    :return: couple (Boolean{target project exists on server}, project)
    """
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


def get_full_list_of_projects(server):
    """
    Return the full list of projects on Tableau Server
    :param server:
    :return:
    """
    page_nb = 1
    while True:
        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        project_names = [x.name for x in all_project_items]
        if pag_it.page_number == (pag_it.total_available // pag_it.page_size) + 1:
            return project_names
        page_nb += 1
