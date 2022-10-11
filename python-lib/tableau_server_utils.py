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

    all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
    pages_in_total = (pag_it.total_available // pag_it.page_size) + 1

    while page_nb <= pages_in_total:
        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
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
    page_nb = 1
    all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
    pages_in_total = (pag_it.total_available // pag_it.page_size) + 1
    all_projects = set()

    while page_nb <= pages_in_total:

        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
        project_names = [x.name.encode('utf-8') for x in all_project_items]
        for name in project_names:
            all_projects.add(name)

        page_nb += 1

    return all_projects


def get_dict_of_projects_paths(server):
    page_nb = 1
    all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
    pages_in_total = (pag_it.total_available // pag_it.page_size) + 1
    all_projects = {}

    while page_nb <= pages_in_total:

        all_project_items, pag_it = server.projects.get(req_options=tsc.RequestOptions(pagenumber=page_nb))
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
