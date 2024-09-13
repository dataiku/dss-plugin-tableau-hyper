import logging
from tableau_server_utils import get_dict_of_projects_paths, build_directory_structure, get_tableau_server_connection
import tableauserverclient as client


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def build_select_choices(choices=None):
    if not choices:
        return {"choices": []}
    if isinstance(choices, str):
        return {"choices": [{"label": "{}".format(choices)}]}
    if isinstance(choices, list):
        return {"choices": choices}
    if isinstance(choices, dict):
        returned_choices = []
        for choice_key in choices:
            returned_choices.append({
                "label": choice_key,
                "value": choices.get(choice_key)
            })


def do(payload, config, plugin_config, inputs):
    parameter_name = payload.get('parameterName')
    if parameter_name == "project_id":
        retrieve_project_list = config.get('retrieve_project_list', False)
        if not retrieve_project_list:
            return build_select_choices()
        try:
            server_url, username, password, site_id, ignore_ssl, auth_type = get_tableau_server_connection(config)
        except Exception as err:
            return build_select_choices("{}".format(err))
        try:
            server = client.Server(server_url, use_server_version=True)
        except Exception as err:
            logger.error("Connection error for parameter {} : {}".format(parameter_name, err))
            return build_select_choices("Check the connection details ({})".format(err))
        if ignore_ssl:
            server.add_http_options({'verify': False})
        if auth_type=="pta-preset":
            tableau_auth = client.PersonalAccessTokenAuth(username, password, site_id=site_id)
        else:
            tableau_auth = client.TableauAuth(username, password, site_id=site_id)
        try:
            with server.auth.sign_in(tableau_auth):
                project_details = get_dict_of_projects_paths(server)
                projects = build_directory_structure(project_details)
        except Exception as err:
            logger.error("Authentication error for parameter {} : {}".format(parameter_name, err))
            return build_select_choices("Check the authentication details")
        choices = []

        # https://stackoverflow.com/questions/24728933/sort-dictionary-alphabetically-when-the-key-is-a-string-name
        sorted_projects_names = sorted(projects.keys(), key=lambda name: name.lower())

        for project_name in sorted_projects_names:
            choices.append({
                "label": "{}".format(project_name),
                "value": "{}".format(projects.get(project_name))
            })

        return build_select_choices(choices)
    return build_select_choices()
