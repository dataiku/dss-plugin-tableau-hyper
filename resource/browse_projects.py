from tableau_server_utils import get_dict_of_projects_paths, build_directory_structure, get_tableau_server_connection
import tableauserverclient as client


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
        server_url, username, password, site_id, ignore_ssl = get_tableau_server_connection(config)
        server = client.Server(server_url, use_server_version=True)
        if ignore_ssl:
            server.add_http_options({'verify': False})
        tableau_auth = client.TableauAuth(username, password, site_id=site_id)
        with server.auth.sign_in(tableau_auth):
            project_details = get_dict_of_projects_paths(server)
            project_sorted = build_directory_structure(project_details)
        choices = []
        for project_name in project_sorted:
            choices.append({
                "label": "{}".format(project_name),
                "value": "{}".format(project_sorted.get(project_name))
            })
        return build_select_choices(choices)
    return build_select_choices()
