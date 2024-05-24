# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario


TEST_PROJECT_KEY = "PLUGINTESTTABLEAUHYPER"


def test_with_pat(user_dss_clients):
    dss_scenario.run(
        user_dss_clients,
        project_key=TEST_PROJECT_KEY,
        scenario_id="WITH_PAT",
        user="data_scientist_1",
    )


def test_with_geom_and_pat(user_dss_clients):
    dss_scenario.run(
        user_dss_clients,
        project_key=TEST_PROJECT_KEY,
        scenario_id="WITH_GEOM_AND_PAT",
        user="data_scientist_1",
    )
