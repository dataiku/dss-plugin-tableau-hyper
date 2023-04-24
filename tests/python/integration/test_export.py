
# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario


TEST_PROJECT_KEY = "PLUGINTESTTABLEAUHYPER"


def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="WITH_GEOM_PRESETS")

def test_with_manual_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="WITH_MANUAL_PRESETS")

def test_with_none(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="WITH_NONE")

def test_with_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="WITH_PRESETS")

def test_with_settings(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="WITH_SETTINGS")