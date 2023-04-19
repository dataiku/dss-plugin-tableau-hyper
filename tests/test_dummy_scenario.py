
# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario


TEST_PROJECT_KEY = "http://localhost:15200/projects/PLUGINTESTTABLEAUHYPER/flow/"


def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="with_geom_presets")

def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="with_manual_presets")

def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="with_none")

def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="with_presets")

def test_with_geom_presets(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="with_settings")