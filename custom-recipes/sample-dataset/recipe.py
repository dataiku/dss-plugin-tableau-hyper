"""
A recipe for creating a random dataset in DSS with an exhaustive list of types.
"""

import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np
import datetime
import random
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

# Should read one table only
output_names = get_output_names_for_role('main_output')
output_dataset = dataiku.Dataset(output_names[0])

n_rows = get_recipe_config()['log_rows']
N = 10**int(n_rows)

geo_bool = get_recipe_config()['geo_bool']
geo_bool = bool(geo_bool)

date_bool = get_recipe_config()['date_bool']
date_bool = bool(date_bool)


def generate_date():
    timestamp = datetime.datetime.now().timestamp()
    dt_object = datetime.datetime.fromtimestamp(timestamp)
    return dt_object


def generate_geopoint():
        lat = round(-73.98 + random.random()*0.2, 2)
        long = round(40.75 + random.random()*0.2, 2)
        return "POINT({} {})".format(lat, long)


dataset = {}
dataset['int_serie'] = np.random.randint(0, 1000, size=N)
dataset['float_serie'] = np.random.random(size=N)
if geo_bool:
    dataset['geo_serie'] = [generate_geopoint() for i in range(N)]
if date_bool:
    dataset['date_serie'] = [generate_date() for i in range(N)]

output_dataset.write_with_schema(pd.DataFrame(dataset))
