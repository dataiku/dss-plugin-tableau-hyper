import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np
import datetime
import random

output_names = get_output_names_for_role('main_output')
output_dataset = dataiku.Dataset(output_names[0])

n_rows = get_recipe_config()['n_rows']
N = 10**int(n_rows)


def generate_date():
    timestamp = datetime.datetime.now().timestamp()
    dt_object = datetime.datetime.fromtimestamp(timestamp)
    return dt_object


def generate_geopoint(self):
        lat = round(-73.98 + random.random()*0.2, 2)
        long = round(40.75 + random.random()*0.2, 2)
        return "POINT({} {})".format(lat, long)


int_serie = np.random.randint(0, 10000, size=N)
float_serie = np.random.random(size=N)
date_serie = [generate_date() for i in range(N)]
geopoint_serie = [generate_geopoint() for i in range(N)]

output = {"int_serie": int_serie,
          "float_serie": float_serie,
          "date_serie": date_serie,
          "geopoint_serie": geopoint_serie}

output_dataset.write_with_schema(pd.DataFrame(output))