import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np

output_names = get_output_names_for_role('main_output')
output_dataset = dataiku.Dataset(output_names[0])

n_rows = get_recipe_config()['n_rows']
N = 10**int(n_rows)

int_serie = np.random.randint(0, 10000, size=N)
float_serie = np.random.random(-1000, 1000, size=N)

output = {"int_serie": int_serie, "float_serie": float_serie}

output_dataset.write_with_schema(pd.DataFrame(output))