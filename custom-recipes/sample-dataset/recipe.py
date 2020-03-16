import dataiku
from dataiku.customrecipe import *

input_A_names = get_input_names_for_role('input_A_role')
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

output_names = get_output_names_for_role('main_output')
output_dataset = dataiku.Dataset(output_names[0])

my_variable = get_recipe_config()['n_rows']
my_variable = get_recipe_config().get('n_rows', None)

import dataiku
import pandas as pd, numpy as np

output = {"A": np.random.randint(0, 100, size=100)}

output_dataset.write_with_schema(pd.DataFrame(output))