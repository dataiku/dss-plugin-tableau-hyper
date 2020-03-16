import dataiku
from dataiku.customrecipe import *

input_A_names = get_input_names_for_role('input_A_role')
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

output_names = get_output_names_for_role('main_output')
output_dataset = dataiku.Dataset(output_names[0])

my_variable = get_recipe_config()['parameter_name']
my_variable = get_recipe_config().get('parameter_name', None)

import dataiku
import pandas as pd, numpy as np

output = {"A": np.random.randint(0, 100, size=100)}

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
output_dataset =  dataiku.Dataset("my_dataset")
output_dataset.write_with_schema(pd.DataFrame(output))