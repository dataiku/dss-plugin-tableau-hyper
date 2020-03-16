import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]

nbr_rows = get_recipe_config().get('nbr_rows', None)
nbr_rows = int(nbr_rows)

serie_float = np.random.uniform(low=-10, high=10, size=nbr_rows)
serie_int = np.random.randint(0, high=1000, size=nbr_rows)

exhaustive_types_1e6_df = pd.DataFrame({
    'serie_float': serie_float,
    'serie_int': serie_int
})

exhaustive_types_1e6 = dataiku.Dataset("exhaustive_types_1e6")
exhaustive_types_1e6.write_with_schema(exhaustive_types_1e6_df)