import logging
import pickle

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter
from exporter import TableauHyperExporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')

args = {"config": None, "plugin_config": None, "rows": None, "schema": None}
path = "/Users/thibaultdesfontaines/Library/DataScienceStudio/dss_home/plugins/dev/" \
       "tableau-hyper-export/python-test/mock_dss_inputs/test_null_values/{}.pkl"
for key in args:
    with open(path.format(key), "rb") as file:
        args[key] = pickle.load(file)

exporter = TableauHyperExporter(args["config"], args["plugin_config"])
destination_file_path = "/Users/thibaultdesfontaines/Library/DataScienceStudio/dss_home/plugins/dev/" \
                        "tableau-hyper-export/python-test/mock_dss_inputs/test_null_values/test_output/output.hyper"
exporter.open_to_file(args['schema'], destination_file_path)
for row in args['rows']:
    exporter.write_row(row)
exporter.close()


print(args['config'])
print(args['plugin_config'])
print(args['schema'])
print(args['rows'][:5])
