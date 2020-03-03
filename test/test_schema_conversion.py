from export_schema_conversion import SchemaConversionDSSHyper
from mock_dss_generator import MockDataset

import logging
import pdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

def test_schema_conversion():
    dataset = MockDataset(include_geo=True)
    converter = SchemaConversionDSSHyper()
    assert converter.is_geo(dataset.schema)
    text_schema = converter.dss_schema_geo_to_text(dataset.schema)
    print(dataset.schema)
    print(text_schema)
    hyper_schema_w_geo = converter.convert_schema_dss_to_hyper(dataset.schema)
    hyper_schema_wo_geo = converter.convert_schema_dss_to_hyper(text_schema)
    return True

if __name__ == "__main__":
    print(test_schema_conversion())