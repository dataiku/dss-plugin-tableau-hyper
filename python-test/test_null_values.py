"""
Test null values handling in Hyper converter
"""

from pandas import Timestamp
from pandas import NaT


def test_null_values():
    """
    Test conversion of null values from DSS to
    :return:
    """

    nan = float("nan")

    # ===> Expected parameters input from DSS in case of exporter component
    config = {}
    plugin_config = {}
    schema = {'columns': [{'name': 'id', 'type': 'bigint'}, {'name': 'name', 'type': 'string'},
                          {'name': 'host_id', 'type': 'int'}, {'name': 'host_id_copy', 'type': 'bigint'},
                          {'name': 'host_id_copy_copy', 'type': 'smallint'}, {'name': 'host_name', 'type': 'string'},
                          {'name': 'latitude', 'type': 'double'}, {'name': 'latitude_copy', 'type': 'float'},
                          {'name': 'longitude', 'type': 'double'}, {'name': 'price', 'type': 'double'},
                          {'name': 'price_copy', 'type': 'float'}, {'name': 'minimum_nights', 'type': 'bigint'},
                          {'name': 'number_of_reviews', 'type': 'bigint'}, {'name': 'last_review', 'type': 'string'},
                          {'name': 'last_review_parsed', 'type': 'date'}, {'name': 'reviews_per_month', 'type': 'double'}], 'userModified': True}
    rows = [(2539.0, 'Clean', 2787.0, 2787.0, '2787', 'John', 40.647, 40.6474, -73.97237, 149.0, 149.0, 1.0, 9.0, '2018-10-19', Timestamp('2018-10-19 00:00:00'), 0.21),
            (2595.0, nan, nan, nan, nan, 'Jennifer', nan, 40.75362014770508, -73.98376999999999, 225.0, 225.0, 1.0, 45.0, '2019-05-21', Timestamp('2019-05-21 00:00:00'), 0.38),
            (nan, 'THE VILLAGE OF HARLEM....NEW YORK !', 4632.0, 4632.0, '4632', 'Elisabeth', 40.809020000000004, 40.80902099609375, -73.9419, 150.0, nan, 3.0, 0.0, nan, NaT, nan),
            (3831.0, 'Cozy Entire Floor of Brownstone', nan, 4869.0, '4869', nan, 40.685140000000004, 40.68513870239258, nan, nan, nan, 1.0, 270.0, '2019-07-05', NaT, 4.64),
            (5022.0, 'Entire Apt: Spacious Studio/Loft by central park', nan, nan, nan, 'Laura', 40.79851, 40.79851150512695, -73.94399, 80.0, 80.0, 10.0, 9.0, '2018-11-19', NaT, 0.1)]
    # <===

    # ===> Externalise the DSS exporter to execute unit tests
    exporter = TableauHyperExporter(config, plugin_config)
    destination_file_path = "/Users/thibaultdesfontaines/Library/DataScienceStudio/dss_home/plugins/dev/" \
                            "tableau-hyper-export/python-test/test_outputs/output.hyper"
    exporter.open_to_file(schema, destination_file_path)
    for row in rows:
        exporter.write_row(row)
    exporter.close()
    # <===
