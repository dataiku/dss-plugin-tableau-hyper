from mock_dss_generator import MockDataset

def test_table_creation():
    dataset = MockDataset(include_geo=True, rows_number=1000)
    return True

if __name__ == "__main__":
    print(test_table_creation())