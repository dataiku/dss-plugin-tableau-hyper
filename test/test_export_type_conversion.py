from type_conversion import TypeConversion
from tableauhyperapi import TypeTag

def test_mapping():
    type_converter = TypeConversion()
    type_converter.hyper_type_to_dss(TypeTag.DOUBLE)
    return True

if __name__ == "__main__":
    print(test_mapping())