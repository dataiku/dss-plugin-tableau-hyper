from type_conversion import TypeConversionDSSHyper

import datetime
import pdb


def test_type_conversion():
    converter = TypeConversionDSSHyper()
    assert converter.translate('bigint').__str__() == "INT"
    assert converter.translate('geopoint').__str__() == "GEOGRAPHY"
    assert converter.translate('bad_type_input').__str__() == "TEXT"
    assert converter.set_type("POINT(-90 89)", 'geopoint') == "point(-90 89)"
    timestamp = datetime.datetime.now().timestamp()
    dt_object = datetime.datetime.fromtimestamp(timestamp)
    assert converter.set_type(dt_object, "date") is not None
    return True


if __name__ == "__main__":
    print(test_type_conversion())