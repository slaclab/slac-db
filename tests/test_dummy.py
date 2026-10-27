import pytest
import slac_db.create
import slac_db.device


@pytest.fixture
def setup_data():
    prod_path = slac_db.device._db_path
    slac_db.device._meta = None
    slac_db.create.dummy_db()
    yield
    slac_db.device._meta = None
    slac_db.device._db_path = prod_path

def test_dev_cameras(setup_data):
    slac_db.device._meta = None

    assert slac_db.device.get_all_areas() == ["DEV_AREA"]
    assert slac_db.device.build_device("DEV_CAMERA") == {
        'device_name': 'DEV_CAMERA',
        'area': 'DEV_AREA',
        'device_type': 'PROF',
        'cs_name': '13SIM1',
        'is_active': True,
        'sum_l_meters': 0.614,
        'accessor': {
            'acquire': '13SIM1:cam1:Acquire',
            'image': '13SIM1:image1:ArrayData',
            'n_bits': '13SIM1:cam1:N_OF_BITS',
            'n_col': '13SIM1:cam1:SizeX',
            'n_row': '13SIM1:cam1:SizeY',
        },
    }
    slac_db.device._meta = None
