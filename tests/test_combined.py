import unittest
import slac_db.create.combined
import slac_db.device
import slac_db.io
import pykern.pkio
from pathlib import Path


test_data_path = Path(__file__).parent / 'test_data'

class test_combined(unittest.TestCase):
    def test_accessor_parser(self):
        p = slac_db.create.combined._Parser()
        value = p.accessor_map["CQ01B"]
        expected = slac_db.io.read_dict(
            test_data_path / "CQ01B_accessors.yaml"
        )
        self.assertEqual(value, expected)
    
    def test_address_parser(self):
        p = slac_db.create.combined._Parser()
        value = p.address_map["OTRS:DIAG0:420"]
        expected = pykern.pkio.read_text(
            test_data_path / "OTRDG02_names.txt"
        ).splitlines()
        self.assertEqual(len(value), len(expected))

    def test_db(self):
        value = slac_db.device.get_all_addresses("OTRDG02")
        expected = pykern.pkio.read_text(
            test_data_path / "OTRDG02_names.txt"
        ).splitlines()
        self.assertEqual(len(value), len(expected))
