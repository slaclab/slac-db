import unittest
import slac_db.create.combined
import slac_db.device
import pykern.pkio
from pathlib import Path


test_data_path = Path(__file__).parent / 'test_data'

class test_combined(unittest.TestCase):
    def test_parser(self):
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
