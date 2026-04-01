import unittest
import slac_db.directory_service

class TestDirectoyrService(unittest.TestCase):
    def test_get_otrdg02_pvs(self):
        all_pvs = slac_db.directory_service.get_addresses(
            device="OTRDG04",
        )
        num_pvs = 2838
        self.assertEqual(len(all_pvs), num_pvs)
