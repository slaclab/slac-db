import unittest
import slac_db.dn_dict

class test_DNDict(unittest.TestCase):
    def test_add_get(self):
        d = slac_db.dn_dict.DNDict()
        d.add("TEST:0:EX1")
        d.add("TEST:0:EX2")
        d.add("TEST:1:EX1")
        self.assertEqual(len(d.get("TEST")), 3)
        self.assertEqual(len(d.get("TEST:0")), 2)
        self.assertEqual(len(d.get("TEST:0:EX1")), 1)
