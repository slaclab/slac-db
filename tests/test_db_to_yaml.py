import os
import slac_db
import slac_db.config
import slac_db.db_to_yaml
import unittest
import yaml


class test_db_to_yaml(unittest.TestCase):
    def test_compare_yaml(self):
        def get_yaml_area(location):
            with open(location, "r") as area_file:
                return yaml.safe_load(area_file)
        output = slac_db.db_to_yaml.write()
        del output["DMPH_1"]
        yaml_areas = sorted([
            a[:-5] for a in os.listdir(slac_db.config.yaml())
        ])
        db_areas = sorted([k for k in output.keys()])
        self.assertEqual(yaml_areas, db_areas)
        for a in db_areas:
            example = get_yaml_area(
                slac_db.get_yaml(area=a)
            )
            print(a)
            self.assertEqual(sorted(example.keys()), sorted(output[a].keys()))
