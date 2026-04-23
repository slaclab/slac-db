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

        test = slac_db.db_to_yaml.write()
        yaml_areas = sorted([
            a[:-5] for a in os.listdir(slac_db.config.yaml())
        ])
        db_areas = sorted([k for k in test.keys()])
        self.assertEqual(yaml_areas, db_areas)

        for a in db_areas:
            test_devices = test[a]
            example_devices = get_yaml_area(slac_db.get_yaml(area=a))
            self.assertEqual(
                {a: sorted(test_devices.keys())},
                {a: sorted(example_devices.keys())}
            )
            for device_name, device in example_devices.items():
                self.assertEqual(
                    {(a, device_name): sorted(device)},
                    {(a, device_name): sorted(test_devices[device_name])}
                )

                
