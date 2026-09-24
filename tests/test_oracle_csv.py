import csv

import slac_db.config
from slac_db.create.lcls_elements import get_lcls_elements_csv

package_data  = slac_db.config.package_data()
reference_csv = package_data / "lcls_elements.csv"
test_csv      = package_data / "test_lcls_elements.csv"

# Regenerate the csv from Oracle, only works on production.
generate_tcsv = False
if generate_tcsv:
    get_lcls_elements_csv(csv_output=str(test_csv))


def _load(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [dict(zip(header, row)) for row in reader]
    return header, rows


def test_csv_matches_reference():
    test_header, test_rows           = _load(test_csv)
    reference_header, reference_rows = _load(reference_csv)

    assert set(test_header) == set(reference_header)
    assert test_rows == reference_rows
