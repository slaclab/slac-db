import csv
import unittest
import slac_db.config
from slac_db.create.lcls_elements import (
    _ORACLE_TO_REFERENCE,
    build_lcls_elements_csv,
    get_oracle_elements_csv,
)
# Assumes a web db and oracle db is downloaded in package_data
# These should be generated/downloaded on the same day.
package_data  = slac_db.config.package_data()
reference_csv = package_data.joinpath("web_lcls_elements.csv")
oracle_csv    = package_data.joinpath("oracle_lcls_elements.csv")
test_csv      = package_data.joinpath("test_lcls_elements.csv")

# Regenerate the csv from oracle, only works on production.
generate_tcsv = False
if generate_tcsv:
    get_oracle_elements_csv(csv_output=str(oracle_csv))


def _load(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [dict(zip(header, row)) for row in reader]
    return header, rows


def _num(v):
    # Normalize numeric strings so web download oracle numbers match.
    # Leading and trailing zeros are ignored when comparing numeric values.
    # i.e. 0.0 and 0 are considered equal.
    try:
        return float(v)
    except (ValueError, TypeError):
        return v

@unittest.skipUnless(generate_tcsv, "Skipping test on workflow, this is manual currently.")
def test_csv_matches_reference():
    build_lcls_elements_csv(str(oracle_csv), str(test_csv))

    _, built_rows     = _load(test_csv)
    _, reference_rows = _load(reference_csv)

    curated = list(_ORACLE_TO_REFERENCE.values())
    key = lambda r: (r["Area"], r["Element"])

    def norm(rows):
        return sorted(
            ({c: _num(r[c]) for c in curated} for r in rows),
            key=key,
        )

    assert norm(built_rows) == norm(reference_rows)
