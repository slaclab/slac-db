#!/usr/bin/env python3
"""Export Oracle LCLS elements csv using the source tree without installing slac-db."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slac_db.create.lcls_elements import get_lcls_elements_csv


if __name__ == "__main__":
    output = Path(__file__).resolve().parents[1].joinpath(
        "slac_db", "package_data", "test_lcls_elements.csv"
    )
    get_lcls_elements_csv(csv_output=output)
