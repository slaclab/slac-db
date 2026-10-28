import csv
import sys
import unittest
from datetime import datetime
import slac_db.config
from slac_db.create.lcls_elements import (
    _ORACLE_TO_REFERENCE,
    get_lcls_elements_csv,
)
# Assumes a web db and oracle db is downloaded in package_data
# These should be generated/downloaded on the same day.
package_data  = slac_db.config.package_data()
reference_csv = package_data.joinpath("lcls_elements.csv")
test_csv      = package_data.joinpath("test_lcls_elements.csv")

# Regenerate the curated csv directly from oracle, only works on production.
generate_tcsv = False
if generate_tcsv:
    get_lcls_elements_csv(csv_output=str(test_csv))


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


def _normalize_value(column, value):
    if column == "Sections":
        return value.removeprefix("\t")
    if column == "Op Status Date":
        if not value:
            return value
        if "/" in value:
            return datetime.strptime(value, "%m/%d/%Y")
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if column == "Element Comment":
        return value.replace("\r\n", "\n")
    return _num(value)

@unittest.skipUnless(
    reference_csv.exists() and test_csv.exists(),
    "Needs both lcls_elements.csv and test_lcls_elements.csv in package_data.",
)
def test_csv_matches_reference():

    _, built_rows     = _load(test_csv)
    _, reference_rows = _load(reference_csv)

    curated = list(_ORACLE_TO_REFERENCE.values())
    key = lambda r: (r["Area"], r["Element"])
    built_by_key = {key(r): r for r in built_rows}
    reference_by_key = {key(r): r for r in reference_rows}

    only_built = sorted(built_by_key.keys() - reference_by_key.keys())
    only_reference = sorted(reference_by_key.keys() - built_by_key.keys())
    assert not only_built, f"rows only in oracle export: {only_built[:10]}"
    assert not only_reference, f"rows only in web reference: {only_reference[:10]}"

    # Count value mismatches per column so a failure shows what diverged.
    col_diffs = {}
    for k in built_by_key:
        built, reference = built_by_key[k], reference_by_key[k]
        for c in curated:
            if _normalize_value(c, built[c]) != _normalize_value(c, reference[c]):
                col_diffs.setdefault(c, []).append((k, reference[c], built[c]))

    report = "\n".join(
        f"  {c}: {len(samples)} rows; e.g. {samples[:3]}"
        for c, samples in col_diffs.items()
    )
    assert not col_diffs, f"columns differing (key, web, oracle):\n{report}"


def summarize_elements_csv(path):
    # Summary used to check an export is not active-only before building DBs.
    header, data = _load(path)
    return {
        "columns": len(header),
        "rows": len(data),
        "active": sum(1 for r in data if r.get("Active") == "A"),
        "inactive": sum(1 for r in data if r.get("Active") == "I"),
        "areas": len({r["Area"] for r in data}),
    }


def find_element(path, element):
    _, data = _load(path)
    return [r for r in data if r.get("Element") == element]


@unittest.skipUnless(reference_csv.exists(), f"{reference_csv.name} not found.")
def test_csv_has_inactive_elements():
    summary = summarize_elements_csv(reference_csv)
    assert summary["inactive"] > 0, (
        f"{reference_csv.name} has no inactive (Active='I') devices; "
        "the export likely has an active-only filter applied."
    )


if __name__ == "__main__":
    # Quick report for a new export: `python tests/test_lcls_elements_csv.py <csv> [...]`
    paths = sys.argv[1:] or [str(reference_csv)]
    print(f"{'file':40} {'rows':>7} {'active':>7} {'inactive':>8} {'areas':>6}")
    for p in paths:
        s = summarize_elements_csv(p)
        name = p.rsplit("/", 1)[-1]
        print(f"{name:40} {s['rows']:>7} {s['active']:>7} {s['inactive']:>8} {s['areas']:>6}")
