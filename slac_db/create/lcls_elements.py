import csv
from sqlalchemy import text, create_engine
from contextlib import contextmanager
import slac_db.config
import slac_db.element_tables

_ORACLE_TNS      = 'slacprod' # name/connection of Oracle DB on prod
_ORACLE_USERNAME = 'lcls_read'

_ORACLE_TO_REFERENCE = {
    "area":                       "Area",
    "element":                    "Element",
    "epics_channel_access_name":  "Control System Name",
    "keyword":                    "Keyword",
    "beampath":                   "Beampath",
    "suml_m":                     "SumL (m)",
    "effective_length":           "Effective Length (m)",
    "rf_frequency":               "Rf Frequency (MHz)",
    "engineering_name":           "Engineering Name",
    "active_flag":                "Active",
}

@contextmanager
def get_connection():
    """Yield a connection to Oracle. Only works on production.
    Using as a context manager so the connection and engine are always cleaned up:
        with get_connection() as conn:
    """
    engine = create_engine(_get_remote_uri())
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


def _get_oracle_pw(username=_ORACLE_USERNAME):
    """Get Oracle password. This only works on production.
    """
    try:
        import subprocess
        cmd      = subprocess.run(['getPwd', username], capture_output=True, text=True, check=True)
        password = cmd.stdout.strip()
        return password
    except Exception as e:
        print(f"Could not get Oracle password: {e}")
        return None

def _get_remote_uri():
    """Get string needed to connect to Oracle remotely.
    """
    password  = _get_oracle_pw(_ORACLE_USERNAME)
    connection_string = f'oracle+cx_oracle://{_ORACLE_USERNAME}:{password}@{_ORACLE_TNS}'
    return connection_string


def get_oracle_elements_csv(csv_output='oracle_elements.csv'):
    """Get a csv file from Oracle that has all devices in LCLS_ELEMENTS table.
    This function only works on production.

    Args:
        csv_output: Name of the output csv file.
    """
    import pandas as pd
    sql_query = text("select * from lcls_infrastructure.V_LCLS_ELEMENTS_DIAG")
    try:
        with get_connection() as connection:
            df = pd.read_sql(sql_query, connection)
            df.to_csv(csv_output, index=False)
    except Exception as e:
        print(f"An error occurred {e}")
        raise


def build_lcls_elements_csv(oracle_csv, lcls_csv):
    """Convert oracle csv into the curated lcls_elements.csv format."""
    try:
        with open(oracle_csv, newline="") as input_csv, open(lcls_csv, "w", newline="") as output_csv:
            keymap = _ORACLE_TO_REFERENCE
            reader = csv.DictReader(input_csv)
            writer = csv.writer(output_csv)
            writer.writerow(keymap.values())
            for row in reader:
                writer.writerow(row[c] for c in keymap)
    except Exception as e:
        print(f"Unable to build the LCLS elements csv: {e}")
        raise

def get_lcls_elements_csv(oracle_csv='oracle_elements.csv', lcls_csv='lcls_elements.csv'):
    """Get oracle csv on production, then convert to lcls_element.csv needed for this repo."""
    get_oracle_elements_csv(csv_output=oracle_csv)
    build_lcls_elements_csv(oracle_csv, lcls_csv)


def to_oracle_db(csv_source=None):
    """ Build  oracle DB with SQLAlchemy.

    Args:
        csv_source: Location of Oracle CSV file
    """
    p = _Parser(csv_source=csv_source)
    return slac_db.element_tables.recreate(p)

class _Parser():
    """Container for DB row data.
    """
    def __init__(self, csv_source=None):
        if not csv_source:
            csv_source = (
                slac_db.config.package_data() / "lcls_elements.csv"
            )
        self.rows = {}
        with open(csv_source, "r") as c:
            reader = csv.reader(c)
            self._parse_csv(reader)

    def _parse_csv(self, reader):
        next(reader)  # skip group header row
        names = [r.lower() for r in next(reader)]
        i = 0
        # Track station names already recorded for KLYS sub-cavity dedup.
        # Maps station_name -> index in self.rows for the canonical row.
        # Keyed on the stripped element name (e.g. "K21_5") so that dirty
        # cs_name data (truncated or transposed digits in sectors 12-19)
        # cannot produce duplicate station entries.
        _klys_seen = {}
        for row in reader:
            values = [None if v == '' else v for v in row]
            d = dict(zip(names, values))
            element = d.get("element") or ""
            cs_name = d.get("control system name") or ""
            keyword = d.get("keyword") or ""
            # Deduplicate klystron sub-cavities (K21_5A/B/C/D -> K21_5).
            # A sub-cavity row is an LCAV whose element ends in A-D and whose
            # cs_name contains "KLYS" (handles both "KLYS:LI{s}:{n}1" and
            # legacy reversed form "LI{s}:KLYS:{n}1" used in sectors 12-19).
            # Dedup key is the station name (element minus trailing letter) so
            # that cs_name inconsistencies in the source data don't create
            # duplicate entries (e.g. K12_3A has cs LI12:KLYS:3 while
            # K12_3B/C/D have LI12:KLYS:31).
            if (
                keyword == "LCAV"
                and "KLYS" in cs_name
                and len(element) > 1
                and element[-1] in "ABCD"
            ):
                station = element[:-1]  # e.g. "K21_5A" -> "K21_5"
                if station in _klys_seen:
                    # Already have a row for this station; skip this sub-cavity.
                    continue
                # First time seeing this station: record it and rename element.
                d["element"] = station
                _klys_seen[station] = i
            self.rows[i] = d
            i += 1
