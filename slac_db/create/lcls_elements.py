import csv
from sqlalchemy import text, create_engine
from contextlib import contextmanager
import slac_db.config
import slac_db.element_tables

ORACLE_TO_YAML_TYPE_MAP = {
    "SOLE": "magnet",
    "QUAD": "magnet",
    "XCOR": "magnet",
    "YCOR": "magnet",
    "BEND": "magnet",
    "PROF": "screen",
    "WIRE": "wire",
    "LBLM": "lblm",
    "BPM": "bpm",
    "LCAV": "tcav",
    "TCAV": "tcav",
    "INST": "pmt",
    "IMON": "toroid",
}

_ORACLE_TNS      = 'slacprod' # name/connection of Oracle DB on prod
_ORACLE_USERNAME = 'lcls_read'
# sql query to get only active devices
_ORACLE_ACTIVE_QUERY = "select * from lcls_infrastructure.V_LCLS_ELEMENTS_DIAG"

# Mapping oracle column (V_LCLS_ELEMENTS_REPORT) -> web headers.
# Order matches the web lcls_elements.csv so the query output is drop-in
# interchangeable with the web export. Only the 10 Title Case headers the
# downstream SQLite schema reads matter functionally; the rest are carried for
# web-dump parity. "area2" and "filename" have no Oracle source (see
# _NO_ORACLE_SOURCE); "beampath" is built by the pathway join.
_ORACLE_TO_REFERENCE = {
    "element":                      "Element",
    "area":                         "Area",
    "element_type":                 "Type",
    "epics_channel_access_name":    "Control System Name",
    "area2":                        "Area2",
    "component_name":               "Component Name",
    "component_comments":           "Component Comments",
    "slc_name":                     "Slc Name",
    "symbols_upload_id":            "Symbols Upload Id",
    "ltu_upload_id":                "Ltu Upload Id",
    "first_sourced_from":           "First Sourced From",
    "active_flag":                  "Active",
    "filename":                     "Filename",
    "sector":                       "Sector",
    "sector_location":              "Sector Location",
    "undulator_cell":               "Undulator Cell",
    "keyword":                      "Keyword",
    "barcode":                      "Barcode",
    "engineering_name":             "Engineering Name",
    "suml_m":                       "SumL (m)",
    "suml_ft":                      "SumL (ft)",
    "linacz_m":                     "Linac Z (m)",
    "linacz_ft":                    "Linac Z (ft)",
    "ltu_suml_m":                   "Ltu SumL (m)",
    "ltu_suml_ft":                  "Ltu SumL (ft)",
    "ltu_linacz_m":                 "Ltu Linac Z (m)",
    "ltu_linacz_ft":                "Ltu Linac Z (ft)",
    "drawing_no":                   "Local Area Drawing",
    "irmis_ioc_names":              "Irmis Ioc Names",
    "element_comment":              "Element Comment",
    "sections":                     "Sections",
    "begin_dbmark":                 "Begin Dbmark",
    "begin_suml":                   "Begin SumL (m)",
    "end_dbmark":                   "End Dbmark",
    "end_suml":                     "End SumL (m)",
    "bsa_flag":                     "Bsa Device",
    "obstruction":                  "Obstruction",
    "effective_length":             "Effective Length (m)",
    "rf_frequency":                 "Rf Frequency (MHz)",
    "rf_amplitude":                 "Rf Amplitude (MeV)",
    "rf_phase":                     "Rf Phase (deg)",
    "rf_gradient":                  "Rf Gradient (MeV/m)",
    "rf_power_fraction":            "Rf Power Fraction",
    "z_length":                     "Z Length (m)",
    "fringe_field_integral":        "Fringe Field Integral",
    "integrated_field_bl":          "Integrated Field BL (kG-m)",
    "field_b":                      "Field B (T)",
    "integrated_field_grad_gl":     "Integrated Field Grad GL (kG)",
    "field_gradient_g":             "Field Gradient G (T/m)",
    "xal_scale_name":               "Xal Scale Name",
    "xal_scale_value":              "Xal Scale Value",
    "xal_polarity":                 "Xal Polarity",
    "magnet_x_coor":                "Magnet X Coor (m)",
    "magnet_y_coor":                "Magnet Y Coor (m)",
    "magnet_z_coor":                "Magnet Z Coor (m)",
    "magnet_x_angle":               "Magnet X Angle (rad)",
    "magnet_y_angle":               "Magnet Y Angle (rad)",
    "magnet_z_angle":               "Magnet Z Angle (rad)",
    "solenoid_strength_ks":         "Solenoid Strength KS (1/m)",
    "undulator_period_length":      "Undulator Period Length (m)",
    "undulator_strength_k":         "Undulator Strength K",
    "x_size":                       "X Size (m)",
    "y_size":                       "Y Size (m)",
    "section":                      "Section",
    "dist_from_section_start":      "Dist From Section Start (m)",
    "xal_keyword":                  "Xal Keyword",
    "s_display":                    "S Display",
    "ltu_rf_frequency":             "Ltu Rf Frequency (MHz)",
    "ltu_rf_amplitude":             "Ltu Rf Amplitude (MeV)",
    "ltu_rf_phase":                 "Ltu Rf Phase (deg)",
    "ltu_rf_gradient":              "Ltu Rf Gradient (MeV/m)",
    "ltu_rf_power_fraction":        "Ltu Rf Power Fraction",
    "ltu_z_length":                 "Ltu Z Length (m)",
    "ltu_fringe_field_integral":    "Ltu Fringe Field Integral",
    "ltu_integrated_field_bl":      "Ltu Integrated Field BL (kG-m)",
    "ltu_field_b":                  "Ltu Field B (T)",
    "ltu_integrated_field_grad_gl": "Ltu Integrated Field Grad GL (kG)",
    "ltu_field_gradient_g":         "Ltu Field Gradient G (T/m)",
    "ltu_xal_scale_name":           "Ltu Xal Scale Name",
    "ltu_xal_scale_value":          "Ltu Xal Scale Value",
    "ltu_xal_polarity":             "Ltu Xal Polarity",
    "ltu_magnet_x_coor":            "Ltu Magnet X Coor",
    "ltu_magnet_y_coor":            "Ltu Magnet Y Coor",
    "ltu_magnet_z_coor":            "Ltu Magnet Z Coor",
    "ltu_magnet_x_angle":           "Ltu Magnet X Angle",
    "ltu_magnet_y_angle":           "Ltu Magnet Y Angle",
    "ltu_magnet_z_angle":           "Ltu Magnet Z Angle",
    "ltu_solenoid_strength_ks":     "Ltu Solenoid Strength Ks",
    "ltu_undulator_period_length":  "Ltu Undulator Period Length",
    "ltu_undulator_strength_k":     "Ltu Undulator Strength K",
    "ltu_x_size":                   "Ltu X Size",
    "ltu_y_size":                   "Ltu Y Size",
    "ltu_section":                  "Ltu Section",
    "ltu_dist_from_section_start":  "Ltu Dist From Section Start",
    "ltu_xal_keyword":              "Ltu Xal Keyword",
    "ltu_s_display":                "Ltu S Display",
    "ioc_captar_search":            "Ioc Captar Search",
    "slc_captar_search":            "Slc Captar Search",
    "ops_status":                   "Operation Status",
    "ops_status_date":              "Op Status Date",
    "beampath":                     "Beampath",
}

# Web columns with no V_LCLS_ELEMENTS_REPORT source; set as NULL.
_NO_ORACLE_SOURCE = {"area2", "filename"}

def _select_expr(col):
    # beampath comes from the pathway join; unsourced columns are NULL; rest from t.
    if col == "beampath":
        return "path.pathname"
    if col in _NO_ORACLE_SOURCE:
        return "NULL"
    return f"t.{col}"

_SELECT_COLUMNS = ",\n  ".join(
    f'{_select_expr(col)} AS "{header}"'
    for col, header in _ORACLE_TO_REFERENCE.items()
)

# sql query for curated lcls_elements.csv with inactive devices kept.
# The nested join builds the beampath; the (+) outer joins keep elements with no beampath.
_ORACLE_QUERY = f"""
SELECT
  {_SELECT_COLUMNS}
FROM V_LCLS_ELEMENTS_REPORT t,
     (SELECT u.element_id, u.area2, p.pathname
      FROM lcls_elements_upd u,
           (SELECT areaname,
                   LISTAGG(pathname || ', ') WITHIN GROUP (ORDER BY pathname) AS pathname
            FROM lcls_pathway
            GROUP BY areaname) p
      WHERE u.upload_id = (SELECT MAX(upload_id) FROM lcls_elements_upd)
        AND u.area2 = p.areaname (+)) path
WHERE t.element_id = path.element_id (+)
  AND t.element_type = 'MAD'
"""

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


def get_oracle_elements_csv(oracle_csv='oracle_elements.csv', sql=_ORACLE_QUERY):
    """Dump an Oracle query to csv. This function only works on production.

    With the default _ORACLE_QUERY the output columns match the
    lcls_elements.csv columns (101 web columns, inactive devices included).
	
    Args:
        oracle_csv: Name of the output csv file.
        sql: Query to run. Defaults to _ORACLE_QUERY.
    """

    #TODO: add code/confirm bare snake case oracle dump works.
    try:
        with get_connection() as connection, \
            open(oracle_csv, "w", newline="") as output_csv:
            result = connection.execute(sql)
            writer = csv.writer(output_csv)
            writer.writerow(result.keys())
            for row in result:
                writer.writerow("" if v is None else str(v) for v in row)
    except Exception as e:
        print(f"An error occurred {e}")
        raise


def get_lcls_elements_csv(csv_output="lcls_elements.csv"):
    """Get the lcls_elements.csv file from Oracle.
    This function only works on production.
    """

    get_oracle_elements_csv(oracle_csv=csv_output)
    # TODO: add code here to change formatting to match web output
    return
    
def to_oracle_db(csv_source=None):
    """Build  oracle DB with SQLAlchemy.

    Args:
        csv_source: Location of Oracle CSV file
    """
    p = _Parser(csv_source=csv_source)
    return slac_db.element_tables.recreate(p)


class _Parser:
    """Container for DB row data."""

    def __init__(self, csv_source=None):
        if not csv_source:
            csv_source = slac_db.config.package_data().joinpath("lcls_elements.csv")
        self.rows = []
        with open(csv_source, "r") as c:
            reader = csv.reader(c)
            self._parse_csv(reader)

    def _parse_csv(self, reader):
        next(reader)  # skip group header row
        header = next(reader)
        index = {r.lower(): i for r, i in zip(header, range(len(header)))}
        wanted_cols = [
            k.lower()
            for k in slac_db.element_tables.schema["elements"].keys()
            if k != "yaml_type"
        ]

        def get_col(row, col):
            return row[index[col]]

        _klys_seen = set()
        for row in reader:
            d = {c: get_col(row, c) or None for c in wanted_cols}
            d["yaml_type"] = ORACLE_TO_YAML_TYPE_MAP.get(d["keyword"], None)
            cs_name = d["control system name"] or ""
            element = d["element"] or ""
            keyword = d["keyword"] or ""
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
                _klys_seen.add(station)
            self.rows.append(d)
