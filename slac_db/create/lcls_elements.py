import csv
from sqlalchemy import create_engine, text
import slac_db.config
import slac_db.oracle

_ORACLE_TNS      = 'slacprod' # name of Oracle DB on prod
_ORACLE_USERNAME = 'lcls_read'

def get_lcls_elements_csv(csv_output='lcls_elements.csv'):
    """Get the lcls_elements.csv file from Oracle. 
    This function only works on production.
    
    Args:
        csv_output: Name of the output csv file.
    """
    password  = _get_oracle_pw(_ORACLE_USERNAME)
    connection_string = f'oracle+cx_oracle://{_ORACLE_USERNAME}:{password}@{_ORACLE_TNS}'
    engine    = create_engine(connection_string)
    sql_query = text("select * from lcls_infrastructure.V_LCLS_ELEMENTS_DIAG")

    try:
        with engine.connect() as connection:
            import pandas as pd
            df = pd.read_sql(sql_query, connection)
            df.to_csv(csv_output, index=False)

    except Exception as e:
        print(f"An error occurred: {e}")

    engine.dispose()
    return None


def to_oracle_db(csv_source=None):
    """ Build  oracle DB with SQLAlchemy.

    Args:
        csv_source: Location of Oracle CSV file
    """
    p = _Parser(csv_source=csv_source)
    return slac_db.oracle.recreate(p)

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
        names = [r.lower() for r in next(reader)]
        i = 0
        for row in reader:
            values = [None if v == '' else v for v in row]
            self.rows[i] =  dict(zip(names, values))
            i += 1

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

