import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slac_db.db_to_yaml import build

if __name__ == "__main__":
    build()
