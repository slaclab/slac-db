import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slac_db import create, db_to_yaml

start_time = time.time()

if __name__ == "__main__":
    #must be done on production
    #TODO: add csv grab from oracle
    create.directory_service_db()

    #can be done locally with new csv + directory service db:
    #currently takes ~20s
    create.oracle_db()
    create.device_db()
    db_to_yaml.write()

print("--- %s seconds ---" % (time.time() - start_time))


