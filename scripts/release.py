import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slac_db import create, db_to_yaml

start_time = time.time()

if __name__ == "__main__":
    create.directory_service_db()
    create.oracle_db()
    create.device_db()
    db_to_yaml.write()
    #slac_db.write.YAMLWriter(use_meme=False, suppress_warnings=True)

print("--- %s seconds ---" % (time.time() - start_time))


