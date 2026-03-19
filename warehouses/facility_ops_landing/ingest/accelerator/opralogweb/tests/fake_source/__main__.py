from . import create_fake_source_db, update_fake_source_db
import sys

db_url = sys.argv[1]
if len(sys.argv) == 3:
    update_fake_source_db(db_url)
else:
    create_fake_source_db(db_url)
