"""Create seed a fake Opralog DB (web-based version)"""

import datetime as dt
import logging
import sys

from sqlalchemy import (
    Engine,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

from opralog_model import (
    metadata as opralogdb,
    insert_empty_downtime_logbook,
    insert_downtime_record,
)

LOGGER = logging.getLogger(__name__)


def migrate(engine: Engine, *, drop_all: bool = True) -> None:
    """Create the database, dropping tables first if requested"""
    if drop_all:
        LOGGER.info(f"Dropping tables in {engine.url}.")
        opralogdb().drop_all(engine)
    LOGGER.info(f"Creating tables in {engine.url}.")
    opralogdb().create_all(engine)


def main():
    logging.basicConfig(level=logging.INFO)

    db_url = sys.argv[1]
    engine = create_engine(db_url, echo=False)

    migrate(engine, drop_all=True)
    session_maker = sessionmaker(engine)
    mcr_log_name, mcr_log_id = "MCR Running Log", 24
    with session_maker() as session:
        insert_empty_downtime_logbook(session, mcr_log_name, mcr_log_id)
        for entry_num in range(1, 101):
            insert_downtime_record(
                session,
                mcr_log_name,
                chapter_num=entry_num % 5,
                record={
                    "EntryTimestamp": dt.datetime.now(),
                    "AdditionalComment": f"Comment {entry_num}",
                    "Equipment": f"Equipment {entry_num}",
                    "Lost Time": 4.0 + 0.1 * entry_num,
                    "Group": f"Group {entry_num}",
                    "Group Leader comments": f"GL comment {entry_num}",
                },
            )


if __name__ == "__main__":
    main()
