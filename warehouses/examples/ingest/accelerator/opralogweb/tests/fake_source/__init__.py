"""Create a fake Opralog DB"""

import datetime as dt
import logging
from typing import Any

from sqlalchemy import (
    Engine,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import sessionmaker

from .opralogmodel import (
    ENTRYID_START,
    get_latest_entry_id,
    metadata as opralogdb,
    insert_empty_downtime_logbook,
    insert_downtime_record,
    update_downtime_record,
)

LOGGER = logging.getLogger(__name__)

MCR_LOG_NAME, MCR_LOG_ID = "MCR Running Log", 24


def migrate(engine: Engine, *, drop_all: bool = True) -> None:
    """Create the database, dropping tables first if requested"""
    if drop_all:
        LOGGER.info(f"Dropping tables in {engine.url}.")
        opralogdb().drop_all(engine)
    LOGGER.info(f"Creating tables in {engine.url}.")
    opralogdb().create_all(engine)


def create_and_fill(engine: Engine):
    """Create tables and populate with data"""
    migrate(engine, drop_all=True)

    LOGGER.info(f"Populating tables in {engine.url}.")
    session_maker = sessionmaker(engine)
    with session_maker() as session:
        insert_empty_downtime_logbook(session, MCR_LOG_NAME, MCR_LOG_ID)
        entry_id = ENTRYID_START
        for entry_num in range(1, 101):
            entry_id = insert_downtime_record(
                session,
                MCR_LOG_NAME,
                chapter_num=entry_num % 5,
                record={
                    "EntryTimestamp": dt.datetime.now(),
                    "AdditionalComment": f"Comment {entry_id}",
                    "Equipment": f"Equipment {entry_num}",
                    "Lost Time": 4.0 + 0.1 * entry_num,
                    "Group": f"Group {entry_num}",
                    "Group Leader comments": f"GL comment {entry_num}",
                },
            )
            entry_id += 1


def update_and_append_new_records(engine: Engine):
    """Alter existing entries and add new ones"""
    LOGGER.info(f"Updating and appending new records in {engine.url}.")

    session_maker = sessionmaker(engine)
    with session_maker() as session:
        latest_entry_id = get_latest_entry_id(session)
        if latest_entry_id is None:
            raise RuntimeError(
                "Entries table has no records. Fill table first before calling this function."
            )

        entry_id = ENTRYID_START + 10
        update_downtime_record(
            session,
            entry_id,
            record={
                "AdditionalComment": f"Comment {entry_id} updated.",
                "Lost Time": 3.5,
                "ChangedAt": dt.datetime.now(),
            },
        )
        entry_id = latest_entry_id + 1
        for entry_num in range(10):
            entry_id = insert_downtime_record(
                session,
                MCR_LOG_NAME,
                chapter_num=entry_num % 5,
                record={
                    "EntryTimestamp": dt.datetime.now(),
                    "AdditionalComment": f"Comment {entry_id}",
                    "Equipment": f"Equipment {entry_num}",
                    "Lost Time": 4.0 + 0.1 * entry_num,
                    "Group": f"Group {entry_num}",
                    "Group Leader comments": f"GL comment {entry_num}",
                },
            )
            entry_id += 1


def get_table_max_values(db_url: str) -> dict[str, dict[str, Any]]:
    """Return the maximum value of every column in every table."""
    engine = create_engine(db_url, echo=False)
    meta = opralogdb()
    result: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        for table in meta.sorted_tables:
            result[table.name] = {
                col.name: conn.execute(select(func.max(col))).scalar_one_or_none()
                for col in table.columns
            }
    return result


def create_fake_source_db(db_url: str):
    """Create a fake database stored at the given db_url, e.g. sqlite:///fake_db.sqlite

    Returns the maximum value of every column in every table after population.
    """
    engine = create_engine(db_url, echo=False)
    create_and_fill(engine)


def update_fake_source_db(db_url):
    """Updates a fake database stored at the given db_url, e.g. sqlite:///fake_db.sqlite"""
    update_and_append_new_records(create_engine(db_url, echo=False))
