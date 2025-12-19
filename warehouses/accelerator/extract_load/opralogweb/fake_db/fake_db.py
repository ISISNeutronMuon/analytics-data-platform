#!/usr/bin/env python
"""Create a fake Opralog DB"""

import click
import datetime as dt
import logging

from sqlalchemy import (
    Engine,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

from opralogmodel import (
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
            MCR_LOG_NAME,
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


@click.command("fake_db")
@click.argument("db-url")
@click.option(
    "--simulate-updates",
    is_flag=True,
    help="If supplied, alter old records and add new ones to mirror application updates.",
)
def main(db_url, simulate_updates: bool):
    """Create a fake database stored at the given DB url, e.g. sqlite:///fake_db.sqlite"""
    logging.basicConfig(level=logging.INFO)

    engine = create_engine(db_url, echo=False)
    if simulate_updates:
        update_and_append_new_records(engine)
    else:
        create_and_fill(engine)


if __name__ == "__main__":
    main()
