"""Define tables that look like Opralog tables. They contain only the data we are interested in."""

import datetime as dt
import logging

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Text,
    and_,
)
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.sql import select
import sqlalchemy.sql.functions as sqlf

LOGGER = logging.getLogger(__name__)

# Global database schema
# SQLAlchemy recommends keeping this as a module-level variable
_METADATA = MetaData()

# Base class for ORM mapped classes
ModelBase = declarative_base(metadata=_METADATA)

ENTRYID_START = 100
# See stg_opralogweb__mcr_equipment_downtime to match names
ADDITIONAL_COLUMNS = {
    "Equipment": 1,
    "Group": 2,
    "Lost Time": 3,
    "Group Leader comments": 4,
}
LOGBOOKS = {"MCR Running Log": 24, "Beam Physics": 25}


def metadata() -> MetaData:
    return _METADATA


def commit_or_rollback(func):
    """Decorator to either commit a successful ORM session or rollback on exception"""

    def impl(session: Session, *args, **kwargs):
        try:
            return func(session, *args, **kwargs)
        except Exception:
            session.rollback()
            raise
        finally:
            session.commit()

    return impl


class Logbooks(ModelBase):
    __tablename__ = "Logbooks"

    LogbookId = Column(Integer, primary_key=True, nullable=False)
    LogbookName = Column(String, nullable=False)


class LogbookChapter(ModelBase):
    __tablename__ = "LogbookChapter"

    LogbookChapterNo = Column(Integer, primary_key=True, nullable=False)
    LogbookId = Column(Integer, nullable=False)


class ChapterEntry(ModelBase):
    __tablename__ = "ChapterEntry"

    LogbookEntryId = Column(Integer, primary_key=True, nullable=False)
    EntryId = Column(Integer, nullable=False)
    PrincipalLogbook = Column(Integer, nullable=False)
    LogbookChapterNo = Column(Integer, nullable=False)
    LogbookId = Column(Integer, nullable=False)


class Entries(ModelBase):
    __tablename__ = "Entries"

    EntryId = Column(Integer, primary_key=True, nullable=False)
    EntryTimestamp = Column(DateTime, nullable=False)
    AdditionalComment = Column(Text)
    LastChangedDate = Column(DateTime, nullable=False)


class AdditionalColumns(ModelBase):
    __tablename__ = "AdditionalColumns"

    AdditionalColumnId = Column(Integer, primary_key=True, nullable=False)
    ColTitle = Column(String)


class MoreEntryColumns(ModelBase):
    __tablename__ = "MoreEntryColumns"

    MoreEntryColumnId = Column(Integer, primary_key=True, nullable=False)
    EntryId = Column(Integer, nullable=False)
    ColData = Column(String)
    NumberValue = Column(Float)
    AdditionalColumnId = Column(Integer)


class LogbookEntryChanges(ModelBase):
    __tablename__ = "LogbookEntryChanges"

    ChangeId = Column(Integer, primary_key=True, nullable=False)
    ChangedDate = Column(DateTime)
    LogbookId = Column(Integer)
    ChangeType = Column(String)
    EntryId = Column(Integer, nullable=False)


def get_latest_entry_id(session: Session) -> int | None:
    """Find the latest Entries.EntryId if one exists"""
    return session.execute(select(sqlf.max(Entries.EntryId))).scalar_one_or_none()


@commit_or_rollback
def insert_empty_downtime_logbook(session: Session, name: str, logbook_id: int):
    """Insert an empty logbook with the given name.

    Inserts needed rows in all tables to setup a downtime record logbook
    Raises an exception if one with this name exists
    """
    LOGGER.debug(f"Inserting empty downtime logbook: {name}")
    session.add(Logbooks(LogbookId=logbook_id, LogbookName=name))
    for col_title, col_id in ADDITIONAL_COLUMNS.items():
        session.add(AdditionalColumns(AdditionalColumnId=col_id, ColTitle=col_title))


@commit_or_rollback
def insert_downtime_record(
    session: Session, logbook_name: str, chapter_num: int, record: dict
) -> int:
    """Wrapper to insert the relevant records into the tables to make a new logbook entry"""
    logbook_id = session.execute(
        select(Logbooks.LogbookId).where(Logbooks.LogbookName == logbook_name)
    ).scalar_one()

    latest_entry = get_latest_entry_id(session)
    if latest_entry is None:
        next_entry_id = ENTRYID_START
    else:
        next_entry_id = latest_entry + 1

    next_entry = Entries(
        EntryId=next_entry_id,
        EntryTimestamp=record["EntryTimestamp"],
        AdditionalComment=f"<p>{record['AdditionalComment']}</p>",
        LastChangedDate=record["EntryTimestamp"],
    )
    session.add(next_entry)
    session.add_all(
        [
            MoreEntryColumns(
                EntryId=next_entry_id,
                AdditionalColumnId=ADDITIONAL_COLUMNS[col_title],
                ColData=record[col_title],
            )
            for col_title in ("Group", "Equipment", "Group Leader comments")
        ]
        + [
            MoreEntryColumns(
                EntryId=next_entry_id,
                AdditionalColumnId=ADDITIONAL_COLUMNS["Lost Time"],
                NumberValue=record["Lost Time"],
            )
        ]
    )
    # insert chapter non existent
    if (
        session.execute(
            select(LogbookChapter.LogbookChapterNo).where(
                LogbookChapter.LogbookChapterNo == chapter_num
            )
        ).scalar_one_or_none()
        is None
    ):
        session.add(LogbookChapter(LogbookChapterNo=chapter_num, LogbookId=logbook_id))

    # tie chapter and entry
    next_entry_id = next_entry.EntryId
    session.add(
        ChapterEntry(
            EntryId=next_entry_id,
            PrincipalLogbook=logbook_id,
            LogbookChapterNo=chapter_num,
            LogbookId=logbook_id,
        )
    )

    # insert a changelog entry
    session.add(
        LogbookEntryChanges(
            ChangedDate=record["EntryTimestamp"],
            LogbookId=logbook_id,
            ChangeType="Entry modified",
            EntryId=next_entry_id,
        )
    )

    return next_entry_id  # type: ignore


@commit_or_rollback
def update_downtime_record(
    session: Session, logbook_name: str, entry_id: int, record: dict
):
    """Wrapper to insert the relevant records into the tables to make a new logbook entry"""
    # Make edits
    entry = session.execute(
        select(Entries).where(Entries.EntryId == entry_id)
    ).scalar_one()
    entry.AdditionalComment = f"<p>{record['AdditionalComment']}</p>"  # type: ignore
    entry.LastChangedDate = record["ChangedAt"]

    more_entry_columns = session.execute(
        select(MoreEntryColumns).where(
            and_(
                MoreEntryColumns.EntryId == entry_id,
                MoreEntryColumns.AdditionalColumnId == ADDITIONAL_COLUMNS["Lost Time"],
            )
        )
    ).scalar_one()
    more_entry_columns.NumberValue = record["Lost Time"]

    # Record in change log
    logbook_id = session.execute(
        select(Logbooks.LogbookId).where(Logbooks.LogbookName == logbook_name)
    ).scalar_one()

    session.add(
        LogbookEntryChanges(
            ChangedDate=dt.datetime.now(),
            LogbookId=logbook_id,
            ChangeType="Entry modified",
            EntryId=entry_id,
        )
    )
