-- dbo schema
-- create schema if not exists dbo;

-- Logbooks
drop table if exists Logbooks;
create table Logbooks (
    LogbookId INTEGER PRIMARY KEY,
    LogbookName VARCHAR
);
insert into
    Logbooks
values
    (24, 'MCR Running Log'),
    (25, 'Beam Physics');

-- LogbookChapter
drop table if exists LogbookChapter;
create table LogbookChapter (
    LogbookChapterNo INTEGER PRIMARY KEY,
    LogbookId INTEGER
);
insert into
    LogbookChapter
values
    (1, 24),
    (2, 25),
    (3, 24);


-- ChapterEntry
drop table if exists ChapterEntry;
create table ChapterEntry (
    LogbookEntryId INTEGER PRIMARY KEY NOT NULL,
    EntryId INTEGER NOT NULL,
    PrincipalLogbook INTEGER,
    LogbookChapterNo INTEGER,
    LogbookId INTEGER
);
insert into
    ChapterEntry
values
    (1, 100, 24, 1, 24),
    (2, 101, 25, 2, 25),
    (3, 102, 24, 3, 24);


-- Entries
drop table if exists Entries;
create table Entries (
    EntryId INTEGER PRIMARY KEY NOT NULL,
    EntryTimestamp TIMESTAMP,
    AdditionalComment VARCHAR
);
insert into
  Entries
values
  (100, '2024-10-12 08:02:31.000', '<p>LTR 1 comment</p>'),
  (101, '2024-10-13 10:03:31.000', '<p>Beam physics comment</p>'),
  (102, '2024-11-14 14:06:31.000', '<p>LTR 2 comment</p>');

-- AdditionalColumns
drop table if exists AdditionalColumns;
create table AdditionalColumns(
  AdditionalColumnId INTEGER PRIMARY KEY NOT NULL,
  ColTitle VARCHAR
);
insert into
  AdditionalColumns
values
  (1, 'Group'),
  (2, 'Lost Time'),
  (3, 'Equipment'),
  (4, 'Group Leader comments');

-- MoreEntryColumns
drop table if exists MoreEntryColumns;
create table MoreEntryColumns(
    MoreEntryColumnId INTEGER PRIMARY KEY NOT NULL,
    EntryId INTEGER NOT NULL,
    ColData VARCHAR,
    NumberValue DOUBLE,
    AdditionalColumnId INTEGER
);
insert into
  MoreEntryColumns
values
    (1, 100, '3.7', 3.7, 2),
    (2, 100, 'Equipment A', null, 3),
    (3, 100, 'Group A', null, 1),
    (4, 102, '6.8', 6.8, 2),
    (5, 102, 'Equipment B', null, 3),
    (6, 102, 'Group B', null, 1),
    (7, 102, 'Group B Leader comment', null, 4);

-- LogbookEntryChanges
drop table if exists LogbookEntryChanges;
create table LogbookEntryChanges(
  ChangeId INTEGER PRIMARY KEY NOT NULL,
  ChangedDate TIMESTAMP,
  LogbookId INTEGER,
  ChangeType VARCHAR,
  EntryId INTEGER NOT NULL
);
insert into
    LogbookEntryChanges
values
    (1, '2024-10-12 08:02:31.000', 24, 'Entry created', 100),
    (2, '2024-10-13 10:03:31.000', 25, 'Entry created', 101),
    (3, '2024-11-14 14:06:31.000', 24, 'Entry created', 102);
