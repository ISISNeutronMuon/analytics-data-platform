-- dbo schema
create schema if not exists dbo;

-- dbo.Logbooks
drop table if exists dbo.Logbooks;
create table dbo.Logbooks (
    LogbookId INTEGER PRIMARY KEY,
    LogbookName VARCHAR
);
insert into
    dbo.Logbooks
values
    (24, 'MCR Running Log'),
    (25, 'Beam Physics');

-- dbo.LogbookChapter
drop table if exists dbo.LogbookChapter;
create table dbo.LogbookChapter (
    LogbookChapterNo INTEGER PRIMARY KEY,
    LogbookId INTEGER
);
insert into
    dbo.LogbookChapter
values
    (1, 24),
    (2, 25),
    (3, 24);


-- dbo.ChapterEntry
drop table if exists dbo.ChapterEntry;
create table dbo.ChapterEntry (
    LogbookEntryId INTEGER PRIMARY KEY,
    EntryId INTEGER,
    PrincipalLogbook INTEGER,
    LogbookChapterNo INTEGER,
    LogbookId INTEGER,
);
insert into
    dbo.ChapterEntry
values
    (1, 100, 24, 1, 24),
    (2, 101, 25, 2, 25),
    (3, 102, 24, 3, 24);


-- dbo.Entries
drop table if exists dbo.Entries;
create table dbo.Entries (
    EntryId INTEGER PRIMARY KEY,
    EntryTimestamp TIMESTAMP,
    AdditionalComment VARCHAR
);
insert into
  dbo.Entries
values
  (100, '2024-10-12 08:02:31.000', '<p>LTR 1 comment</p>'),
  (101, '2024-10-13 10:03:31.000', '<p>Beam physics comment</p>'),
  (102, '2024-11-14 14:06:31.000', '<p>LTR 2 comment</p>');

-- dbo.AdditionalColumns
drop table if exists dbo.AdditionalColumns;
create table dbo.AdditionalColumns(
  AdditionalColumnId INTEGER PRIMARY KEY,
  ColTitle VARCHAR
);
insert into
  dbo.AdditionalColumns
values
  (1, 'Group'),
  (2, 'Lost Time'),
  (3, 'Equipment'),
  (4, 'Group Leader comments');

-- dbo.MoreEntryColumns
drop table if exists dbo.MoreEntryColumns;
create table dbo.MoreEntryColumns(
    MoreEntryColumnId INTEGER PRIMARY KEY,
    EntryId INTEGER,
    ColData VARCHAR,
    NumberValue DOUBLE,
    AdditionalColumnId INTEGER
);
insert into
  dbo.MoreEntryColumns
values
    (1, 100, '3.7', 3.7, 2),
    (2, 100, 'Equipment A', null, 3),
    (3, 100, 'Group A', null, 1),
    (4, 102, '6.8', 6.8, 2),
    (5, 102, 'Equipment B', null, 3),
    (6, 102, 'Group B', null, 1),
    (7, 102, 'Group B Leader comment', null, 4);
