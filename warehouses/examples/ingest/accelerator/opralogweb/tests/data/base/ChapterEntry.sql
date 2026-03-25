CREATE TABLE ChapterEntry (
    LogbookEntryId INTEGER PRIMARY KEY NOT NULL,
    EntryId INTEGER,
    PrincipalLogbook INTEGER,
    LogbookChapterNo INTEGER,
    LogbookId INTEGER
);

INSERT INTO ChapterEntry (LogbookEntryId, EntryId, PrincipalLogbook, LogbookChapterNo, LogbookId) VALUES (1, 101, 24, 1, 24);
INSERT INTO ChapterEntry (LogbookEntryId, EntryId, PrincipalLogbook, LogbookChapterNo, LogbookId) VALUES (2, 102, 24, 1, 24);
INSERT INTO ChapterEntry (LogbookEntryId, EntryId, PrincipalLogbook, LogbookChapterNo, LogbookId) VALUES (3, 103, 24, 2, 24);
INSERT INTO ChapterEntry (LogbookEntryId, EntryId, PrincipalLogbook, LogbookChapterNo, LogbookId) VALUES (4, 104, 24, 2, 24);
INSERT INTO ChapterEntry (LogbookEntryId, EntryId, PrincipalLogbook, LogbookChapterNo, LogbookId) VALUES (5, 105, 24, 2, 24);
