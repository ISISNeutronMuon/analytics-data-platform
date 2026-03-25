CREATE TABLE Entries (
    EntryId INTEGER PRIMARY KEY NOT NULL,
    EntryTimestamp TEXT,
    AdditionalComment TEXT,
    LastChangedDate TEXT,
    LogicallyDeleted TEXT
);

INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (101, '2024-01-01 00:00:01.000000', '<p>Comment 101.</p>', '2024-01-01 00:00:02.000000', 'N');
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (102, '2024-01-01 00:01:03.000000', '<p>Comment 102.</p>', '2024-01-01 00:01:04.000000', 'N');
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (103, '2024-01-01 00:03:05.000000', '<p>Comment 103.</p>', '2024-01-01 00:03:06.000000', 'N');
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (104, '2024-01-01 00:15:07.000000', '<p>Comment 104.</p>', '2024-01-01 00:15:08.000000', 'N');
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (105, '2024-01-01 00:45:09.000000', '<p>Comment 105.</p>', '2024-01-01 00:45:10.000000', 'N');
