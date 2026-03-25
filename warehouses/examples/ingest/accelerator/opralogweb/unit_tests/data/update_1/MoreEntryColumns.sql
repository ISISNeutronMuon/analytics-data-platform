-- Update lost time
UPDATE MoreEntryColumns SET NumberValue = 5.1 WHERE EntryId = 104;

-- New records
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (25, 106, 'Equipment 6', NULL, 1);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (26, 106, 'Group 6', NULL, 2);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (27, 106, 'Group Leader comments 6', NULL, 4);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (28, 106, NULL, 4.8, 3);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (29, 107, 'Equipment 7', NULL, 1);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (30, 107, 'Group 7', NULL, 2);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (31, 107, 'Group Leader comments 7', NULL, 4);
INSERT INTO MoreEntryColumns (MoreEntryColumnId, EntryId, ColData, NumberValue, AdditionalColumnId) VALUES (32, 107, NULL, 4.6, 3);
