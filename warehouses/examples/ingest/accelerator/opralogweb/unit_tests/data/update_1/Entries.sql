-- Update an entry
UPDATE Entries SET AdditionalComment = '<p>Comment 104 updated.</p>', LastChangedDate = '2024-01-03 00:45:09.000000' WHERE EntryId = 104;

-- Insert new ones
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (106, '2024-01-03 10:30:09.000000', '<p>Comment 106</p>', '2024-01-03 10:30:09.000000', 'N');
INSERT INTO Entries (EntryId, EntryTimestamp, AdditionalComment, LastChangedDate, LogicallyDeleted) VALUES (107, '2024-01-03 11:45:09.000000', '<p>Comment 107</p>', '2024-01-03 11:45:09.000000', 'N');
