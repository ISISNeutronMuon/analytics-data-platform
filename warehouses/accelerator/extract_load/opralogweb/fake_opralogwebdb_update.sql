-- Mimic and update in the DB

update Entries
  set AdditionalComment = '<p>LTR 1 comment updated</p>'
where EntryId = 100;

update MoreEntryColumns
  set NumberValue = 4.7
where EntryId = 100 and AdditionalColumnId = 2;

insert into
   LogbookEntryChanges
values
   (4, '2024-11-14 15:06:31.000', 24, 'Entry modified', 100);
