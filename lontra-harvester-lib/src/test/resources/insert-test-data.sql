INSERT INTO dwca_resource (id,name,sourcefileid,gbif_package_id,record_count) VALUES
(1,'QMOR','qmor-specimens','ada5d0b1-07de-4dc0-83d4-e312f0fb81cb',11);

-- id will be set automatically by the sequence
INSERT INTO import_log (sourcefileid,gbif_package_id,updated_by,event_end_date_time) VALUES
('qmor-specimens','ada5d0b1-07de-4dc0-83d4-e312f0fb81cb','Jim','2014-07-10');