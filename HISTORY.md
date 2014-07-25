Version History
===============
Version 0.3 2014-??-??
* Added new module (component) for the node
* Changes made to the resource_management table:
```
ALTER SEQUENCE resource_management_resource_id_seq RENAME TO resource_management_id_seq;
ALTER TABLE resource_management RENAME resource_id  TO id;
ALTER TABLE resource_management RENAME source_file_id  TO sourcefileid;
ALTER TABLE resource_management DROP COLUMN last_updated;
```

Version 0.2 2014-01-14
* Split project into 2 components (Issue #6)
* Renamed package net.canadensys.processing to net.canadensys.harvester
* Add new Generic steps
* Fixed Issue #5 and #7

Version 0.1 2013-12-02
* Initial version as used by Canadensys
