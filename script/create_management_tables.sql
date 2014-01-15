CREATE SEQUENCE import_log_id_seq;
CREATE TABLE import_log
(
  id integer DEFAULT nextval('import_log_id_seq') NOT NULL,
  sourcefileid character varying(50),
  record_quantity integer,
  updated_by character varying(50),
  import_process_duration_ms integer,
  event_end_date_time timestamp,
  CONSTRAINT import_log_pkey PRIMARY KEY (id )
);
CREATE SEQUENCE resource_management_resource_id_seq;
CREATE TABLE resource_management
(
  resource_id integer DEFAULT nextval('resource_management_resource_id_seq') NOT NULL,
  name character varying(255),
  key character varying(36),
  archive_url character varying(255),
  source_file_id character varying(50),
  last_updated timestamp,
  CONSTRAINT resource_management_pkey PRIMARY KEY (resource_id ),
  CONSTRAINT resource_management_source_file_id_key UNIQUE (source_file_id)
);
