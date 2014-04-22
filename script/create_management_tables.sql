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
