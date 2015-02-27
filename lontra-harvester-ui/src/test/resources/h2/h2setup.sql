-- support hstore as type
CREATE DOMAIN hstore AS OTHER;

-- text is mapped to CLOB but, not indices can be created on CLOB so change the domain to VARCHAR
-- this is allowed if done before tables creation
CREATE DOMAIN IF NOT EXISTS TEXT AS VARCHAR;