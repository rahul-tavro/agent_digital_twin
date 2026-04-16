CREATE TABLE IF NOT EXISTS catalog_core.agent_controls (
  agent_id string,
  agent_internal_id string,
  identifier string,
  name string,
  objective string,
  domain string,
  created_ts timestamp,
  updated_ts timestamp
)
PARTITIONED BY (day(created_ts))
LOCATION 's3://{{S3_BUCKET}}/iceberg/core/agent_controls/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);

