CREATE TABLE IF NOT EXISTS catalog_core.agent_mcp_servers (
  agent_id string,
  agent_internal_id string,
  name string,
  url string,
  version_number string,
  status string,
  last_updated_ts timestamp,
  created_ts timestamp,
  updated_ts timestamp
)
PARTITIONED BY (day(created_ts))
LOCATION 's3://{{S3_BUCKET}}/iceberg/core/agent_mcp_servers/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);

