CREATE TABLE IF NOT EXISTS catalog_core.agent_resource (
    identifier string,
    mcp_server_id string,
    name string,
    description string,
    uri_template string,
    mime_type string,
    type string,
    tags string,
    version string,
    created_ts timestamp,
    updated_ts timestamp
)
LOCATION 's3://{{S3_BUCKET}}/iceberg/core/agent_resource'
TBLPROPERTIES (
	'table_type' = 'ICEBERG',
	'format' = 'parquet',
	'write_compression' = 'snappy'
);
