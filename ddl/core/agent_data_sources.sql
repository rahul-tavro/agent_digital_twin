CREATE TABLE IF NOT EXISTS catalog_core.agent_data_sources (
    agent_id string,
	agent_internal_id string,
    access_level string,
    contains_pii boolean,
    contains_phi boolean,
    contains_pci boolean,
    created_ts timestamp,
    updated_ts timestamp,
    relationship_id string,
    parent_relationship_id string,
    source_object_id string,
    source_object_domain string,
    source_object_name string,
    source_object_type string,
    target_object_id string,
    target_object_domain string,
    target_object_name string,
    target_object_type string
)
PARTITIONED BY (day(created_ts))
LOCATION 's3://{{S3_BUCKET}}/iceberg/core/agent_data_sources/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
