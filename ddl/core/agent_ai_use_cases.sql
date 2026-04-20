CREATE TABLE IF NOT EXISTS catalog_core.agent_ai_use_cases (
  agent_id string,
  identifier string,
  name string,
  description string,
  proposed_by string,
  owner string,
  function string,
  problem_statement string,
  expected_benefits string,
  priority string,
  status string,
  created_ts timestamp,
  updated_ts timestamp,
  agent_internal_id string,
  agent_risk_exposure_are decimal(10, 2),
  no_of_associated_agents int,
  inherent_risk_classification string,
  residual_risk_classification string,
  agent_risk_tier_art string,
  blended_risk_score decimal(10, 2),
  inherent_risk_classification_score decimal(10, 2),
  residual_risk_classification_score decimal(10, 2),
  solution_approach string
)
PARTITIONED BY (day(created_ts))
LOCATION 's3://{{S3_BUCKET}}/iceberg/core/agent_ai_use_cases/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
