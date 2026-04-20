CREATE TABLE IF NOT EXISTS catalog_risk_management.agent_risk_scenarios (
  risk_scenario_id string,
  assessment_id string,
  attack_complexity_ac string,
  attack_requirements_at string,
  attack_vector_av string,
  cvss_4_0_vector string,
  agentic_ai_core_security_risks string,
  privileges_required_pr string,
  subsequent_system_availability_sa string,
  subsequent_system_confidentiality_sc string,
  subsequent_system_integrity_si string,
  created_ts timestamp,
  updated_ts timestamp,
  user_interaction_ui string,
  vulnerable_system_availability_va string,
  vulnerable_system_confidentiality_vc string,
  vulnerable_system_integrity_vi string,
  created_by string,
  updated_by string,
  threat_multiplier decimal(10, 2),
  cvss_score decimal(10, 2),
  aivss_score decimal(10, 2))
PARTITIONED BY (day(`created_ts`))
LOCATION 's3://{{S3_BUCKET}}/iceberg/risk_management/agent_risk_scenarios'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='snappy',
  'format'='parquet'
);
