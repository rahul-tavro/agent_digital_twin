import os
import json
import hashlib
import boto3
from datetime import datetime
from pyspark.sql import SparkSession

# ── Config ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
NESSIE_URI       = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")
ICEBERG_BUCKET   = os.environ.get("ICEBERG_BUCKET", "warehouse")
CATALOG          = "nessie.catalog_core"

# ── S3 client pointing to MinIO ─────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# ── Spark session ───────────────────────────────────────────────────────────────
def get_spark():
    return (
        SparkSession.builder
        .appName("AgentCardProcessor")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", f"s3a://{ICEBERG_BUCKET}/")
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ── Helpers ─────────────────────────────────────────────────────────────────────
def _hash(obj) -> str:
    raw = json.dumps(obj, sort_keys=True, default=str) if obj is not None else ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _sq(val) -> str:
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def _bool(val) -> str:
    if val is None:
        return "NULL"
    return "true" if val else "false"

def _array_str(lst) -> str:
    if not lst:
        return "array()"
    items = ", ".join(f"'{str(i).replace(chr(39), chr(39)+chr(39))}'" for i in lst)
    return f"array({items})"

def execute_dml(spark, sql: str, label: str = ""):
    try:
        spark.sql(sql)
        print(f"  ✓ {label} succeeded")
    except Exception as e:
        print(f"  ✗ {label} failed: {e}")
        raise

def execute_query(spark, sql: str):
    return spark.sql(sql).collect()

def has_meaningful_data(data) -> bool:
    def is_meaningful(value):
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        if isinstance(value, (list, dict)) and len(value) == 0:
            return False
        return True
    if isinstance(data, dict):
        return any(is_meaningful(v) for v in data.values())
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                if any(is_meaningful(v) for v in item.values()):
                    return True
        return False
    return False

# ── Upsert functions ─────────────────────────────────────────────────────────────

def get_current_agent_source_hash(spark, agent_id: str):
    sql = f"""
        SELECT source_hash FROM {CATALOG}.agents
        WHERE agent_id = {_sq(agent_id)} AND is_current = true
        ORDER BY updated_ts DESC LIMIT 1
    """
    result = execute_query(spark, sql)
    if result:
        return result[0]["source_hash"]
    return None


def upsert_agent(spark, card: dict, now_str: str, incoming_source_hash: str = None):
    ident = card.get("identification", {})
    agent_id = ident.get("agent_id")
    agent_internal_id = ident.get("agent_internal_id")

    row = {
        "agent_name":             card.get("name"),
        "agent_description":      card.get("description"),
        "protocol_version":       card.get("protocol_version"),
        "preferred_transport":    card.get("preferredTransport"),
        "supports_auth_ext_card": card.get("supports_authenticated_extended_card"),
        "card_version":           card.get("version"),
        "source_system":          card.get("provider", {}).get("organization"),
    }

    source_hash = incoming_source_hash or _hash(card)
    record_hash = _hash(row)

    sql = f"""
        MERGE INTO {CATALOG}.agents AS t
        USING (
            SELECT
                {_sq(agent_id)}                          AS agent_id,
                {_sq(agent_internal_id)}                 AS agent_internal_id,
                {_sq(row['agent_name'])}                 AS agent_name,
                {_sq(row['agent_description'])}          AS agent_description,
                {_sq(row['protocol_version'])}           AS protocol_version,
                {_sq(row['preferred_transport'])}        AS preferred_transport,
                {_bool(row['supports_auth_ext_card'])}   AS supports_auth_ext_card,
                {_sq(row['card_version'])}               AS card_version,
                {_sq(source_hash)}                       AS source_hash,
                {_sq(row['source_system'])}              AS source_system,
                {_sq(record_hash)}                       AS record_hash,
                TIMESTAMP '{now_str}'                    AS now_ts
        ) AS s
        ON t.agent_id = s.agent_id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            agent_internal_id      = s.agent_internal_id,
            agent_description      = s.agent_description,
            protocol_version       = s.protocol_version,
            preferred_transport    = s.preferred_transport,
            supports_auth_ext_card = s.supports_auth_ext_card,
            card_version           = s.card_version,
            source_hash            = s.source_hash,
            source_system          = s.source_system,
            record_hash            = s.record_hash,
            updated_ts             = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_id, agent_internal_id, agent_name, agent_description,
            protocol_version, preferred_transport, supports_auth_ext_card,
            card_version, source_hash, source_system, record_hash,
            valid_from_ts, valid_to_ts, is_current, created_ts, updated_ts
        ) VALUES (
            s.agent_id, s.agent_internal_id, s.agent_name, s.agent_description,
            s.protocol_version, s.preferred_transport, s.supports_auth_ext_card,
            s.card_version, s.source_hash, s.source_system, s.record_hash,
            s.now_ts, NULL, true, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agents MERGE")
    return agent_internal_id


def upsert_agent_identification(spark, card: dict, agent_internal_id: str, now_str: str):
    ident    = card.get("identification", {})
    agent_id = ident.get("agent_id")
    tags     = ident.get("tags") if isinstance(ident.get("tags"), list) else []

    sql = f"""
        MERGE INTO {CATALOG}.agent_identifications AS t
        USING (
            SELECT
                {_sq(agent_internal_id)}              AS agent_internal_id,
                {_sq(agent_id)}                       AS agent_id,
                {_sq(ident.get('goal_orientation'))}  AS goal_orientation,
                {_sq(ident.get('role'))}              AS role,
                {_sq(ident.get('instruction'))}       AS instruction,
                {_sq(ident.get('owner'))}             AS owner,
                {_sq(ident.get('environment'))}       AS environment,
                {_array_str(tags)}                    AS tags,
                {_sq(ident.get('governance_status'))} AS governance_status,
                {_sq(ident.get('reviewer'))}          AS reviewer,
                {_sq(ident.get('cost_center'))}       AS cost_center,
                TIMESTAMP '{now_str}'                 AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, goal_orientation = s.goal_orientation,
            role = s.role, instruction = s.instruction, owner = s.owner,
            environment = s.environment, tags = s.tags,
            governance_status = s.governance_status, reviewer = s.reviewer,
            cost_center = s.cost_center, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, goal_orientation, role, instruction,
            owner, environment, tags, governance_status, reviewer, cost_center,
            is_current, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.goal_orientation, s.role, s.instruction,
            s.owner, s.environment, s.tags, s.governance_status, s.reviewer, s.cost_center,
            true, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_identifications MERGE")


def upsert_agent_configuration(spark, card: dict, agent_internal_id: str, now_str: str):
    cfg = card.get("configuration", {})
    if not has_meaningful_data(cfg):
        print("Skipping agent_configurations: empty.")
        return

    agent_id       = card.get("identification", {}).get("agent_id")
    caps           = card.get("capabilities", {})
    execution_mode = "streaming" if caps.get("streaming") else "batch"

    sql = f"""
        MERGE INTO {CATALOG}.agent_configurations AS t
        USING (
            SELECT
                {_sq(agent_internal_id)}                 AS agent_internal_id,
                {_sq(agent_id)}                          AS agent_id,
                {_sq(cfg.get('access_scope'))}           AS access_scope,
                {_sq(cfg.get('memory_type'))}            AS memory_type,
                {_sq(cfg.get('data_freshness_policy'))}  AS data_freshness_policy,
                {_sq(cfg.get('autonomy_level'))}         AS autonomy_level,
                {_sq(cfg.get('reasoning_model'))}        AS reasoning_model,
                NULL                                     AS human_in_the_loop_flag,
                {_sq(execution_mode)}                    AS execution_mode,
                TIMESTAMP '{now_str}'                    AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, access_scope = s.access_scope,
            memory_type = s.memory_type, data_freshness_policy = s.data_freshness_policy,
            autonomy_level = s.autonomy_level, reasoning_model = s.reasoning_model,
            execution_mode = s.execution_mode, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, access_scope, memory_type,
            data_freshness_policy, autonomy_level, reasoning_model,
            human_in_the_loop_flag, execution_mode,
            valid_from_ts, valid_to_ts, is_current, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.access_scope, s.memory_type,
            s.data_freshness_policy, s.autonomy_level, s.reasoning_model,
            s.human_in_the_loop_flag, s.execution_mode,
            s.now_ts, NULL, true, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_configurations MERGE")


def upsert_agent_tools(spark, card: dict, agent_internal_id: str, now_str: str):
    tools    = card.get("tool", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(tools):
        print("Skipping agent_tools: empty.")
        return

    select_rows = []
    for tool in tools:
        delegation = str(tool.get("delegation_possible")).lower() == "true" if tool.get("delegation_possible") is not None else None
        select_rows.append(f"""
            SELECT
                {_sq(agent_internal_id)}             AS agent_internal_id,
                {_sq(agent_id)}                      AS agent_id,
                {_sq(tool.get('identifier'))}        AS tool_id,
                {_sq(tool.get('name'))}              AS tool_name,
                {_sq(tool.get('description'))}       AS tool_description,
                {_bool(delegation)}                  AS delegation_possible,
                {_sq(tool.get('allowed_delegates'))} AS allowed_delegates,
                {_sq(tool.get('input_schema'))}      AS input_schema_json_text,
                {_sq(tool.get('output_schema'))}     AS output_schema_json_text,
                {_sq(tool.get('default_value'))}     AS default_config_json_text,
                TIMESTAMP '{now_str}'                AS now_ts
        """)

    sql = f"""
        MERGE INTO {CATALOG}.agent_tools AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.tool_id = s.tool_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, tool_description = s.tool_description,
            delegation_possible = s.delegation_possible, allowed_delegates = s.allowed_delegates,
            input_schema_json_text = s.input_schema_json_text,
            output_schema_json_text = s.output_schema_json_text,
            default_config_json_text = s.default_config_json_text, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, tool_id, tool_name, tool_description,
            delegation_possible, allowed_delegates, input_schema_json_text,
            output_schema_json_text, default_config_json_text, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.tool_id, s.tool_name, s.tool_description,
            s.delegation_possible, s.allowed_delegates, s.input_schema_json_text,
            s.output_schema_json_text, s.default_config_json_text, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_tools BULK MERGE ({len(tools)})")


def upsert_agent_llm_models(spark, card: dict, agent_internal_id: str, now_str: str):
    models   = card.get("llm_model", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(models):
        print("Skipping llm_models: empty.")
        return

    select_rows = [f"""
        SELECT {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(m.get('name'))} AS name,
               {_sq(m.get('version'))} AS version_number,
               TIMESTAMP '{now_str}' AS now_ts
    """ for m in models]

    sql = f"""
        MERGE INTO {CATALOG}.agent_llm_models AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.name = s.name
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, version_number = s.version_number, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, name, version_number, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.name, s.version_number, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_llm_models BULK MERGE ({len(models)})")


def upsert_agent_ai_models(spark, card: dict, agent_internal_id: str, now_str: str):
    models   = card.get("ai_model", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(models):
        print("Skipping ai_models: empty.")
        return

    select_rows = [f"""
        SELECT NULL AS ai_model_id,
               {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(m.get('name'))} AS model_name,
               {_sq(m.get('owner'))} AS owner,
               {_sq(m.get('department_executive'))} AS department_executive,
               {_sq(m.get('description'))} AS description,
               TIMESTAMP '{now_str}' AS now_ts
    """ for m in models]

    sql = f"""
        MERGE INTO {CATALOG}.agent_ai_models AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.model_name = s.model_name
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, owner = s.owner,
            department_executive = s.department_executive,
            description = s.description, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            ai_model_id, agent_internal_id, agent_id, model_name,
            owner, department_executive, description, created_ts, updated_ts
        ) VALUES (
            s.ai_model_id, s.agent_internal_id, s.agent_id, s.model_name,
            s.owner, s.department_executive, s.description, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_ai_models BULK MERGE ({len(models)})")


def upsert_agent_guardrail(spark, card: dict, agent_internal_id: str, now_str: str):
    guardrail = card.get("guardrail", {})
    agent_id  = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(guardrail):
        print("Skipping guardrails: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_guardrails AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(guardrail.get('name'))} AS name,
                   {_sq(guardrail.get('description'))} AS description,
                   {_sq(guardrail.get('model'))} AS model,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.name = s.name
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, description = s.description,
            model = s.model, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, name, description, model, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.name, s.description, s.model, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_guardrails MERGE")


def upsert_agent_mcp_server(spark, card: dict, agent_internal_id: str, now_str: str):
    mcp      = card.get("mcp_server", {})
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(mcp):
        print("Skipping mcp_servers: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_mcp_servers AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(mcp.get('name'))} AS name,
                   {_sq(mcp.get('url'))} AS url,
                   {_sq(mcp.get('version_number'))} AS version_number,
                   TIMESTAMP '{now_str}' AS last_updated_ts,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, name = s.name, url = s.url,
            version_number = s.version_number,
            last_updated_ts = s.last_updated_ts, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, name, url, version_number,
            last_updated_ts, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.name, s.url, s.version_number,
            s.last_updated_ts, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_mcp_servers MERGE")


def upsert_agent_memory(spark, card: dict, agent_internal_id: str, now_str: str):
    memory   = card.get("memory", {})
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(memory):
        print("Skipping memories: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_memories AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(memory.get('identifier'))} AS identifier,
                   {_sq(memory.get('name'))} AS name,
                   {_sq(memory.get('type'))} AS type,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, identifier = s.identifier,
            name = s.name, type = s.type, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, identifier, name, type, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.identifier, s.name, s.type, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_memories MERGE")


def upsert_agent_knowledge_source(spark, card: dict, agent_internal_id: str, now_str: str):
    ks       = card.get("knowledge_source", {})
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(ks):
        print("Skipping knowledge_sources: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_knowledge_sources AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(ks.get('identifier'))} AS identifier,
                   {_sq(ks.get('name'))} AS name,
                   {_sq(ks.get('access_mechanism'))} AS access_mechanism,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, identifier = s.identifier,
            name = s.name, access_mechanism = s.access_mechanism, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, identifier, name, access_mechanism, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.identifier, s.name, s.access_mechanism, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_knowledge_sources MERGE")


def upsert_agent_controls(spark, card: dict, agent_internal_id: str, now_str: str):
    controls = card.get("control", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(controls):
        print("Skipping controls: empty.")
        return

    select_rows = [f"""
        SELECT {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(c.get('identifier'))} AS identifier,
               {_sq(c.get('name'))} AS name,
               {_sq(c.get('objective'))} AS objective,
               {_sq(c.get('domain'))} AS domain,
               TIMESTAMP '{now_str}' AS now_ts
    """ for c in controls]

    sql = f"""
        MERGE INTO {CATALOG}.agent_controls AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.name = s.name
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, identifier = s.identifier,
            objective = s.objective, domain = s.domain, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, identifier, name, objective, domain, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.identifier, s.name, s.objective, s.domain, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_controls BULK MERGE ({len(controls)})")


def upsert_agent_business_processes(spark, card: dict, agent_internal_id: str, now_str: str):
    procs    = card.get("business_process", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(procs):
        print("Skipping business_processes: empty.")
        return

    select_rows = [f"""
        SELECT {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(p.get('identifier'))} AS business_process_id,
               {_sq(p.get('name'))} AS process_name,
               {_sq(p.get('business_criticality'))} AS criticality,
               TIMESTAMP '{now_str}' AS now_ts
    """ for p in procs]

    sql = f"""
        MERGE INTO {CATALOG}.agent_business_processes AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.business_process_id = s.business_process_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, process_name = s.process_name,
            criticality = s.criticality, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, business_process_id, process_name, criticality, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.business_process_id, s.process_name, s.criticality, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_business_processes BULK MERGE ({len(procs)})")


def upsert_agent_business_applications(spark, card: dict, agent_internal_id: str, now_str: str):
    apps     = card.get("application", []) or []
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(apps):
        print("Skipping business_applications: empty.")
        return

    select_rows = [f"""
        SELECT {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(a.get('identifier'))} AS business_application_id,
               {_sq(a.get('name'))} AS application_name,
               {_sq(a.get('business_criticality'))} AS criticality,
               TIMESTAMP '{now_str}' AS now_ts
    """ for a in apps]

    sql = f"""
        MERGE INTO {CATALOG}.agent_business_applications AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.business_application_id = s.business_application_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, application_name = s.application_name,
            criticality = s.criticality, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, business_application_id, application_name, criticality, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.business_application_id, s.application_name, s.criticality, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_business_applications BULK MERGE ({len(apps)})")


def upsert_agent_prompt_template(spark, card: dict, agent_internal_id: str, now_str: str):
    tmpl     = card.get("prompt_template", {})
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(tmpl):
        print("Skipping prompt_templates: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_prompt_templates AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(tmpl.get('identifier'))} AS identifier,
                   {_sq(tmpl.get('name'))} AS name,
                   {_sq(tmpl.get('description'))} AS description,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, identifier = s.identifier,
            name = s.name, description = s.description, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, identifier, name, description, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.identifier, s.name, s.description, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_prompt_templates MERGE")


def upsert_agent_regulation_or_framework(spark, card: dict, agent_internal_id: str, now_str: str):
    reg      = card.get("regulation_or_framework", {})
    agent_id = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(reg):
        print("Skipping regulations: empty.")
        return

    sql = f"""
        MERGE INTO {CATALOG}.agent_regulations_or_frameworks AS t
        USING (
            SELECT {_sq(agent_internal_id)} AS agent_internal_id,
                   {_sq(agent_id)} AS agent_id,
                   {_sq(reg.get('name'))} AS name,
                   {_sq(reg.get('type'))} AS type,
                   {_sq(reg.get('regulatory_authority'))} AS regulatory_authority,
                   {_sq(reg.get('jurisdiction'))} AS jurisdiction,
                   {_sq(reg.get('requirement'))} AS requirement,
                   TIMESTAMP '{now_str}' AS now_ts
        ) AS s
        ON t.agent_internal_id = s.agent_internal_id
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, name = s.name, type = s.type,
            regulatory_authority = s.regulatory_authority, jurisdiction = s.jurisdiction,
            requirement = s.requirement, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, name, type, regulatory_authority,
            jurisdiction, requirement, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.name, s.type, s.regulatory_authority,
            s.jurisdiction, s.requirement, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, "agent_regulations_or_frameworks MERGE")


def upsert_agent_ai_use_cases(spark, card: dict, agent_internal_id: str, now_str: str):
    use_cases = card.get("ai_use_case", []) or []
    agent_id  = card.get("identification", {}).get("agent_id")
    if not has_meaningful_data(use_cases):
        print("Skipping ai_use_cases: empty.")
        return

    select_rows = [f"""
        SELECT {_sq(agent_internal_id)} AS agent_internal_id,
               {_sq(agent_id)} AS agent_id,
               {_sq(u.get('identifier'))} AS identifier,
               {_sq(u.get('name'))} AS name,
               {_sq(u.get('description'))} AS description,
               {_sq(u.get('proposed_by'))} AS proposed_by,
               {_sq(u.get('owner'))} AS owner,
               {_sq(u.get('business_function'))} AS function,
               {_sq(u.get('problem_statement'))} AS problem_statement,
               {_sq(u.get('expected_benefits'))} AS expected_benefits,
               {_sq(u.get('priority'))} AS priority,
               {_sq(u.get('status'))} AS status,
               TIMESTAMP '{now_str}' AS now_ts
    """ for u in use_cases]

    sql = f"""
        MERGE INTO {CATALOG}.agent_ai_use_cases AS t
        USING ({" UNION ALL ".join(select_rows)}) AS s
        ON t.agent_internal_id = s.agent_internal_id AND t.name = s.name
        WHEN MATCHED THEN UPDATE SET
            agent_id = s.agent_id, identifier = s.identifier,
            description = s.description, proposed_by = s.proposed_by,
            owner = s.owner, function = s.function,
            problem_statement = s.problem_statement,
            expected_benefits = s.expected_benefits,
            priority = s.priority, status = s.status, updated_ts = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
            agent_internal_id, agent_id, identifier, name, description,
            proposed_by, owner, function, problem_statement, expected_benefits,
            priority, status, created_ts, updated_ts
        ) VALUES (
            s.agent_internal_id, s.agent_id, s.identifier, s.name, s.description,
            s.proposed_by, s.owner, s.function, s.problem_statement, s.expected_benefits,
            s.priority, s.status, s.now_ts, s.now_ts
        )
    """
    execute_dml(spark, sql, f"agent_ai_use_cases BULK MERGE ({len(use_cases)})")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PROCESSOR  — called by webhook.py
# ══════════════════════════════════════════════════════════════════════════════

def process_card(bucket: str, key: str):
    print(f"\n{'='*60}")
    print(f"Processing s3://{bucket}/{key}")
    print(f"{'='*60}")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Read JSON from MinIO
    obj      = s3.get_object(Bucket=bucket, Key=key)
    card     = json.loads(obj["Body"].read().decode("utf-8"))

    agent_id = card.get("identification", {}).get("agent_id")
    if not agent_id:
        raise ValueError("Missing identification.agent_id in card JSON")

    print(f"agent_id={agent_id}")

    # Start Spark
    spark = get_spark()

    # Hash check — skip if no changes
    incoming_hash = _hash(card)
    try:
        existing_hash = get_current_agent_source_hash(spark, agent_id)
        if existing_hash == incoming_hash:
            print(f"No changes detected for agent_id={agent_id}. Skipping.")
            return
        print(f"Change detected. Proceeding with upserts.")
    except Exception as e:
        print(f"[WARN] Hash check failed, continuing: {e}")

    # Run all upserts
    steps = [
        ("agents",                        lambda: upsert_agent(spark, card, now_str, incoming_hash)),
        ("agent_identifications",         lambda: upsert_agent_identification(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_configurations",          lambda: upsert_agent_configuration(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_tools",                   lambda: upsert_agent_tools(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_controls",                lambda: upsert_agent_controls(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_knowledge_sources",       lambda: upsert_agent_knowledge_source(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_llm_models",              lambda: upsert_agent_llm_models(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_ai_use_cases",            lambda: upsert_agent_ai_use_cases(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_business_processes",      lambda: upsert_agent_business_processes(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_business_applications",   lambda: upsert_agent_business_applications(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_guardrails",              lambda: upsert_agent_guardrail(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_mcp_servers",             lambda: upsert_agent_mcp_server(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_memories",                lambda: upsert_agent_memory(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_prompt_templates",        lambda: upsert_agent_prompt_template(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_regulations_or_frameworks", lambda: upsert_agent_regulation_or_framework(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
        ("agent_ai_models",               lambda: upsert_agent_ai_models(spark, card, card.get("identification", {}).get("agent_internal_id"), now_str)),
    ]

    # Step 1 must succeed to get agent_internal_id
    print(f"\n[Step 1/{len(steps)}] agents")
    agent_internal_id = upsert_agent(spark, card, now_str, incoming_hash)

    for i, (name, fn) in enumerate(steps[1:], start=2):
        print(f"\n[Step {i}/{len(steps)}] {name}")
        try:
            fn()
        except Exception as e:
            print(f"  [ERROR] {name} failed: {e}")

    print(f"\n✅ Processing complete for agent_id={agent_id}")