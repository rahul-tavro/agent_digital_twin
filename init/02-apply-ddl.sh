#!/bin/bash
set -euo pipefail

DDL_ROOT="${DDL_ROOT:-/opt/ddl}"
WORK_DIR="/tmp/ddl-transformed"
ICEBERG_BUCKET="${ICEBERG_BUCKET:-warehouse}"

mkdir -p "${WORK_DIR}"

echo "Transforming DDL files..."

transform_file() {
  local src="$1"
  local dst="$2"

  awk '
    BEGIN { using_inserted = 0 }
    {
      gsub(/s3:\/\/\{\{S3_BUCKET\}\}/, "s3a://'"${ICEBERG_BUCKET}"'")
      gsub(/s3:\/\//, "s3a://")
      gsub(/day\(/, "days(")

      if ($0 ~ /^CREATE TABLE IF NOT EXISTS[[:space:]]+[a-zA-Z0-9_]+\./) {
        sub(/^CREATE TABLE IF NOT EXISTS[[:space:]]+/, "CREATE TABLE IF NOT EXISTS nessie.")
      }

      if ($0 ~ /^[[:space:]]*PARTITIONED BY[[:space:]]*\(/ && using_inserted == 0) {
        print "USING iceberg"
        using_inserted = 1
      }

      if ($0 ~ /table_type/ && $0 ~ /ICEBERG/) {
        next
      }

      print
    }
  ' "$src" > "$dst"
}

# Create namespaces
cat > "${WORK_DIR}/00-namespaces.sql" <<'SQL'
CREATE NAMESPACE IF NOT EXISTS nessie.catalog_core;
CREATE NAMESPACE IF NOT EXISTS nessie.catalog_curated;
CREATE NAMESPACE IF NOT EXISTS nessie.catalog_raw;
SQL

# Transform all DDL files
find "${DDL_ROOT}" -type f -name "*.sql" | sort | while IFS= read -r ddl_file; do
  out_file="${WORK_DIR}/$(basename "${ddl_file}")"
  echo "Transforming ${ddl_file} -> ${out_file}"
  transform_file "${ddl_file}" "${out_file}"
done

run_spark_sql_with_retry() {
  local sql_file="$1"
  local attempts=20
  local wait_seconds=5
  local i

  for ((i = 1; i <= attempts; i++)); do
    if spark-sql -f "${sql_file}"; then
      return 0
    fi
    echo "Retry ${i}/${attempts} for ${sql_file}..."
    sleep "${wait_seconds}"
  done

  echo "Failed: ${sql_file}"
  return 1
}

# Combine ALL sql files into one and run once
echo "Applying all DDL in single Spark session..."
cat "${WORK_DIR}/00-namespaces.sql" \
    $(find "${WORK_DIR}" -type f -name "*.sql" ! -name "00-namespaces.sql" | sort) \
    > "${WORK_DIR}/all-ddl.sql"

run_spark_sql_with_retry "${WORK_DIR}/all-ddl.sql"

echo "Applying namespace SQL..."
run_spark_sql_with_retry "${WORK_DIR}/00-namespaces.sql"

echo "Applying DDL files..."
find "${WORK_DIR}" -type f -name "*.sql" ! -name "00-namespaces.sql" | sort | while IFS= read -r f; do
  echo "Applying ${f}..."
  run_spark_sql_with_retry "${f}"
done

echo "Bootstrap complete."
