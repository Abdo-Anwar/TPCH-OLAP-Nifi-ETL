#!/usr/bin/env bash
# ============================================================
#  OLAP Lab – Quick-start script
#  Run this ONCE before docker compose up
# ============================================================

set -e

echo "📁  Creating required local folders..."
mkdir -p init-scripts      # MySQL DDL / seed files go here
mkdir -p nifi-drivers      # MySQL JDBC jar goes here
mkdir -p parquet-output    # NiFi writes Parquet files here
mkdir -p spark-jobs        # PySpark query scripts go here

# ─────────────────────────────────────────────────────────────
# 1. Download MySQL JDBC driver (needed by NiFi to talk to MySQL)
# ─────────────────────────────────────────────────────────────
JDBC_JAR="mysql-connector-j-8.0.33.jar"
JDBC_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/${JDBC_JAR}"

if [ ! -f "nifi-drivers/${JDBC_JAR}" ]; then
    echo "⬇️   Downloading MySQL JDBC driver..."
    curl -L "${JDBC_URL}" -o "nifi-drivers/${JDBC_JAR}"
    echo "✅  JDBC driver saved to nifi-drivers/${JDBC_JAR}"
else
    echo "✅  JDBC driver already present."
fi

echo ""
echo "🚀  All done!  Now run:  docker compose up -d"
echo ""
echo "Services:"
echo "  MySQL      → localhost:3306  (user: tpch_user / tpch1234)"
echo "  NiFi UI    → https://localhost:8443/nifi  (admin / adminadminadmin)"
echo "  Spark UI   → http://localhost:8080"
echo ""
echo "Next steps:"
echo "  1. Generate TPC-H data  (tpch-dbgen, scale factor 1)"
echo "  2. Copy the .tbl files into init-scripts/ and load into MySQL"
echo "  3. In NiFi: build the ETL flow MySQL → Star Schema → Parquet"
echo "  4. In Spark: run spark-jobs/query.py against parquet-output/"
