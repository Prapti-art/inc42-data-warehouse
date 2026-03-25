#!/bin/bash
set -e

echo "============================================"
echo "  Inc42 Data Warehouse Pipeline"
echo "============================================"

# ── Initialize Airflow DB (first run only) ──
if [ ! -f /opt/airflow/airflow.db ]; then
    echo "Initializing Airflow database..."
    airflow db init

    echo "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Inc42 \
        --lastname Admin \
        --role Admin \
        --email admin@inc42.com
fi

# ── Run DB migrations (for upgrades) ──
airflow db migrate

# ── Start Airflow scheduler in background ──
echo "Starting Airflow scheduler..."
airflow scheduler &

# ── Start Airflow webserver ──
echo "Starting Airflow webserver on port 8080..."
echo ""
echo "  Dashboard: http://localhost:8080"
echo "  Username:  admin"
echo "  Password:  admin"
echo ""
echo "============================================"

exec airflow webserver --port 8080
