"""
DAG: gold_upsert_dim_permit_plan
CDC SCD-2 upsert for dim_permit_plan.

CHANGES vs previous version:
  - validity_days + renewable_flag removed; replaced by permit_frequency_unit
  - Source query now uses permit_rate_descriptions (prd) as the primary table,
    joined to permit_rates — matching the initial load exactly.
  - Hash input updated to use permit_frequency_unit instead of validity_days/renewable_flag.
  - INSERT now writes permit_frequency_unit instead of validity_days/renewable_flag.

All other logic (SCD-2 expire + insert, change detection, workflow) unchanged.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_permit_plan(**context):
    permit_ids = context["dag_run"].conf.get("permit_ids", [])

    if not permit_ids:
        print("No permit_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pid in permit_ids:

            print(f"\n🔄 Processing permit_id: {pid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # CHANGED: source is now permit_rate_descriptions (prd) joined to permit_rates,
            #          matching the initial load exactly.
            #          permit_frequency_unit replaces validity_days + renewable_flag.
            bc.execute("""
                SELECT
                    prd.id                          AS permit_id,
                    prd.description                 AS permit_type,
                    prd.permit_frequency_unit        AS permit_frequency_unit,
                    MAX(prt.rate)                   AS price,
                    COUNT(DISTINCT prt.facility_id) AS max_facilities_allowed,

                    MD5(CONCAT_WS('|',
                        prd.description,
                        prd.permit_frequency_unit,
                        MAX(prt.rate),
                        COUNT(DISTINCT prt.facility_id)
                    )) AS record_hash

                FROM permit_rate_descriptions prd
                LEFT JOIN permit_rates prt
                    ON prd.id = prt.permit_rate_description_id

                WHERE prd.id = %s
                  AND prd.active_status = '1'

                GROUP BY prd.id, prd.description, prd.permit_frequency_unit
            """, (pid,))

            new = bc.fetchone()
            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
                SELECT *
                FROM {GOLD_DB}.dim_permit_plan
                WHERE permit_id = %s AND is_current = 1
            """, (pid,))
            current = gc.fetchone()

            # STEP 3: CHANGE DETECTION
            if current and current["record_hash"] == new["record_hash"]:
                print("✅ No change detected")
                continue

            # STEP 4: EXPIRE OLD RECORD
            if current:
                print("♻️ Expiring old record")
                gc.execute(f"""
                    UPDATE {GOLD_DB}.dim_permit_plan
                    SET is_current = 0, effective_end_date = NOW()
                    WHERE permit_id = %s AND is_current = 1
                """, (pid,))

            # STEP 5: INSERT NEW RECORD
            # CHANGED: permit_frequency_unit replaces validity_days + renewable_flag
            print("🆕 Inserting new record")
            gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_permit_plan (
                    permit_id, permit_type, permit_frequency_unit, price,
                    max_facilities_allowed,
                    effective_start_date, effective_end_date, is_current, record_hash
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), '9999-12-31 23:59:59', 1, %s)
            """, (
                new["permit_id"],
                new["permit_type"],
                new["permit_frequency_unit"],   # CHANGED
                new["price"],
                new["max_facilities_allowed"],
                new["record_hash"]
            ))

            gold_conn.commit()

    except Exception as e:
        gold_conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        bc.close()
        gc.close()
        bronze_conn.close()
        gold_conn.close()


with DAG(
    dag_id="gold_upsert_dim_permit_plan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_permit_plan",
        python_callable=upsert_dim_permit_plan
    )