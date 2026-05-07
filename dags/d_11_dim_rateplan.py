from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_rateplan(**context):

    pricing_ids = context["dag_run"].conf.get("pricing_ids", [])

    if not pricing_ids:
        print("No pricing_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pid in pricing_ids:

            print(f"\n🔄 Processing pricing_id: {pid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # Matches initial load: rates table, same columns
            bc.execute("""
            SELECT
                r.id                    AS pricing_id,
                r.description           AS rate_plan_name,
                r.rate_type_id          AS rate_type,
                r.free_hours            AS free_minutes,
                r.max_stay              AS max_daily_cap,
                r.price                 AS base_rate,
                r.active                AS is_dynamic_flag,

                MD5(CONCAT_WS('|',
                    r.description, r.rate_type_id, r.free_hours,
                    r.max_stay, r.price, r.active
                )) AS record_hash

            FROM rates r

            WHERE r.id = %s
            """, (pid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
            SELECT *
            FROM {GOLD_DB}.dim_rateplan
            WHERE pricing_id = %s AND is_current = 1
            """, (pid,))

            current = gc.fetchone()

            # STEP 3: CHANGE DETECTION
            if current and current["record_hash"] == new["record_hash"]:
                print("✅ No change detected")
                continue

            # STEP 4: EXPIRE OLD RECORD
            if current:
                print("♻️ Expiring old record")
                # FIX: column is effective_end_date (not dw_effective_end_date)
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_rateplan
                SET is_current = 0, effective_end_date = NOW()
                WHERE pricing_id = %s AND is_current = 1
                """, (pid,))

            # STEP 5: INSERT NEW RECORD
            # FIX: columns are effective_start_date / effective_end_date
            # (not dw_effective_start_date / dw_effective_end_date)
            print("🆕 Inserting new record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_rateplan (
                pricing_id, rate_plan_name, rate_type, free_minutes, max_daily_cap,
                base_rate, is_dynamic_flag,
                effective_start_date, effective_end_date,
                is_current, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), '2038-01-19 03:14:07', 1, %s)
            ON DUPLICATE KEY UPDATE
                rate_plan_name  = VALUES(rate_plan_name),
                rate_type       = VALUES(rate_type),
                free_minutes    = VALUES(free_minutes),
                max_daily_cap   = VALUES(max_daily_cap),
                base_rate       = VALUES(base_rate),
                is_dynamic_flag = VALUES(is_dynamic_flag),
                record_hash     = VALUES(record_hash),
                updated_at      = NOW()
            """, (
                new["pricing_id"], new["rate_plan_name"], new["rate_type"],
                new["free_minutes"], new["max_daily_cap"], new["base_rate"],
                new["is_dynamic_flag"], new["record_hash"]
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
    dag_id="gold_upsert_dim_rateplan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_rateplan",
        python_callable=upsert_dim_rateplan
    )
