"""
DAG: gold_upsert_fact_passes
CDC incremental upsert for fact_passes.

FIX: Original used single get_gold_connection() and ran:
     FROM {BRONZE_DB}.user_passes up  — cross-server on gold conn → fails.
     Solution: fetch user_passes row from bronze, resolve dim keys from gold,
     then write to gold.

Triggered by Kafka listener with conf = {"user_pass_ids": [1, 2, ...]}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def _dk(dt_val):
    if dt_val is None:
        return None
    if hasattr(dt_val, "strftime"):
        return int(dt_val.strftime("%Y%m%d"))
    return None


def upsert_fact_passes(**context):
    user_pass_ids = context["dag_run"].conf.get("user_pass_ids", [])

    if not user_pass_ids:
        print("No user_pass_ids received")
        return

    # FIX: two separate connections — bronze (remote) and gold (local)
    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for upid in user_pass_ids:
            print(f"\n🔄 Processing user_pass_id: {upid}")

            # STEP 1: Fetch source row from BRONZE
            # FIX: was queried on gold cursor with {BRONZE_DB} prefix → cross-server fail
            bc.execute("""
            SELECT
                up.id           AS source_user_pass_id,
                up.user_id,
                up.rate_id,
                up.start_time,
                up.end_time,
                up.cancelled_at,
                up.pass_code    AS source_pass_id
            FROM user_passes up
            WHERE up.id         = %s
              AND up.deleted_at IS NULL
            """, (upid,))

            row = bc.fetchone()
            if not row:
                print("⚠️ No source data found (deleted or not exists)")
                continue

            # STEP 2: Resolve dimension keys from GOLD
            # parker_key via dim_parker.customer_id = up.user_id
            gc.execute(f"""
                SELECT parker_key FROM {GOLD_DB}.dim_parker
                WHERE customer_id = %s LIMIT 1
            """, (row["user_id"],))
            r = gc.fetchone()
            parker_key = r["parker_key"] if r else None

            # pass_key via dim_pass.pass_id = up.rate_id
            gc.execute(f"""
                SELECT pass_key FROM {GOLD_DB}.dim_pass
                WHERE pass_id = %s AND is_current = 1 LIMIT 1
            """, (row["rate_id"],))
            r = gc.fetchone()
            pass_key = r["pass_key"] if r else None

            # STEP 3: Compute derived fields
            period_start = _dk(row["start_time"])
            period_end   = _dk(row["end_time"])

            cancelled_str = str(row["cancelled_at"] or "").strip()
            status_date   = row["cancelled_at"] if cancelled_str else None

            # STEP 4: UPSERT into GOLD fact_passes
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.fact_passes (
                parker_key, pass_key,
                period_start_date_key, period_end_date_key,
                status_date, source_pass_id, source_user_pass_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                parker_key            = VALUES(parker_key),
                pass_key              = VALUES(pass_key),
                period_start_date_key = VALUES(period_start_date_key),
                period_end_date_key   = VALUES(period_end_date_key),
                status_date           = VALUES(status_date),
                source_pass_id        = VALUES(source_pass_id)
            """, (
                parker_key,
                pass_key,
                period_start,
                period_end,
                status_date,
                str(row["source_pass_id"]) if row["source_pass_id"] is not None else None,
                int(upid),
            ))
            gold_conn.commit()
            print(f"✅ fact_passes upserted for user_pass_id={upid}")

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
    dag_id="gold_upsert_fact_passes",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_passes",
        python_callable=upsert_fact_passes
    )
