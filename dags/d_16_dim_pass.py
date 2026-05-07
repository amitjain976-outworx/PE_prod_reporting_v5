"""
DAG: gold_upsert_dim_pass
CDC incremental upsert for dim_pass.

FIX: Original had cross-server JOINs:
     FROM {BRONZE_DB}.rates r
     LEFT JOIN {GOLD_DB}.dim_facility df ...
     LEFT JOIN {GOLD_DB}.dim_partner_account dpa ...
     These fail when bronze is on a remote server.
     Now: fetch rate row from bronze, resolve facility_key and
     partner_account_key from gold separately, then insert.

Triggered by Kafka listener with conf = {"rate_ids": [1, 2, ...]}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_pass(**context):
    rate_ids = context["dag_run"].conf.get("rate_ids", [])

    if not rate_ids:
        print("No rate_ids received")
        return

    # FIX: two separate connections — bronze (remote) and gold (local)
    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for rid in rate_ids:
            print(f"\n🔄 Processing rate_id (pass): {rid}")

            # STEP 1: Fetch rate row from BRONZE
            # FIX: no cross-server JOIN — fetch r columns only, resolve dim keys separately
            bc.execute("""
            SELECT
                r.id            AS pass_id,
                r.facility_id,
                r.partner_id,
                r.active        AS pass_status,
                r.start_date    AS start_datetime,
                r.end_date      AS end_datetime,
                r.description   AS pass_name,
                r.total_usage   AS uses,
                r.price,
                r.created_at,
                r.updated_at,
                r.start_date    AS effective_start_date,
                r.end_date      AS effective_end_date
            FROM rates r
            WHERE r.id = %s
              AND r.rate_type_id = 7
            """, (rid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found (rate_type_id != 7 or not found)")
                continue

            # Compute record_hash (mirrors initial load: MD5(CONCAT(id, facility_id, partner_id)))
            import hashlib
            hash_str = (
                str(new["pass_id"]    or "") +
                str(new["facility_id"] or "") +
                str(new["partner_id"] or "")
            )
            record_hash = hashlib.md5(hash_str.encode()).hexdigest()

            # STEP 2: Resolve dim keys from GOLD
            # FIX: was LEFT JOIN {GOLD_DB}.dim_facility in same SQL — now separate lookups
            gc.execute(f"""
                SELECT facility_key FROM {GOLD_DB}.dim_facility
                WHERE facility_id = %s AND is_current = 1 LIMIT 1
            """, (new["facility_id"],))
            frow = gc.fetchone()
            facility_key = frow["facility_key"] if frow else None

            gc.execute(f"""
                SELECT partner_account_key FROM {GOLD_DB}.dim_partner_account
                WHERE account_id_source = %s AND is_current = 1 LIMIT 1
            """, (new["partner_id"],))
            prow = gc.fetchone()
            partner_account_key = prow["partner_account_key"] if prow else None

            # STEP 3: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
            SELECT pass_key, record_hash
            FROM {GOLD_DB}.dim_pass
            WHERE pass_id = %s AND is_current = 1
            LIMIT 1
            """, (rid,))
            current = gc.fetchone()

            # STEP 4: CHANGE DETECTION
            if current and current["record_hash"] == record_hash:
                print("✅ No change detected")
                continue

            # STEP 5: EXPIRE OLD RECORD (SCD Type 2)
            if current:
                print("♻️ Expiring old record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_pass
                SET is_current = 0, effective_end_date = NOW()
                WHERE pass_id = %s AND is_current = 1
                """, (rid,))

            # STEP 6: INSERT NEW RECORD
            print("🆕 Inserting new record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_pass (
                pass_id, facility_key, partner_account_key, pass_status,
                start_datetime, end_datetime, pass_name, pass_type, uses,
                price, created_at, updated_at,
                effective_start_date, effective_end_date, is_current, record_hash
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)
            ON DUPLICATE KEY UPDATE
                facility_key        = VALUES(facility_key),
                partner_account_key = VALUES(partner_account_key),
                pass_status         = VALUES(pass_status),
                start_datetime      = VALUES(start_datetime),
                end_datetime        = VALUES(end_datetime),
                pass_name           = VALUES(pass_name),
                uses                = VALUES(uses),
                price               = VALUES(price),
                updated_at          = VALUES(updated_at),
                effective_end_date  = VALUES(effective_end_date),
                record_hash         = VALUES(record_hash)
            """, (
                new["pass_id"],
                facility_key,
                partner_account_key,
                str(new["pass_status"]) if new["pass_status"] is not None else None,
                new["start_datetime"],
                new["end_datetime"],
                new["pass_name"],
                "PASS",
                str(new["uses"]) if new["uses"] is not None else None,
                new["price"],
                new["created_at"],
                new["updated_at"],
                new["effective_start_date"],
                new["effective_end_date"],
                record_hash,
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
    dag_id="gold_upsert_dim_pass",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_pass",
        python_callable=upsert_dim_pass
    )
