from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_event(**context):

    event_ids = context["dag_run"].conf.get("event_ids", [])

    if not event_ids:
        print("No event_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True, buffered=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for eid in event_ids:

            print(f"\n🔄 Processing event_id: {eid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # FIX: completely rewritten to match initial load:
            #   events JOIN event_facility (for facility_id)
            #          JOIN event_categories (for event_category via partner_id)
            #   Columns: event_id, facility_id, event_name, event_description,
            #            event_category, start_time, end_time, event_rate,
            #            is_active, created_at, updated_at
            # FIX: removed reference to dim_event_type (does not exist in DDL)
            bc.execute("""
            SELECT
                e.id                AS event_id,
                ef.facility_id,
                e.title             AS event_name,
                e.description       AS event_description,
                ec.name             AS event_category,
                e.start_time,
                e.end_time,
                e.event_rate,
                e.is_active,
                e.created_at,
                e.updated_at

            FROM events e

            LEFT JOIN event_facility ef
                ON e.id = ef.event_id

            LEFT JOIN event_categories ec
                ON e.partner_id = ec.partner_id

            WHERE e.id = %s
              AND e.deleted_at IS NULL
            LIMIT 1  
            """, (eid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # Safe-parse dates (handle zero-dates)
            def safe_dt(val):
                if not val:
                    return None
                s = str(val)
                if s.startswith("0000"):
                    return None
                if isinstance(val, datetime):
                    return val
                return val

            start_dt   = safe_dt(new["start_time"])
            end_dt     = safe_dt(new["end_time"])
            created_at = safe_dt(new["created_at"]) or datetime.now()
            updated_at = safe_dt(new["updated_at"]) or datetime.now()
            event_rate = float(new["event_rate"] or 0)
            is_active  = 1 if str(new["is_active"] or "") == "1" else 0

            start_date = start_dt.date() if start_dt and hasattr(start_dt, "date") else None
            end_date   = end_dt.date()   if end_dt   and hasattr(end_dt,   "date") else None

            # STEP 2: CHECK IF RECORD EXISTS in GOLD
            gc.execute(f"""
            SELECT event_id
            FROM {GOLD_DB}.dim_event
            WHERE event_id = %s AND facility_id = %s
            """, (eid, new["facility_id"]))

            existing = gc.fetchone()

            if existing:
                print("♻️ Updating existing record")
                # FIX: UPDATE all columns that match DDL
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_event
                SET
                    event_name        = %s,
                    event_description = %s,
                    event_category    = %s,
                    event_start_date  = %s,
                    event_end_date    = %s,
                    event_start_time  = %s,
                    event_end_time    = %s,
                    event_rate        = %s,
                    is_active         = %s,
                    updated_at        = %s
                WHERE event_id = %s AND facility_id = %s
                """, (
                    new["event_name"], new["event_description"], new["event_category"],
                    start_date, end_date, start_dt, end_dt,
                    event_rate, is_active, updated_at,
                    eid, new["facility_id"]
                ))
            else:
                print("🆕 Inserting new record")
                # FIX: INSERT matches DDL exactly — no event_type_key, venue_name, expected_attendance
                gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_event (
                    event_id, facility_id, event_name, event_description, event_category,
                    event_start_date, event_end_date, event_start_time, event_end_time,
                    event_rate, is_active, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    event_name        = VALUES(event_name),
                    event_description = VALUES(event_description),
                    event_category    = VALUES(event_category),
                    event_start_date  = VALUES(event_start_date),
                    event_end_date    = VALUES(event_end_date),
                    event_start_time  = VALUES(event_start_time),
                    event_end_time    = VALUES(event_end_time),
                    event_rate        = VALUES(event_rate),
                    is_active         = VALUES(is_active),
                    updated_at        = VALUES(updated_at)
                """, (
                    eid, new["facility_id"],
                    new["event_name"], new["event_description"], new["event_category"],
                    start_date, end_date, start_dt, end_dt,
                    event_rate, is_active, created_at, updated_at
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
    dag_id="gold_upsert_dim_event",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_event",
        python_callable=upsert_dim_event
    )
