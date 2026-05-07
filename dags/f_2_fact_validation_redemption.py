from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# FIX: import get_bronze_connection — original used single get_gold_connection()
# and ran FROM {BRONZE_DB}.tickets cross-server on the gold connection.
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_fact_validation_redemption(**context):

    ticket_ids = context["dag_run"].conf.get("ticket_ids", [])

    if not ticket_ids:
        print("⚠️ No ticket_ids received")
        return

    print(f"📥 Processing ticket_ids: {ticket_ids}")

    # FIX: two separate connections — bronze (remote) and gold (local)
    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for tid in ticket_ids:

            # ── Fetch ticket from BRONZE ─────────────────────────────────────
            bc.execute("""
                SELECT
                    t.id,
                    t.facility_id,
                    t.promocode,
                    t.paid_date,
                    t.created_at,
                    t.paid_amount,
                    t.session_id,
                    t.reservation_id,
                    t.deleted_at
                FROM tickets t
                WHERE t.id = %s
                  AND t.deleted_at IS NULL
                  AND t.promocode IS NOT NULL
                LIMIT 1
            """, (tid,))
            ticket = bc.fetchone()

            if not ticket:
                print(f"⚠️ ticket_id={tid} not found, deleted, or has no promocode — skipping")
                continue

            # ── Resolve dim/fact keys from GOLD ─────────────────────────────
            redemption_ts = ticket["paid_date"] or ticket["created_at"]

            # date_key
            gc.execute(f"""
                SELECT date_key FROM {GOLD_DB}.dim_date
                WHERE full_date = DATE(%s) LIMIT 1
            """, (redemption_ts,))
            r = gc.fetchone()
            date_key = r["date_key"] if r else None

            # facility_key
            gc.execute(f"""
                SELECT facility_key FROM {GOLD_DB}.dim_facility
                WHERE facility_id = %s AND is_current = 1 LIMIT 1
            """, (ticket["facility_id"],))
            r = gc.fetchone()
            facility_key = r["facility_key"] if r else None

            # promo_key
            gc.execute(f"""
                SELECT promo_key FROM {GOLD_DB}.dim_promo_code
                WHERE promo_code = %s AND is_current = 1 LIMIT 1
            """, (ticket["promocode"],))
            r = gc.fetchone()
            promo_key = r["promo_key"] if r else None

            # canonical_session_key via fact_parking_session.user_session_id
            gc.execute(f"""
                SELECT canonical_session_key FROM {GOLD_DB}.fact_parking_session
                WHERE user_session_id = %s LIMIT 1
            """, (ticket["session_id"],))
            r = gc.fetchone()
            canonical_session_key = r["canonical_session_key"] if r else None

            # reservation_key
            reservation_key = None
            if ticket["reservation_id"]:
                gc.execute(f"""
                    SELECT reservation_key FROM {GOLD_DB}.fact_reservation
                    WHERE source_reservation_id = %s LIMIT 1
                """, (ticket["reservation_id"],))
                r = gc.fetchone()
                reservation_key = r["reservation_key"] if r else None

            # approved_flag via BRONZE promotions table
            approved_flag = 0
            if ticket["promocode"]:
                bc.execute("""
                    SELECT status FROM promotions WHERE name = %s LIMIT 1
                """, (ticket["promocode"],))
                pr = bc.fetchone()
                if pr:
                    status_val = pr["status"] if isinstance(pr, dict) else pr[0]
                    approved_flag = 1 if status_val == 1 else 0

            # ── UPSERT into GOLD fact_validation_redemption ──────────────────
            gc.execute(f"""
                INSERT INTO {GOLD_DB}.fact_validation_redemption (
                    redemption_ts_utc, date_key, facility_key, promo_key,
                    canonical_session_key, reservation_key,
                    redemption_amount, approved_flag, rule_version, source_ticket_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
                ON DUPLICATE KEY UPDATE
                    redemption_ts_utc   = VALUES(redemption_ts_utc),
                    date_key            = VALUES(date_key),
                    facility_key        = VALUES(facility_key),
                    promo_key           = VALUES(promo_key),
                    reservation_key     = VALUES(reservation_key),
                    redemption_amount   = VALUES(redemption_amount),
                    approved_flag       = VALUES(approved_flag)
            """, (
                redemption_ts, date_key, facility_key, promo_key,
                canonical_session_key, reservation_key,
                float(ticket["paid_amount"] or 0),
                approved_flag,
                int(tid),
            ))

        gold_conn.commit()
        print(f"✅ Successfully upserted {len(ticket_ids)} records")

    except Exception as e:
        gold_conn.rollback()
        print(f"❌ Error occurred: {str(e)}")
        raise

    finally:
        bc.close()
        gc.close()
        bronze_conn.close()
        gold_conn.close()


with DAG(
    dag_id="gold_upsert_fact_validation_redemption",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact", "kafka"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_validation_redemption",
        python_callable=upsert_fact_validation_redemption
    )
