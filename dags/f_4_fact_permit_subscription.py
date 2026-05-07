"""
DAG: gold_upsert_fact_permit_subscription
CDC incremental upsert for fact_permit_subscription.

FIX: Original used single get_gold_connection() and ran:
     FROM {BRONZE_DB}.permit_requests pr
     LEFT JOIN {BRONZE_DB}.anet_transactions at ...
     These cross-server JOINs fail when bronze is on a remote server.

     Solution: fetch source row from bronze, resolve dim keys from gold,
     then write to gold. All joins are split into separate queries.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_fact_permit_subscription(**context):
    permit_ids = context["dag_run"].conf.get("permit_ids", [])

    if not permit_ids:
        print("No permit_ids received")
        return

    # FIX: two separate connections — bronze (remote) and gold (local)
    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pid in permit_ids:
            print(f"\n🔄 Processing permit_id: {pid}")

            # ── Fetch permit_request from BRONZE ─────────────────────────────
            bc.execute("""
                SELECT
                    pr.id, pr.user_id, pr.facility_id, pr.partner_id,
                    pr.desired_start_date, pr.desired_end_date,
                    pr.deleted_at, pr.cancelled_at, pr.status,
                    pr.permit_rate, pr.anet_transaction_id, pr.permit_final_amount
                FROM permit_requests pr
                WHERE pr.id = %s AND pr.deleted_at IS NULL
                LIMIT 1
            """, (pid,))
            pr = bc.fetchone()

            if not pr:
                print(f"⚠️ permit_id={pid} not found or deleted — skipping")
                continue

            # ── Resolve paid_amount from BRONZE anet_transactions ─────────────
            anet_total = 0
            if pr["anet_transaction_id"]:
                bc.execute("""
                    SELECT total FROM anet_transactions WHERE id = %s LIMIT 1
                """, (pr["anet_transaction_id"],))
                anet_row = bc.fetchone()
                if anet_row:
                    anet_total = float(anet_row["total"] or 0) if isinstance(anet_row, dict) else float(anet_row[0] or 0)

            # ── Resolve dim keys from GOLD ────────────────────────────────────
            # parker_key via dim_parker.customer_id = pr.user_id
            gc.execute(f"""
                SELECT parker_key FROM {GOLD_DB}.dim_parker
                WHERE customer_id = %s LIMIT 1
            """, (pr["user_id"],))
            r = gc.fetchone()
            parker_key = r["parker_key"] if r else None

            # facility_key via dim_facility.facility_id = pr.facility_id
            gc.execute(f"""
                SELECT facility_key FROM {GOLD_DB}.dim_facility
                WHERE facility_id = %s AND is_current = 1 LIMIT 1
            """, (pr["facility_id"],))
            r = gc.fetchone()
            facility_key = r["facility_key"] if r else None

            # product_key via dim_parking_product.product_id = pr.partner_id
            gc.execute(f"""
                SELECT product_key FROM {GOLD_DB}.dim_parking_product
                WHERE product_id = %s LIMIT 1
            """, (pr["partner_id"],))
            r = gc.fetchone()
            product_key = r["product_key"] if r else None

            # ── Derive status ─────────────────────────────────────────────────
            if pr["deleted_at"]:
                status = "deleted"
            elif pr["cancelled_at"]:
                status = "cancelled"
            elif str(pr["status"]) == "1":
                status = "active"
            elif str(pr["status"]) == "2":
                status = "suspended"
            else:
                status = "pending"

            billed_amount = float(pr["permit_rate"] or 0)
            paid_amount   = anet_total
            balance       = billed_amount - paid_amount

            # ── UPSERT into GOLD fact_permit_subscription ─────────────────────
            gc.execute(f"""
                INSERT INTO {GOLD_DB}.fact_permit_subscription (
                    parker_key, facility_key, facility_group_key, product_key,
                    period_start_date_key, period_end_date_key,
                    status, billed_amount, paid_amount, balance,
                    spaces_entitled, source_permit_id
                )
                VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
                ON DUPLICATE KEY UPDATE
                    status                = VALUES(status),
                    billed_amount         = VALUES(billed_amount),
                    paid_amount           = VALUES(paid_amount),
                    balance               = VALUES(balance),
                    period_start_date_key = VALUES(period_start_date_key),
                    period_end_date_key   = VALUES(period_end_date_key),
                    facility_key          = VALUES(facility_key),
                    parker_key            = VALUES(parker_key),
                    product_key           = VALUES(product_key)
            """, (
                parker_key, facility_key, product_key,
                pr["desired_start_date"], pr["desired_end_date"],
                status, billed_amount, paid_amount, balance,
                int(pid),
            ))

        gold_conn.commit()
        print("✅ Done")

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
    dag_id="gold_upsert_fact_permit_subscription",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_permit_subscription",
        python_callable=upsert_fact_permit_subscription
    )
