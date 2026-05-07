from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_promo_code(**context):

    promo_code_ids = context["dag_run"].conf.get("promo_code_ids", [])

    if not promo_code_ids:
        print("No promo_code_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True, buffered=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pcid in promo_code_ids:

            print(f"\n🔄 Processing promo_code_id: {pcid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # FIX: added JOIN to promotions (for is_tax_applicable)
            #      and promotion_facilities (for facility_id) — matches initial load
            # FIX: effective_to uses deleted_at (not expired_at) — matches initial load
            bc.execute("""
            SELECT
                pc.id                   AS promo_code_id,
                pc.promocode            AS promo_code,
                pt.name                 AS promo_type,
                pc.discount_type,

                CASE
                    WHEN pc.discount_value IS NULL OR pc.discount_value = '' THEN 0
                    WHEN pc.discount_value LIKE '%\\%%'
                        THEN CAST(REPLACE(pc.discount_value, '%', '') AS DECIMAL(10,2))
                    WHEN pc.discount_value REGEXP '^[0-9.]+$'
                        THEN
                            CASE
                                WHEN CAST(pc.discount_value AS DECIMAL(20,2)) > 99999999
                                THEN 99999999.99
                                ELSE CAST(pc.discount_value AS DECIMAL(10,2))
                            END
                    ELSE 0
                END AS discount_value,

                CASE
                    WHEN pc.valid_from IS NULL OR pc.valid_from = '' THEN NULL
                    WHEN pc.valid_from LIKE '____-__-__%' THEN DATE(pc.valid_from)
                    WHEN pc.valid_from LIKE '%/%/%'
                        THEN STR_TO_DATE(pc.valid_from, '%m/%d/%Y')
                    ELSE NULL
                END AS start_date,

                CASE
                    WHEN pc.valid_to IS NULL OR pc.valid_to = '' THEN NULL
                    WHEN pc.valid_to LIKE '____-__-__%' THEN DATE(pc.valid_to)
                    WHEN pc.valid_to LIKE '%/%/%'
                        THEN STR_TO_DATE(pc.valid_to, '%m/%d/%Y')
                    ELSE NULL
                END AS end_date,

                pc.status               AS is_active,
                pr.is_tax_applicable    AS is_tax_fees_applicable,
                pf.facility_id,
                pc.created_at           AS effective_from,

                CASE
                    WHEN pc.deleted_at IS NULL THEN '2038-01-19 03:14:07'
                    ELSE pc.deleted_at
                END AS effective_to,

                MD5(CONCAT_WS('|',
                    pc.id, pt.name, pc.discount_type,
                    CASE
                        WHEN pc.discount_value IS NULL OR pc.discount_value = '' THEN 0
                        WHEN pc.discount_value LIKE '%\\%%'
                            THEN CAST(REPLACE(pc.discount_value, '%', '') AS DECIMAL(10,2))
                        WHEN pc.discount_value REGEXP '^[0-9.]+$'
                            THEN CASE
                                WHEN CAST(pc.discount_value AS DECIMAL(20,2)) > 99999999
                                THEN 99999999.99
                                ELSE CAST(pc.discount_value AS DECIMAL(10,2))
                            END
                        ELSE 0
                    END,
                    CASE
                        WHEN pc.valid_from IS NULL OR pc.valid_from = '' THEN NULL
                        WHEN pc.valid_from LIKE '____-__-__%' THEN DATE(pc.valid_from)
                        WHEN pc.valid_from LIKE '%/%/%' THEN STR_TO_DATE(pc.valid_from, '%m/%d/%Y')
                        ELSE NULL
                    END,
                    CASE
                        WHEN pc.valid_to IS NULL OR pc.valid_to = '' THEN NULL
                        WHEN pc.valid_to LIKE '____-__-__%' THEN DATE(pc.valid_to)
                        WHEN pc.valid_to LIKE '%/%/%' THEN STR_TO_DATE(pc.valid_to, '%m/%d/%Y')
                        ELSE NULL
                    END,
                    pc.status,
                    pr.is_tax_applicable,
                    pf.facility_id
                )) AS record_hash

            FROM promo_codes pc

            LEFT JOIN promotions pr
                ON pc.promotion_id = pr.id

            LEFT JOIN promo_types pt
                ON pc.promo_type_id = pt.id

            LEFT JOIN promotion_facilities pf
                ON pc.promotion_id = pf.promotion_id

            WHERE pc.id = %s
              AND pc.deleted_at IS NULL
            """, (pcid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
            SELECT *
            FROM {GOLD_DB}.dim_promo_code
            WHERE promo_code_id = %s AND is_current = 1
            """, (pcid,))

            current = gc.fetchone()

            # STEP 3: CHANGE DETECTION
            if current and current["record_hash"] == new["record_hash"]:
                print("✅ No change detected")
                continue

            # STEP 4: EXPIRE OLD RECORD
            if current:
                print("♻️ Expiring old record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_promo_code
                SET
                    is_current         = 0,
                    effective_to       = NOW(),
                    effective_end_date = NOW()
                WHERE promo_code_id = %s AND is_current = 1
                """, (pcid,))

            # STEP 5: INSERT NEW RECORD
            # FIX: columns are effective_start_date / effective_end_date
            # (not dw_effective_start_date / dw_effective_end_date)
            # FIX: added is_tax_fees_applicable and facility_id columns
            print("🆕 Inserting new record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_promo_code (
                promo_code_id, promo_code, promo_type, discount_type, discount_value,
                start_date, end_date, is_active, is_tax_fees_applicable, facility_id,
                effective_from, effective_to,
                effective_start_date, effective_end_date, is_current, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    NOW(), '2038-01-19 03:14:07', 1, %s)
            ON DUPLICATE KEY UPDATE
                promo_type             = VALUES(promo_type),
                discount_type          = VALUES(discount_type),
                discount_value         = VALUES(discount_value),
                is_active              = VALUES(is_active),
                is_tax_fees_applicable = VALUES(is_tax_fees_applicable),
                facility_id            = VALUES(facility_id),
                effective_from         = VALUES(effective_from),
                effective_to           = VALUES(effective_to),
                record_hash            = VALUES(record_hash),
                updated_at             = NOW()
            """, (
                new["promo_code_id"], new["promo_code"], new["promo_type"],
                new["discount_type"], new["discount_value"],
                new["start_date"], new["end_date"],
                new["is_active"],
                int(new["is_tax_fees_applicable"] or 0),
                new["facility_id"],
                new["effective_from"], new["effective_to"],
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
    dag_id="gold_upsert_dim_promo_code",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_promo_code",
        python_callable=upsert_dim_promo_code
    )
