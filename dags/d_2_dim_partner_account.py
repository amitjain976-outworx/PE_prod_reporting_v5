from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_partner_account(**context):

    account_ids = context["dag_run"].conf.get("account_ids", [])

    if not account_ids:
        print("No account_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for aid in account_ids:

            print(f"\n🔄 Processing account_id: {aid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # FIX: partner_id logic matches initial load:
            #   if is_partner=1 AND user_type=3 → use uid as partner_id
            #   else → use created_by (fallback to uid)
            # record_hash matches initial load: name|user_type|partner_id|country|status
            bc.execute("""
            SELECT
                u.id                AS account_id_source,
                u.name              AS account_name,
                u.user_type         AS account_type,

                CASE
                    WHEN u.is_partner = 1 AND u.user_type = 3 THEN u.id
                    WHEN u.created_by IS NOT NULL              THEN u.created_by
                    ELSE u.id
                END AS partner_id,

                u.country,

                CASE
                    WHEN u.status = 1 THEN 'ACTIVE'
                    ELSE 'INACTIVE'
                END AS status,

                CASE
                    WHEN CAST(u.created_at AS CHAR) = '0000-00-00 00:00:00'
                         OR u.created_at IS NULL
                    THEN NULL
                    ELSE u.created_at
                END AS created_at,

                CASE
                    WHEN CAST(u.updated_at AS CHAR) = '0000-00-00 00:00:00'
                         OR u.updated_at IS NULL
                    THEN NULL
                    ELSE u.updated_at
                END AS updated_at,

                MD5(CONCAT_WS('|',
                    u.name,
                    u.user_type,
                    CASE
                        WHEN u.is_partner = 1 AND u.user_type = 3 THEN u.id
                        WHEN u.created_by IS NOT NULL              THEN u.created_by
                        ELSE u.id
                    END,
                    u.country,
                    CASE WHEN u.status = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END
                )) AS record_hash

            FROM users u

            WHERE u.id = %s
              AND u.deleted_at IS NULL
              AND u.is_partner = 1
            """, (aid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found (or user is not a partner)")
                continue

            # STEP 2: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
            SELECT *
            FROM {GOLD_DB}.dim_partner_account
            WHERE account_id_source = %s AND is_current = 1
            """, (aid,))

            current = gc.fetchone()

            # STEP 3: CHANGE DETECTION
            if current and current["record_hash"] == new["record_hash"]:
                print("✅ No change detected")
                continue

            # STEP 4: EXPIRE OLD RECORD (SCD Type 2)
            if current:
                print("♻️ Expiring old record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_partner_account
                SET
                    is_current         = 0,
                    effective_end_date = NOW()
                WHERE account_id_source = %s AND is_current = 1
                """, (aid,))

            # STEP 5: INSERT NEW RECORD
            # FIX: columns are effective_start_date / effective_end_date
            # (not dw_effective_start_date / dw_effective_end_date)
            print("🆕 Inserting new record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_partner_account (
                account_id_source, account_name, account_type, partner_id, country,
                status, created_at, updated_at,
                effective_start_date, effective_end_date, is_current, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), '2038-01-19 03:14:07', 1, %s)
            """, (
                new["account_id_source"], new["account_name"], new["account_type"],
                new["partner_id"], new["country"], new["status"],
                new["created_at"], new["updated_at"], new["record_hash"]
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
    dag_id="gold_upsert_dim_partner_account",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_partner_account",
        python_callable=upsert_dim_partner_account
    )
