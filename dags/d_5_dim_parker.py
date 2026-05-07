from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_parker(**context):

    customer_ids = context["dag_run"].conf.get("customer_ids", [])

    if not customer_ids:
        print("No customer_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for cid in customer_ids:

            print(f"\n🔄 Processing customer_id: {cid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # FIX: added phone_number (u.phone), email (u.email)
            # FIX: added user_type = 5 filter (parkers only — matches initial load)
            bc.execute("""
            SELECT
                u.id            AS customer_id,
                u.user_type     AS customer_type,

                CASE
                    WHEN u.social_type IS NOT NULL     THEN 'SOCIAL'
                    WHEN u.user_prefrences IS NOT NULL THEN 'APP'
                    ELSE 'DIRECT'
                END AS signup_channel,

                u.name          AS parker_name,

                CASE
                    WHEN u.is_loyalty = 1 THEN 1
                    ELSE 0
                END AS loyalty_tier,

                CASE
                    WHEN u.status = 1 THEN 'ACTIVE'
                    ELSE 'INACTIVE'
                END AS account_status,

                u.city          AS home_city,
                u.phone         AS phone_number,
                u.email         AS email,
                u.created_at    AS effective_start_date

            FROM users u

            WHERE u.id = %s
              AND u.deleted_at IS NULL
              AND COALESCE(u.is_partner, 0) = 0
              AND u.user_type = 5
            """, (cid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found (user not found, is a partner, or not user_type=5)")
                continue

            # STEP 2: CHECK IF RECORD EXISTS in GOLD
            gc.execute(f"""
            SELECT customer_id
            FROM {GOLD_DB}.dim_parker
            WHERE customer_id = %s
            """, (cid,))

            existing = gc.fetchone()

            if existing:
                print("♻️ Updating existing record")
                # FIX: added phone_number and email to UPDATE
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_parker
                SET
                    customer_type  = %s,
                    signup_channel = %s,
                    parker_name    = %s,
                    loyalty_tier   = %s,
                    account_status = %s,
                    home_city      = %s,
                    phone_number   = %s,
                    email          = %s
                WHERE customer_id = %s
                """, (
                    new["customer_type"], new["signup_channel"], new["parker_name"],
                    new["loyalty_tier"], new["account_status"], new["home_city"],
                    new["phone_number"], new["email"],
                    new["customer_id"]
                ))
            else:
                print("🆕 Inserting new record")
                # FIX: added phone_number and email to INSERT
                gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_parker (
                    customer_id, customer_type, signup_channel, parker_name,
                    loyalty_tier, account_status, home_city,
                    phone_number, email, effective_start_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    customer_type  = VALUES(customer_type),
                    signup_channel = VALUES(signup_channel),
                    parker_name    = VALUES(parker_name),
                    loyalty_tier   = VALUES(loyalty_tier),
                    account_status = VALUES(account_status),
                    home_city      = VALUES(home_city),
                    phone_number   = VALUES(phone_number),
                    email          = VALUES(email)
                """, (
                    new["customer_id"], new["customer_type"], new["signup_channel"],
                    new["parker_name"], new["loyalty_tier"], new["account_status"],
                    new["home_city"], new["phone_number"], new["email"],
                    new["effective_start_date"]
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
    dag_id="gold_upsert_dim_parker",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_parker",
        python_callable=upsert_dim_parker
    )
