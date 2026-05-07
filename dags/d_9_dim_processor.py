from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_processor(**context):

    # FIX: initial load sources from facility_payment_type directly by id
    # so conf key is processor_ids (facility_payment_type.id values)
    processor_ids = context["dag_run"].conf.get("processor_ids", [])

    if not processor_ids:
        print("No processor_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pid in processor_ids:

            print(f"\n🔄 Processing processor_id (facility_payment_type.id): {pid}")

            # STEP 1: FETCH from BRONZE facility_payment_type
            # FIX: initial load uses:
            #   SELECT DISTINCT id, LEFT(payment_type,100), LEFT(payment_type,100)
            #   FROM facility_payment_type
            # processor_name and provider are BOTH set to payment_type value
            bc.execute("""
            SELECT
                id,
                LEFT(payment_type, 100) AS payment_type
            FROM facility_payment_type
            WHERE id = %s
            """, (pid,))

            row = bc.fetchone()
            if not row:
                print(f"⚠️ No facility_payment_type found for id={pid}")
                continue

            fpt_id   = row["id"]
            fpt_name = row["payment_type"]

            # STEP 2: UPSERT INTO GOLD dim_processor
            # processor_name = provider = payment_type (matches initial load)
            print("⬆️ Upserting processor")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_processor (processor_id, processor_name, provider)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                processor_name = VALUES(processor_name),
                provider       = VALUES(provider)
            """, (fpt_id, fpt_name, fpt_name))

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
    dag_id="gold_upsert_dim_processor",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_processor",
        python_callable=upsert_dim_processor
    )
