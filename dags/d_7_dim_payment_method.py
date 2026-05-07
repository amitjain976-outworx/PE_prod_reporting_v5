from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# NOTE: d_7 reads ONLY from GOLD (dim_processor → dim_payment_method).
# No bronze source reads needed — NO CHANGE REQUIRED for dual-connection fix.
from db_config import get_gold_connection, GOLD_DB


def upsert_dim_payment_method(**context):
    conf = context["dag_run"].conf or {}

    processor_ids = conf.get("processor_ids", conf.get("payment_method_ids", []))

    if not processor_ids:
        print("No processor_ids received")
        return

    conn   = get_gold_connection()
    cursor = conn.cursor(dictionary=True)

    method_types = ["CARD", "Google Pay", "Apple Pay", "CASH"]

    try:
        for pid in processor_ids:
            print(f"\n🔄 Processing processor_id: {pid}")

            cursor.execute(f"""
                SELECT processor_key, processor_id, processor_name, provider
                FROM {GOLD_DB}.dim_processor
                WHERE processor_key = %s OR processor_id = %s
                LIMIT 1
            """, (pid, pid))
            proc = cursor.fetchone()

            if not proc:
                print("⚠️ No processor found in dim_processor")
                continue

            for method_type in method_types:
                cursor.execute(f"""
                    SELECT payment_method_key
                    FROM {GOLD_DB}.dim_payment_method
                    WHERE payment_method_id = %s
                      AND method_type = %s
                    LIMIT 1
                """, (proc["processor_key"], method_type))

                existing = cursor.fetchone()

                if existing:
                    print(f"♻️ Updating existing record for {method_type}")
                    cursor.execute(f"""
                        UPDATE {GOLD_DB}.dim_payment_method
                        SET
                            provider_name    = %s,
                            provider_country = %s
                        WHERE payment_method_id = %s
                          AND method_type = %s
                    """, (
                        proc["provider"], "United States",
                        proc["processor_key"], method_type
                    ))
                else:
                    print(f"🆕 Inserting new record for {method_type}")
                    cursor.execute(f"""
                        INSERT INTO {GOLD_DB}.dim_payment_method (
                            payment_method_id, method_type, provider_name, provider_country
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (
                        proc["processor_key"], method_type,
                        proc["provider"], "United States"
                    ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="gold_upsert_dim_payment_method",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_payment_method",
        python_callable=upsert_dim_payment_method
    )
