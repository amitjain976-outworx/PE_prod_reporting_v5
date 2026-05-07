from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_parking_product(**context):

    product_ids = context["dag_run"].conf.get("product_ids", [])

    if not product_ids:
        print("No product_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for pid in product_ids:

            print(f"\n🔄 Processing product_id: {pid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            bc.execute("""
            SELECT
                sm.id                       AS product_id,
                LEFT(sm.service_type, 255)  AS name

            FROM service_masters sm

            WHERE sm.id = %s
              AND sm.deleted_at IS NULL
            """, (pid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: CHECK IF RECORD EXISTS in GOLD
            gc.execute(f"""
            SELECT product_id
            FROM {GOLD_DB}.dim_parking_product
            WHERE product_id = %s
            """, (pid,))

            existing = gc.fetchone()

            if existing:
                print("♻️ Updating existing record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_parking_product
                SET name = %s
                WHERE product_id = %s
                """, (new["name"], new["product_id"]))
            else:
                print("🆕 Inserting new record")
                gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_parking_product (product_id, name)
                VALUES (%s, %s)
                """, (new["product_id"], new["name"]))

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
    dag_id="gold_upsert_dim_parking_product",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_parking_product",
        python_callable=upsert_dim_parking_product
    )
