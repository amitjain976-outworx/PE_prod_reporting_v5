from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_source_system(**context):

    source_ref_ids = context["dag_run"].conf.get("source_ref_ids", [])

    if not source_ref_ids:
        print("No source_ref_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for sid in source_ref_ids:

            print(f"\n🔄 Processing source_ref_id: {sid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # FIX: initial load sources from parking_device_types (not lpr_feeds)
            #   SELECT id, COALESCE(name,'UNKNOWN_DEVICE'), NULL, 1, '1.0'
            #   FROM parking_device_types
            # DDL columns: source_ref_id, source_name, api_version, is_current,
            #              reporting_api_version
            bc.execute("""
            SELECT
                pdt.id                              AS source_ref_id,
                COALESCE(pdt.name, 'UNKNOWN_DEVICE') AS source_name,
                NULL                                AS api_version,
                1                                   AS is_current,
                '1.0'                               AS reporting_api_version

            FROM parking_device_types pdt

            WHERE pdt.id = %s
            """, (sid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: UPSERT into GOLD
            # FIX: columns match DDL exactly — no source_type or vendor_name
            print("⬆️ Upserting record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_source_system (
                source_ref_id, source_name, api_version, is_current, reporting_api_version
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                source_name           = VALUES(source_name),
                api_version           = VALUES(api_version),
                is_current            = VALUES(is_current),
                reporting_api_version = VALUES(reporting_api_version)
            """, (
                new["source_ref_id"], new["source_name"], new["api_version"],
                new["is_current"], new["reporting_api_version"]
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
    dag_id="gold_upsert_dim_source_system",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_source_system",
        python_callable=upsert_dim_source_system
    )
