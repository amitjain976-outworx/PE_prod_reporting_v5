from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_device(**context):

    device_ids = context["dag_run"].conf.get("device_ids", [])

    if not device_ids:
        print("No device_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    # bc = bronze_conn.cursor(dictionary=True)
    bc = bronze_conn.cursor(dictionary=True, buffered=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for did in device_ids:

            print(f"\n🔄 Processing device_id: {did}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            bc.execute("""
            SELECT
                fc.id                   AS device_id,
                pdt.name                AS device_type,
                NULL                    AS manufacturer,
                pd.serial_number        AS model_number,
                pd.created_at           AS install_date,
                mdv.id                  AS firmware_version,
                g.is_active             AS status

            FROM im30_facility_configurations fc

            LEFT JOIN parking_devices pd
                ON fc.facility_id = pd.facility_id

            LEFT JOIN parking_device_types pdt
                ON pd.device_type_id = pdt.id

            LEFT JOIN mobile_device_version mdv
                ON pd.partner_id = mdv.partner_id

            LEFT JOIN gates g
                ON pd.gate_id = g.id

            WHERE fc.id = %s
            """, (did,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: CHECK IF RECORD EXISTS in GOLD
            gc.execute(f"""
            SELECT device_id
            FROM {GOLD_DB}.dim_device
            WHERE device_id = %s
            """, (did,))

            existing = gc.fetchone()

            if existing:
                print("♻️ Updating existing record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_device
                SET
                    device_type      = %s,
                    manufacturer     = %s,
                    model_number     = %s,
                    install_date     = %s,
                    firmware_version = %s,
                    status           = %s
                WHERE device_id = %s
                """, (
                    new["device_type"], new["manufacturer"], new["model_number"],
                    new["install_date"], new["firmware_version"], new["status"],
                    new["device_id"]
                ))
            else:
                print("🆕 Inserting new record")
                gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_device (
                    device_id, device_type, manufacturer, model_number,
                    install_date, firmware_version, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    new["device_id"], new["device_type"], new["manufacturer"],
                    new["model_number"], new["install_date"],
                    new["firmware_version"], new["status"]
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
    dag_id="gold_upsert_dim_device",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_device",
        python_callable=upsert_dim_device
    )
