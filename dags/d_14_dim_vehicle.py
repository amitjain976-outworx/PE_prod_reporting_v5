from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_vehicle(**context):
    conf = context["dag_run"].conf or {}

    vehicle_type_ids   = conf.get("vehicle_type_ids", [])
    permit_vehicle_ids = conf.get("permit_vehicle_ids", [])
    lpr_feed_ids       = conf.get("lpr_feed_vehicle_ids", [])

    if not any([vehicle_type_ids, permit_vehicle_ids, lpr_feed_ids]):
        print("No vehicle IDs received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        vehicle_type_ids_to_process = set(int(x) for x in vehicle_type_ids if x is not None)

        # Resolve permit_vehicle_ids -> vehicle_type_id via BRONZE
        if permit_vehicle_ids:
            fmt = ",".join(["%s"] * len(permit_vehicle_ids))
            bc.execute(f"""
                SELECT DISTINCT pv.vehicle_type_id AS vehicle_type_id
                FROM permit_vehicles pv
                WHERE pv.id IN ({fmt})
                  AND pv.vehicle_type_id IS NOT NULL
            """, tuple(permit_vehicle_ids))
            for row in bc.fetchall():
                vehicle_type_ids_to_process.add(int(row["vehicle_type_id"]))

        # Resolve lpr_feed_ids -> vehicle_type_id via BRONZE
        if lpr_feed_ids:
            fmt = ",".join(["%s"] * len(lpr_feed_ids))
            bc.execute(f"""
                SELECT DISTINCT pv.vehicle_type_id AS vehicle_type_id
                FROM lpr_feeds lf
                JOIN permit_vehicles pv
                    ON LOWER(pv.license_plate_number) = LOWER(lf.license_plate)
                WHERE lf.id IN ({fmt})
                  AND pv.vehicle_type_id IS NOT NULL
            """, tuple(lpr_feed_ids))
            for row in bc.fetchall():
                vehicle_type_ids_to_process.add(int(row["vehicle_type_id"]))

        print(f"\n📋 Total unique vehicle_type_ids to process: {len(vehicle_type_ids_to_process)}")

        for vtid in vehicle_type_ids_to_process:
            print(f"\n🔄 Processing vehicle_type_id: {vtid}")

            # STEP 1: Fetch vehicle data from BRONZE
            bc.execute("""
                SELECT
                    src.vehicle_id,
                    MAX(src.vehicle_type)  AS vehicle_type,
                    MAX(src.vehicle_code)  AS vehicle_code,
                    MAX(src.is_ev_flag)    AS is_ev_flag
                FROM (
                    SELECT
                        pv.vehicle_type_id AS vehicle_id,
                        mvt.name           AS vehicle_type,
                        mvt.code           AS vehicle_code,
                        CASE
                            WHEN mvt.code IS NULL THEN NULL
                            WHEN UPPER(mvt.code) LIKE '%%EV%%'
                                 OR UPPER(mvt.code) IN ('PHEV', 'HEV')
                            THEN 1
                            ELSE 0
                        END AS is_ev_flag
                    FROM permit_vehicles pv
                    LEFT JOIN mst_vehicle_types mvt
                        ON pv.vehicle_type_id = mvt.id
                    WHERE pv.vehicle_type_id = %s
                ) src
                WHERE src.vehicle_id IS NOT NULL
                GROUP BY src.vehicle_id
            """, (vtid,))

            new = bc.fetchone()
            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: Check existence in GOLD
            gc.execute(f"""
                SELECT vehicle_id, vehicle_type, vehicle_code, is_ev_flag
                FROM {GOLD_DB}.dim_vehicle
                WHERE vehicle_id = %s
                LIMIT 1
            """, (vtid,))
            existing = gc.fetchone()

            if existing:
                print("♻️ Updating existing record")
                gc.execute(f"""
                    UPDATE {GOLD_DB}.dim_vehicle
                    SET
                        vehicle_type = %s,
                        vehicle_code = %s,
                        is_ev_flag   = %s
                    WHERE vehicle_id = %s
                """, (
                    new["vehicle_type"] or existing["vehicle_type"],
                    new["vehicle_code"] or existing["vehicle_code"],
                    new["is_ev_flag"] if new["is_ev_flag"] is not None else existing["is_ev_flag"],
                    vtid
                ))
            else:
                print("🆕 Inserting new record")
                gc.execute(f"""
                    INSERT INTO {GOLD_DB}.dim_vehicle (
                        vehicle_id, vehicle_type, vehicle_code, is_ev_flag
                    )
                    VALUES (%s, %s, %s, %s)
                """, (
                    new["vehicle_id"], new["vehicle_type"],
                    new["vehicle_code"], new["is_ev_flag"]
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
    dag_id="gold_upsert_dim_vehicle",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_vehicle",
        python_callable=upsert_dim_vehicle
    )
