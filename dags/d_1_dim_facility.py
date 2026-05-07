from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_facility(**context):

    facility_ids = context["dag_run"].conf.get("facility_ids", [])

    if not facility_ids:
        print("No facility_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True, buffered=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        for fid in facility_ids:

            print(f"\n🔄 Processing facility_id: {fid}")

            # STEP 1: BUILD SOURCE SNAPSHOT from BRONZE
            # Matches initial load: geolocations for lat/lon/city/state,
            # countries for country name, role_user for operator_id,
            # hours_of_operation, facility_types, garage_code, logo, location
            bc.execute("""
            SELECT
                f.id                        AS facility_id,
                f.full_name                 AS facility_name,
                ft.facility_type,
                g.city,
                g.state,
                c.name                      AS country,
                CAST(f.capacity AS SIGNED)  AS capacity,
                ru.user_id                  AS operator_id,
                f.garage_code,
                f.logo,
                MIN(hoo.open_time)          AS open_time,
                MAX(hoo.close_time)         AS close_time,
                g.latitude,
                g.longitude,
                f.entrance_location         AS location,
                f.created_at                AS effective_start_date,

                MD5(CONCAT_WS('',
                    COALESCE(f.full_name,    ''),
                    COALESCE(ft.facility_type,''),
                    COALESCE(g.city,         ''),
                    COALESCE(g.state,        ''),
                    COALESCE(c.name,         ''),
                    COALESCE(CAST(f.capacity AS CHAR), ''),
                    COALESCE(f.garage_code,  ''),
                    COALESCE(f.logo,         '')
                )) AS record_hash

            FROM facilities f

            LEFT JOIN facility_types ft
                ON f.facility_type_id = ft.id

            LEFT JOIN geolocations g
                ON f.id = g.locatable_id
               AND g.locatable_type COLLATE utf8mb3_unicode_ci LIKE '%Facility'

            LEFT JOIN countries c
                ON c.country_code COLLATE utf8mb3_unicode_ci = f.country_code COLLATE utf8mb3_unicode_ci

            LEFT JOIN role_user ru
                ON f.owner_id = ru.user_id

            LEFT JOIN hours_of_operation hoo
                ON f.id = hoo.facility_id

            WHERE f.id = %s

            GROUP BY
                f.id, f.full_name, ft.facility_type,
                g.city, g.state, g.latitude, g.longitude,
                c.name, f.entrance_location, f.capacity,
                ru.user_id, f.created_at, f.garage_code, f.logo
            """, (fid,))

            new = bc.fetchone()

            if not new:
                print("⚠️ No source data found")
                continue

            # STEP 2: FETCH CURRENT RECORD from GOLD
            gc.execute(f"""
            SELECT *
            FROM {GOLD_DB}.dim_facility
            WHERE facility_id = %s AND is_current = 1
            """, (fid,))

            current = gc.fetchone()

            # STEP 3: CHANGE DETECTION
            if current and current["record_hash"] == new["record_hash"]:
                print("✅ No change detected")
                continue

            # STEP 4: EXPIRE OLD RECORD
            if current:
                print("♻️ Expiring old record")
                gc.execute(f"""
                UPDATE {GOLD_DB}.dim_facility
                SET
                    is_current            = 0,
                    effective_end_date    = NOW(),
                    dw_effective_end_date = NOW()
                WHERE facility_id = %s AND is_current = 1
                """, (fid,))

            # STEP 5: INSERT NEW RECORD
            print("🆕 Inserting new record")
            gc.execute(f"""
            INSERT INTO {GOLD_DB}.dim_facility (
                facility_id, facility_name, facility_type, city, state, country,
                capacity, operator_id, garage_code, logo,
                open_time, close_time, latitude, longitude, location,
                effective_start_date, effective_end_date,
                dw_effective_start_date, dw_effective_end_date, is_current, record_hash
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,'9999-12-31 23:59:59', NOW(),'9999-12-31 23:59:59', 1, %s)
            ON DUPLICATE KEY UPDATE
                facility_name        = VALUES(facility_name),
                facility_type        = VALUES(facility_type),
                city                 = VALUES(city),
                state                = VALUES(state),
                country              = VALUES(country),
                capacity             = VALUES(capacity),
                operator_id          = VALUES(operator_id),
                garage_code          = VALUES(garage_code),
                logo                 = VALUES(logo),
                open_time            = VALUES(open_time),
                close_time           = VALUES(close_time),
                latitude             = VALUES(latitude),
                longitude            = VALUES(longitude),
                location             = VALUES(location),
                record_hash          = VALUES(record_hash),
                updated_at           = NOW()
            """, (
                new["facility_id"], new["facility_name"], new["facility_type"],
                new["city"], new["state"], new["country"],
                new["capacity"], new["operator_id"], new["garage_code"], new["logo"],
                new["open_time"], new["close_time"], new["latitude"], new["longitude"],
                new["location"], new["effective_start_date"],
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
    dag_id="gold_upsert_dim_facility",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "scd2", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_facility",
        python_callable=upsert_dim_facility
    )
