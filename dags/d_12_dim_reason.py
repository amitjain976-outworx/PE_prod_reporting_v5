from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB


def upsert_dim_reason(**context):

    conf = context["dag_run"].conf

    ticket_ids     = conf.get("ticket_reason_ids", [])
    warning_ids    = conf.get("warning_reason_ids", [])
    infraction_ids = conf.get("warning_infraction_ids", [])
    master_ids     = conf.get("infraction_reason_ids", [])

    if not any([ticket_ids, warning_ids, infraction_ids, master_ids]):
        print("No reason IDs received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    # Each tuple: (id_list, bronze_query_without_BRONZE_DB_prefix)
    sources = [
        (
            ticket_ids,
            """
            SELECT
                t.id         AS reason_id_source,
                t.reason     AS reason_name,
                t.penalty_fee,
                'ticket'     AS reason_category
            FROM ticket_citation_infraction_reasons t
            WHERE t.id = %s
            """
        ),
        (
            warning_ids,
            """
            SELECT
                w.id         AS reason_id_source,
                w.reason     AS reason_name,
                w.penalty_fee,
                'warning'    AS reason_category
            FROM warning_infraction_reasons w
            WHERE w.id = %s
            """
        ),
        (
            infraction_ids,
            """
            SELECT
                wi.id               AS reason_id_source,
                wi.reason           AS reason_name,
                wi.penalty_fee,
                wi.infraction_name  AS reason_category
            FROM warning_infractions wi
            WHERE wi.id = %s
            """
        ),
        (
            master_ids,
            """
            SELECT
                ir.id        AS reason_id_source,
                ir.reason    AS reason_name,
                ir.penalty_fee,
                'master'     AS reason_category
            FROM infraction_reasons ir
            WHERE ir.id = %s
            """
        ),
    ]

    try:
        for ids_list, query in sources:

            for rid in ids_list:

                print(f"\n🔄 Processing reason_id: {rid}")

                # STEP 1: Fetch source data from BRONZE
                bc.execute(query, (rid,))
                new = bc.fetchone()

                if not new:
                    print("⚠️ No source data found")
                    continue

                # STEP 2: Normalize data
                reason_name = new["reason_name"]
                if reason_name:
                    reason_name = reason_name.strip()
                    reason_name = " ".join(reason_name.split())

                # STEP 3: UPSERT into GOLD
                print("⬆️ Upserting record")
                gc.execute(f"""
                INSERT INTO {GOLD_DB}.dim_reason (
                    reason_id_source, reason_name, penalty_fee, reason_category
                )
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    penalty_fee      = VALUES(penalty_fee),
                    reason_category  = VALUES(reason_category),
                    reason_id_source = VALUES(reason_id_source)
                """, (
                    new["reason_id_source"], reason_name,
                    new["penalty_fee"], new["reason_category"]
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
    dag_id="gold_upsert_dim_reason",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_reason",
        python_callable=upsert_dim_reason
    )
