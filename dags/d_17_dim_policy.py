"""
DAG: gold_upsert_dim_policy
CDC upsert for dim_policy.

SOURCE : SOURCE_DB.business_policy  (WHERE deleted_at IS NULL)
TARGET : GOLD_DB.dim_policy

MODES:
  Targeted  — triggered with { "policy_ids": [1, 2, 3] } in dag_run.conf.
              Upserts only those specific business_policy rows.
  Watermark — triggered with no conf (or empty conf).
              Scans business_policy.id > last watermark in keyset batches.

SCD Type 2 scaffolding (effective_date, is_current) is carried through as-is
from the source row; full SCD2 versioning is out of scope for this load.

Zero-date guard: validity_start_date / validity_end_date values of
'0000-00-00 00:00:00' are converted to NULL before insert.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BATCH_SIZE = 500


# ============================================================
# HELPERS
# ============================================================

def _get(row, key, idx):
    if isinstance(row, dict):
        return row.get(key)
    return row[idx] if row and len(row) > idx else None


def safe_dt(value) -> Optional[datetime]:
    """Parse datetime; returns None for zero-dates and NULL."""
    if value in (None, "", "0000-00-00 00:00:00", "0000-00-00"):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


# ============================================================
# DDL GUARD
# ============================================================

def ensure_dim_policy(gc, gold_conn) -> None:
    """Create dim_policy if it doesn't exist; backfill any missing columns."""
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.dim_policy (
            policy_key          INT          NOT NULL AUTO_INCREMENT,
            policy_id           INT          NOT NULL,
            policy_name         VARCHAR(100) NULL,
            user_type           INT          NULL,
            consumption_channel VARCHAR(11)  NULL,
            discount_type       VARCHAR(11)  NULL,
            discount_value      VARCHAR(11)  NULL,
            validity_start_date DATETIME     NULL,
            validity_end_date   DATETIME     NULL,
            partner_id          INT          NULL,
            created_by          INT          NULL,
            rm_id               INT UNSIGNED NULL,
            status              INT          NULL DEFAULT 0,
            effective_date      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_current          TINYINT(1)   NOT NULL DEFAULT 1,
            created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                                      ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (policy_key),
            INDEX idx_policy_id  (policy_id),
            INDEX idx_partner_id (partner_id),
            INDEX idx_is_current (is_current)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COMMENT='Dimension table for Business Policy'
    """)
    gold_conn.commit()
    log.info("dim_policy DDL guard done")


# ============================================================
# WATERMARK TABLE
# ============================================================

def ensure_watermark_table(gc, gold_conn) -> None:
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.etl_watermarks (
            job_name      VARCHAR(100) PRIMARY KEY,
            last_anet_id  BIGINT NOT NULL DEFAULT 0,
            updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                         ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    gold_conn.commit()


def get_last_policy_id(gc, gold_conn) -> int:
    job_name = "gold_upsert_dim_policy"
    ensure_watermark_table(gc, gold_conn)
    gc.execute(f"""
        SELECT last_anet_id FROM {GOLD_DB}.etl_watermarks
        WHERE job_name = %s LIMIT 1
    """, (job_name,))
    row = gc.fetchone()
    return int(_get(row, "last_anet_id", 0) or 0) if row else 0


def set_last_policy_id(gc, gold_conn, last_id: int) -> None:
    job_name = "gold_upsert_dim_policy"
    gc.execute(f"""
        INSERT INTO {GOLD_DB}.etl_watermarks (job_name, last_anet_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_anet_id = VALUES(last_anet_id)
    """, (job_name, int(last_id)))
    gold_conn.commit()


# ============================================================
# UPSERT SQL
# Unique key is policy_id (not policy_key — that's the surrogate PK).
# On conflict: update all business attributes; preserve policy_key / created_at.
# ============================================================

_UPSERT_SQL = f"""
    INSERT INTO {GOLD_DB}.dim_policy (
        policy_id, policy_name, user_type, consumption_channel,
        discount_type, discount_value,
        validity_start_date, validity_end_date,
        partner_id, created_by, rm_id, status,
        effective_date, is_current, created_at, updated_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        policy_name         = VALUES(policy_name),
        user_type           = VALUES(user_type),
        consumption_channel = VALUES(consumption_channel),
        discount_type       = VALUES(discount_type),
        discount_value      = VALUES(discount_value),
        validity_start_date = VALUES(validity_start_date),
        validity_end_date   = VALUES(validity_end_date),
        partner_id          = VALUES(partner_id),
        created_by          = VALUES(created_by),
        rm_id               = VALUES(rm_id),
        status              = VALUES(status),
        updated_at          = NOW()
"""


def _build_row(r: dict) -> tuple:
    """Convert a business_policy source row into the 16-value insert tuple."""
    now = datetime.now()
    return (
        int(r["id"]),
        str(r["policy_name"])[:100]         if r.get("policy_name")         else None,
        int(r["user_type"])                  if r.get("user_type") is not None else None,
        str(r["consumption_channel"])[:11]   if r.get("consumption_channel") else None,
        str(r["discount_type"])[:11]         if r.get("discount_type")       else None,
        str(r["discount_value"])[:11]        if r.get("discount_value")      else None,
        safe_dt(r.get("validity_start_date")),
        safe_dt(r.get("validity_end_date")),
        int(r["partner_id"])   if r.get("partner_id") is not None else None,
        int(r["created_by"])   if r.get("created_by") is not None else None,
        int(r["rm_id"])        if r.get("rm_id")       is not None else None,
        int(r["status"])       if r.get("status")      is not None else 0,
        now,    # effective_date
        1,      # is_current
        now,    # created_at
        now,    # updated_at
    )


def _do_upsert(gc, gold_conn, rows: List[tuple]) -> int:
    if not rows:
        return 0
    gc.executemany(_UPSERT_SQL, rows)
    gold_conn.commit()
    return gc.rowcount


# ============================================================
# SOURCE FETCH
# ============================================================

_SOURCE_COLS = """
    SELECT
        bp.id,
        bp.policy_name,
        bp.user_type,
        bp.consumption_channel,
        bp.discount_type,
        bp.discount_value,
        CASE WHEN CAST(bp.validity_start_date AS CHAR(19)) = '0000-00-00 00:00:00'
             THEN NULL ELSE bp.validity_start_date END AS validity_start_date,
        CASE WHEN CAST(bp.validity_end_date   AS CHAR(19)) = '0000-00-00 00:00:00'
             THEN NULL ELSE bp.validity_end_date   END AS validity_end_date,
        bp.partner_id,
        bp.created_by,
        bp.rm_id,
        bp.status
    FROM business_policy bp
    WHERE bp.deleted_at IS NULL
"""

_ROW_KEYS = ["id", "policy_name", "user_type", "consumption_channel",
             "discount_type", "discount_value", "validity_start_date",
             "validity_end_date", "partner_id", "created_by", "rm_id", "status"]


def _normalise_rows(raw) -> List[dict]:
    if not raw:
        return []
    if isinstance(raw[0], dict):
        return raw
    return [dict(zip(_ROW_KEYS, r)) for r in raw]


# ============================================================
# TARGETED MODE
# ============================================================

def load_targeted(bc, gc, gold_conn, policy_ids: List[int]) -> Tuple[int, int]:
    ensure_dim_policy(gc, gold_conn)
    if not policy_ids:
        return 0, 0

    log.info(f"[TARGETED] {len(policy_ids)} policy_ids")
    ph = ",".join(["%s"] * len(policy_ids))
    bc.execute(f"{_SOURCE_COLS} AND bp.id IN ({ph})", tuple(policy_ids))
    rows = _normalise_rows(bc.fetchall())

    out = [_build_row(r) for r in rows]
    inserted = _do_upsert(gc, gold_conn, out)
    log.info(f"[TARGETED] Done rows_read={len(rows)} rows_upserted={inserted}")
    return len(rows), inserted


# ============================================================
# WATERMARK MODE
# ============================================================

def load_watermark(bc, gc, gold_conn) -> Tuple[int, int]:
    ensure_dim_policy(gc, gold_conn)
    last_id   = get_last_policy_id(gc, gold_conn)
    rows_read = rows_inserted = 0

    log.info(f"[WATERMARK] Starting from policy_id > {last_id}")

    while True:
        bc.execute(f"{_SOURCE_COLS} AND bp.id > %s ORDER BY bp.id LIMIT %s",
                   (last_id, BATCH_SIZE))
        raw = bc.fetchall()
        if not raw:
            break

        rows = _normalise_rows(raw)
        out  = [_build_row(r) for r in rows]
        inserted = _do_upsert(gc, gold_conn, out)

        rows_read     += len(rows)
        rows_inserted += inserted
        last_id        = int(rows[-1]["id"])
        set_last_policy_id(gc, gold_conn, last_id)
        log.info(f"[WATERMARK] last_policy_id={last_id} "
                 f"read={rows_read} inserted={rows_inserted}")

    log.info(f"[WATERMARK] Done read={rows_read} inserted={rows_inserted}")
    return rows_read, rows_inserted


# ============================================================
# ENTRYPOINT
# ============================================================

def run_load(**context):
    conf       = (context.get("dag_run") and context["dag_run"].conf) or {}
    policy_ids = conf.get("policy_ids", [])

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        if policy_ids:
            log.info(f"Running in CDC-TARGETED mode for policy_ids={policy_ids}")
            load_targeted(bc, gc, gold_conn, list(map(int, policy_ids)))
        else:
            log.info("Running in WATERMARK mode (no policy_ids in conf)")
            load_watermark(bc, gc, gold_conn)
    except Exception as e:
        log.error(f"[ERROR] {str(e)}", exc_info=True)
        gold_conn.rollback()
        raise
    finally:
        bc.close()
        gc.close()
        bronze_conn.close()
        gold_conn.close()


# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────
with DAG(
    dag_id="gold_upsert_dim_policy",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "dim"],
) as dag:

    PythonOperator(
        task_id="upsert_dim_policy",
        python_callable=run_load,
    )