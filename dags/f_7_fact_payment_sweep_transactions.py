"""
DAG: gold_upsert_fact_payment_sweep_transactions
CDC incremental upsert for fact_payment_sweep_transactions.

Source table : inventory_modules.payment_sweep_transactions
Target table : pe_reporting_slave_v7.fact_payment_sweep_transactions

Column mapping:
  pst.id                     → source_sweep_id          (natural/unique key)
  pst.facility_id            → facility_key              (dim_facility)
  pst.transaction_at         → start_date_key            (dim_date)
  pst.funded_at              → end_date_key              (dim_date)
  pst.transaction_at         → start_time_key            (dim_time)
  pst.funded_at              → end_time_key              (dim_time)
  pst.partner_id             → partner_account_key       (dim_partner_account)
  pst.transaction_id
    → fact_payment.processor_txn_id
    → fact_payment.canonical_session_key → canonical_session_key
    → fact_payment.payment_method_key    → payment_method_key
    → fact_payment.payment_key           → payment_key
  pst.cc_base_share          → cc_base_fees
  pst.cc_variable_share      → cc_variable_fees
  pst.pe_base_share          → pe_base_transaction_fees
  pst.pe_variable_share      → pe_variable_transaction_fees
  pst.partner_base_share     → partner_base_fees
  pst.partner_variable_share → partner_variable_fees
  pst.account_number         → account_number
  pst.sweep_batch            → sweep_batch
  pst.service_type           → service_type

CDC modes:
  sweep_ids        (list[int]) — targeted: process specific pst.id rows
  <no conf>                    — watermark: process all rows with id > last seen
"""

import logging
from datetime import datetime, date as date_type, time as time_type
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ============================================================
# GENERIC HELPERS
# ============================================================

def safe_dt(value) -> Optional[datetime]:
    if value is None:
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


def safe_decimal(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(str(value).replace("%", "")), 2)
    except Exception:
        return None


def _g(row, key, idx):
    if isinstance(row, dict):
        return row.get(key)
    return row[idx] if row and len(row) > idx else None


def get_date_part(value) -> Optional[date_type]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date_type):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def get_time_part(value) -> Optional[time_type]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, time_type):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    return None


# ============================================================
# GOLD DIM / FACT LOOKUPS
# ============================================================

def lookup_facility_key(gc, facility_id):
    if not facility_id:
        return None
    gc.execute(f"""
        SELECT facility_key FROM {GOLD_DB}.dim_facility
        WHERE facility_id = %s AND is_current = 1 LIMIT 1
    """, (facility_id,))
    row = gc.fetchone()
    return _g(row, "facility_key", 0) if row else None


def lookup_partner_account_key(gc, partner_id):
    if not partner_id:
        return None
    gc.execute(f"""
        SELECT partner_account_key FROM {GOLD_DB}.dim_partner_account
        WHERE account_id_source = %s AND is_current = 1 LIMIT 1
    """, (partner_id,))
    row = gc.fetchone()
    return _g(row, "partner_account_key", 0) if row else None


def lookup_date_key(gc, value) -> Optional[int]:
    d = get_date_part(value)
    if d is None:
        return None
    gc.execute(f"""
        SELECT date_key FROM {GOLD_DB}.dim_date
        WHERE full_date = %s LIMIT 1
    """, (d,))
    row = gc.fetchone()
    return _g(row, "date_key", 0) if row else None


def lookup_time_key(gc, value) -> Optional[int]:
    t = get_time_part(value)
    if t is None:
        return None
    gc.execute(f"""
        SELECT time_key FROM {GOLD_DB}.dim_time
        WHERE full_time = %s LIMIT 1
    """, (t,))
    row = gc.fetchone()
    return _g(row, "time_key", 0) if row else None


def lookup_payment_keys(gc, transaction_id):
    """
    Resolves payment_key, canonical_session_key, payment_method_key
    from fact_payment using processor_txn_id = transaction_id.
    Returns (payment_key, canonical_session_key, payment_method_key)
    or (None, None, None) when not found.
    """
    if not transaction_id:
        return None, None, None
    gc.execute(f"""
        SELECT payment_key, canonical_session_key, payment_method_key
        FROM {GOLD_DB}.fact_payment
        WHERE processor_txn_id = %s
        LIMIT 1
    """, (str(transaction_id),))
    row = gc.fetchone()
    if not row:
        return None, None, None
    return (
        _g(row, "payment_key", 0),
        _g(row, "canonical_session_key", 1),
        _g(row, "payment_method_key", 2),
    )


# ============================================================
# DDL GUARD
# ============================================================

def ensure_fact_payment_sweep_transactions(gc, gold_conn) -> None:
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.fact_payment_sweep_transactions (
            payment_sweep_key            BIGINT        AUTO_INCREMENT PRIMARY KEY,
            source_sweep_id              BIGINT        NOT NULL
                COMMENT 'payment_sweep_transactions.id from source',
            facility_key                 BIGINT        NULL,
            start_date_key               INT           NULL
                COMMENT 'dim_date key for transaction_at date',
            end_date_key                 INT           NULL
                COMMENT 'dim_date key for funded_at date',
            start_time_key               INT           NULL
                COMMENT 'dim_time key for transaction_at time',
            end_time_key                 INT           NULL
                COMMENT 'dim_time key for funded_at time',
            partner_account_key          BIGINT        NULL,
            canonical_session_key        BIGINT        NULL,
            payment_method_key           BIGINT        NULL,
            payment_key                  BIGINT        NULL,
            cc_base_fees                 DECIMAL(12,2) NULL,
            cc_variable_fees             DECIMAL(12,2) NULL,
            pe_base_transaction_fees     DECIMAL(12,2) NULL,
            pe_variable_transaction_fees DECIMAL(12,2) NULL,
            partner_base_fees            DECIMAL(12,2) NULL,
            partner_variable_fees        DECIMAL(12,2) NULL,
            account_number               VARCHAR(50)   NULL,
            sweep_batch                  VARCHAR(50)   NULL,
            service_type                 VARCHAR(255)  NULL,
            created_at                   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
            updated_at                   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_sweep_id  (source_sweep_id),
            INDEX idx_fpst_facility        (facility_key),
            INDEX idx_fpst_partner         (partner_account_key),
            INDEX idx_fpst_session         (canonical_session_key),
            INDEX idx_fpst_payment         (payment_key),
            INDEX idx_fpst_start_date      (start_date_key),
            INDEX idx_fpst_sweep_batch     (sweep_batch)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Fact table for payment sweep transactions (CC settlement data)'
    """)
    gold_conn.commit()


# ============================================================
# UPSERT SQL
# ============================================================

UPSERT_SQL = f"""
    INSERT INTO {GOLD_DB}.fact_payment_sweep_transactions (
        source_sweep_id,
        facility_key,
        start_date_key, end_date_key,
        start_time_key, end_time_key,
        partner_account_key,
        canonical_session_key,
        payment_method_key,
        payment_key,
        cc_base_fees, cc_variable_fees,
        pe_base_transaction_fees, pe_variable_transaction_fees,
        partner_base_fees, partner_variable_fees,
        account_number, sweep_batch, service_type,
        created_at
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, NOW()
    )
    ON DUPLICATE KEY UPDATE
        facility_key                 = COALESCE(VALUES(facility_key),                facility_key),
        start_date_key               = COALESCE(VALUES(start_date_key),              start_date_key),
        end_date_key                 = COALESCE(VALUES(end_date_key),                end_date_key),
        start_time_key               = COALESCE(VALUES(start_time_key),              start_time_key),
        end_time_key                 = COALESCE(VALUES(end_time_key),                end_time_key),
        partner_account_key          = COALESCE(VALUES(partner_account_key),         partner_account_key),
        canonical_session_key        = COALESCE(VALUES(canonical_session_key),       canonical_session_key),
        payment_method_key           = COALESCE(VALUES(payment_method_key),          payment_method_key),
        payment_key                  = COALESCE(VALUES(payment_key),                 payment_key),
        cc_base_fees                 = VALUES(cc_base_fees),
        cc_variable_fees             = VALUES(cc_variable_fees),
        pe_base_transaction_fees     = VALUES(pe_base_transaction_fees),
        pe_variable_transaction_fees = VALUES(pe_variable_transaction_fees),
        partner_base_fees            = VALUES(partner_base_fees),
        partner_variable_fees        = VALUES(partner_variable_fees),
        account_number               = VALUES(account_number),
        sweep_batch                  = VALUES(sweep_batch),
        service_type                 = VALUES(service_type)
"""


# ============================================================
# WATERMARK TABLE
# ============================================================

def ensure_watermark_table(gc, gold_conn) -> None:
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.etl_watermarks (
            job_name      VARCHAR(100) PRIMARY KEY,
            last_sweep_id BIGINT NOT NULL DEFAULT 0,
            updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                         ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    gold_conn.commit()


def get_last_sweep_id(gc, gold_conn, job_name: str) -> int:
    ensure_watermark_table(gc, gold_conn)
    gc.execute(f"""
        SELECT last_sweep_id FROM {GOLD_DB}.etl_watermarks
        WHERE job_name = %s LIMIT 1
    """, (job_name,))
    row = gc.fetchone()
    return int(_g(row, "last_sweep_id", 0) or 0) if row else 0


def set_last_sweep_id(gc, gold_conn, job_name: str, last_id: int) -> None:
    gc.execute(f"""
        INSERT INTO {GOLD_DB}.etl_watermarks (job_name, last_sweep_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_sweep_id = VALUES(last_sweep_id)
    """, (job_name, int(last_id)))
    gold_conn.commit()


# ============================================================
# ROW BUILDER
# ============================================================

def build_row(gc, pst: dict) -> tuple:
    """
    Builds one upsert row from a payment_sweep_transactions record.
    All dim / fact key lookups are done here.
    """
    src_id         = pst["id"]
    facility_id    = pst.get("facility_id")
    partner_id     = pst.get("partner_id")
    transaction_id = pst.get("transaction_id")
    transaction_at = pst.get("transaction_at")
    funded_at      = pst.get("funded_at")

    txn_dt  = safe_dt(transaction_at)
    fund_dt = safe_dt(funded_at)

    facility_key        = lookup_facility_key(gc, facility_id)
    partner_account_key = lookup_partner_account_key(gc, partner_id)
    start_date_key      = lookup_date_key(gc, txn_dt)
    end_date_key        = lookup_date_key(gc, fund_dt)
    start_time_key      = lookup_time_key(gc, txn_dt)
    end_time_key        = lookup_time_key(gc, fund_dt)

    payment_key, canonical_session_key, payment_method_key = \
        lookup_payment_keys(gc, transaction_id)

    return (
        int(src_id),
        facility_key,
        start_date_key,   end_date_key,
        start_time_key,   end_time_key,
        partner_account_key,
        canonical_session_key,
        payment_method_key,
        payment_key,
        safe_decimal(pst.get("cc_base_share")),
        safe_decimal(pst.get("cc_variable_share")),
        safe_decimal(pst.get("pe_base_share")),
        safe_decimal(pst.get("pe_variable_share")),
        safe_decimal(pst.get("partner_base_share")),
        safe_decimal(pst.get("partner_variable_share")),
        str(pst["account_number"])[:50]  if pst.get("account_number") else None,
        str(pst["sweep_batch"])[:50]     if pst.get("sweep_batch")     else None,
        str(pst["service_type"])[:255]   if pst.get("service_type")    else None,
    )


# ============================================================
# FETCH HELPERS
# ============================================================

_PST_COLS = [
    "id", "facility_id", "partner_id", "transaction_id",
    "transaction_at", "funded_at",
    "cc_base_share", "cc_variable_share",
    "pe_base_share", "pe_variable_share",
    "partner_base_share", "partner_variable_share",
    "account_number", "sweep_batch", "service_type",
]


def _to_dict(row) -> dict:
    if isinstance(row, dict):
        return row
    return dict(zip(_PST_COLS, row))


def fetch_by_ids(bc, sweep_ids: list) -> list:
    """Fetch specific rows by primary key list."""
    if not sweep_ids:
        return []
    ph = ",".join(["%s"] * len(sweep_ids))
    bc.execute(f"""
        SELECT {', '.join(_PST_COLS)}
        FROM payment_sweep_transactions
        WHERE id IN ({ph})
        ORDER BY id ASC
    """, tuple(sweep_ids))
    return [_to_dict(r) for r in bc.fetchall()]


def fetch_after_watermark(bc, last_id: int, batch_size: int = 1000) -> list:
    """Fetch rows with id > last_id for watermark mode."""
    bc.execute(f"""
        SELECT {', '.join(_PST_COLS)}
        FROM payment_sweep_transactions
        WHERE id > %s
        ORDER BY id ASC
        LIMIT %s
    """, (last_id, batch_size))
    return [_to_dict(r) for r in bc.fetchall()]


# ============================================================
# DO UPSERT
# ============================================================

def do_upsert(gc, gold_conn, rows: list) -> int:
    if not rows:
        return 0
    import time as _t
    from mysql.connector import errors as _mysql_errors
    for attempt in range(1, 4):
        try:
            gc.executemany(UPSERT_SQL, rows)
            gold_conn.commit()
            return gc.rowcount or len(rows)
        except _mysql_errors.InternalError as e:
            if e.errno == 1213 and attempt < 3:   # deadlock
                gold_conn.rollback()
                _t.sleep(attempt * 2)
            else:
                gold_conn.rollback()
                raise
    return 0


# ============================================================
# TARGETED MODE — called when sweep_ids provided in conf
# ============================================================

def load_targeted(bc, gc, gold_conn, sweep_ids: list) -> tuple[int, int]:
    ensure_fact_payment_sweep_transactions(gc, gold_conn)
    if not sweep_ids:
        return 0, 0

    log.info(f"[TARGETED] Processing {len(sweep_ids)} sweep rows")
    pst_rows = fetch_by_ids(bc, sweep_ids)
    if not pst_rows:
        log.warning(f"[TARGETED] No rows found for sweep_ids={sweep_ids}")
        return 0, 0

    out = [build_row(gc, r) for r in pst_rows]
    inserted = do_upsert(gc, gold_conn, out)
    log.info(f"[TARGETED] Done rows_read={len(pst_rows)} inserted={inserted}")
    return len(pst_rows), inserted


# ============================================================
# WATERMARK MODE — called when no sweep_ids in conf
# ============================================================

def load_watermark(bc, gc, gold_conn, batch_size: int = 1000) -> tuple[int, int]:
    ensure_fact_payment_sweep_transactions(gc, gold_conn)
    job_name      = "gold_upsert_fact_payment_sweep_transactions"
    last_sweep_id = get_last_sweep_id(gc, gold_conn, job_name)

    rows_read = rows_inserted = 0
    log.info(f"[WATERMARK] Starting from id > {last_sweep_id}")

    while True:
        pst_rows = fetch_after_watermark(bc, last_sweep_id, batch_size)
        if not pst_rows:
            break

        out = [build_row(gc, r) for r in pst_rows]
        inserted = do_upsert(gc, gold_conn, out)
        rows_read     += len(pst_rows)
        rows_inserted += inserted
        last_sweep_id  = int(pst_rows[-1]["id"])

        set_last_sweep_id(gc, gold_conn, job_name, last_sweep_id)
        log.info("[WATERMARK] last_sweep_id=%s read=%s inserted=%s",
                 last_sweep_id, rows_read, rows_inserted)

    return rows_read, rows_inserted


# ============================================================
# ENTRYPOINT
# ============================================================

def run_load(**context):
    conf      = (context.get("dag_run") and context["dag_run"].conf) or {}
    sweep_ids = conf.get("sweep_ids", [])

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        if sweep_ids:
            log.info(f"Running in CDC-TARGETED mode for sweep_ids={sweep_ids}")
            load_targeted(bc, gc, gold_conn, list(map(int, sweep_ids)))
        else:
            log.info("Running in WATERMARK mode (no sweep_ids in conf)")
            load_watermark(bc, gc, gold_conn)
    finally:
        bc.close()
        gc.close()
        bronze_conn.close()
        gold_conn.close()


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="gold_upsert_fact_payment_sweep_transactions",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact", "sweep", "payment"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_payment_sweep_transactions",
        python_callable=run_load,
    )