"""
DAG: gold_upsert_fact_payment
CDC incremental upsert for fact_payment.

CHANGES (this version):
  - tax_exempt_flag  ENUM('0'..'9') NULL ADDED.
      Mapped from tickets.paid_type for TICKET source; NULL for all other sources.
  - sales_tax_exemption DECIMAL(12,2) NULL ADDED — hardcoded 0 in this load.
  - sales_tax_collected  DECIMAL(12,2) NULL ADDED — hardcoded 0 in this load.
  - fetch_tickets_for_anet: t.paid_type added to SELECT.
  - build_row: paid_type=None parameter added; 3 new fields appended to return tuple.
  - UPSERT_SQL: 38 columns (was 35).
  - ensure_fact_payment: DDL guard covers the 3 new columns.
  - amount = NULL when no anet_transaction row (source_txn_id is None path in
    _claim_null_payment_row / load_targeted / load_watermark not affected — those
    paths only process real anet rows where total is always present).

  NEW (validation_refund columns):
  - validate_refund_amount  DECIMAL(10,2) NULL ADDED.
      validation_refunds.total matched by reference_key = ticket_number.
      TICKET source only; NULL for all other sources.
  - vr_anet_trans_id  INT NULL ADDED.
      validation_refunds.anet_transaction_id for the same match.
  - vr_refund_status  ENUM('PENDING','FAILED','REFUNDED') NULL ADDED.
      validation_refunds.transaction_status, normalised to the ENUM set.
  - UPSERT_SQL: 41 columns total (was 38).
  - build_row: 3 new optional kwargs (validate_refund_amount, vr_anet_trans_id,
      vr_refund_status); return tuple is 41 elements.
  - _vr_kwargs_for_ticket() ADDED — thin helper that fetches validation_refund
      data for a single ticket_number and returns a kwargs dict for build_row.
  - fetch_validation_refunds_by_ticket_numbers() ADDED — batch fetcher.
  - handle_validation_refund_update() ADDED — triggered when validation_refund_ids
      arrive in conf (CDC event).  Performs a targeted UPDATE on fact_payment rows
      for the base ticket (extension_overstay_flag=0) identified via
      canonical_session_key → ticket_number match.
  - run_load: accepts validation_refund_ids from conf; runs handler before (or
      instead of) the targeted/watermark path.
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from typing import Optional, Dict, Tuple, Set, List

from db_config import get_bronze_connection, get_gold_connection, GOLD_DB

PROCESSOR_NAME = "Authorize.Net"
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

OVERSTAY_FLAG = 1
EXTEND_FLAG   = 2

_COLUMN_CACHE: Dict[Tuple[str, str], Set[str]] = {}


# ============================================================
# HELPERS
# ============================================================

def safe_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def dk(dt_value) -> Optional[int]:
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        return int(dt_value.strftime("%Y%m%d"))
    return None


def tk(dt_value) -> Optional[int]:
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        return int(dt_value.strftime("%H%M%S"))
    return None


def _get(row, key, idx):
    if isinstance(row, dict):
        return row.get(key)
    return row[idx] if row and len(row) > idx else None


def normalize_payment_method_type(value, *, offline: bool = False) -> Optional[str]:
    if offline:
        return "CASH"
    s = str(value or "").strip().lower()
    if not s:
        return None
    if "apple" in s:
        return "Apple Pay"
    if "google" in s or "gpay" in s:
        return "Google Pay"
    if "cash" in s or "offline" in s:
        return "CASH"
    if any(k in s for k in ("card", "visa", "mastercard", "amex", "discover", "debit", "credit")):
        return "CARD"
    return str(value).strip()[:50] or None


def approved_from_status(status_category: str) -> int:
    s = str(status_category or "").lower()
    return 1 if any(k in s for k in ("approv", "captured", "success", "settled")) else 0


def col_exists(bc, table: str, col: str) -> bool:
    cache_key = ("bronze", table)
    if cache_key not in _COLUMN_CACHE:
        bc.execute("""
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_NAME = %s
        """, (table,))
        _COLUMN_CACHE[cache_key] = {
            (r["COLUMN_NAME"] if isinstance(r, dict) else r[0])
            for r in bc.fetchall()
        }
    return col in _COLUMN_CACHE[cache_key]


# ============================================================
# GOLD LOOKUPS
# ============================================================

def lookup_facility_key(gc, facility_id):
    if not facility_id:
        return None
    gc.execute(f"""
        SELECT facility_key FROM {GOLD_DB}.dim_facility
        WHERE facility_id = %s AND is_current = 1 LIMIT 1
    """, (facility_id,))
    row = gc.fetchone()
    return _get(row, "facility_key", 0) if row else None


def lookup_processor_key(gc, processor_name):
    gc.execute(f"""
        SELECT processor_key FROM {GOLD_DB}.dim_processor
        WHERE processor_name = %s OR provider = %s LIMIT 1
    """, (processor_name, processor_name))
    row = gc.fetchone()
    return _get(row, "processor_key", 0) if row else None


def lookup_payment_method_key(gc, processor_key, method_type):
    if processor_key is None or method_type is None:
        return None
    gc.execute(f"""
        SELECT payment_method_key FROM {GOLD_DB}.dim_payment_method
        WHERE payment_method_id = %s AND method_type = %s LIMIT 1
    """, (processor_key, method_type))
    row = gc.fetchone()
    return _get(row, "payment_method_key", 0) if row else None


def lookup_canonical_session_key(gc, ticket_id):
    if not ticket_id:
        return None
    gc.execute(f"""
        SELECT canonical_session_key FROM {GOLD_DB}.fact_parking_session
        WHERE extension_overstay_flag = 0 AND source_id = %s LIMIT 1
    """, (ticket_id,))
    row = gc.fetchone()
    return _get(row, "canonical_session_key", 0) if row else None


def lookup_canonical_session_key_by_flag(gc, source_id, flag):
    if not source_id:
        return None
    gc.execute(f"""
        SELECT canonical_session_key FROM {GOLD_DB}.fact_parking_session
        WHERE extension_overstay_flag = %s AND source_id = %s LIMIT 1
    """, (flag, int(source_id)))
    row = gc.fetchone()
    return _get(row, "canonical_session_key", 0) if row else None


def lookup_canonical_session_by_reservation(bc, gc, reservation_id):
    if not reservation_id:
        return None
    try:
        bc.execute("""
            SELECT id FROM tickets
            WHERE reservation_id = %s AND deleted_at IS NULL
            ORDER BY id DESC LIMIT 10
        """, (reservation_id,))
        rows = bc.fetchall()
        if not rows:
            return None
        ticket_ids = [int(r["id"] if isinstance(r, dict) else r[0]) for r in rows]
        ph = ",".join(["%s"] * len(ticket_ids))
        gc.execute(f"""
            SELECT canonical_session_key FROM {GOLD_DB}.fact_parking_session
            WHERE extension_overstay_flag = 0 AND source_id IN ({ph})
            ORDER BY canonical_session_key DESC LIMIT 1
        """, tuple(ticket_ids))
        row = gc.fetchone()
        return _get(row, "canonical_session_key", 0) if row else None
    except Exception as e:
        log.warning(f"lookup_canonical_session_by_reservation failed: {e}")
        return None


def lookup_reservation_key(gc, reservation_id):
    if not reservation_id:
        return None
    gc.execute(f"""
        SELECT reservation_key FROM {GOLD_DB}.fact_reservation
        WHERE source_reservation_id = %s LIMIT 1
    """, (reservation_id,))
    row = gc.fetchone()
    return _get(row, "reservation_key", 0) if row else None


def lookup_permit_subscription_key(gc, permit_request_id):
    if not permit_request_id:
        return None
    gc.execute(f"""
        SELECT permit_subscription_key FROM {GOLD_DB}.fact_permit_subscription
        WHERE source_permit_id = %s LIMIT 1
    """, (permit_request_id,))
    row = gc.fetchone()
    return _get(row, "permit_subscription_key", 0) if row else None


def lookup_pass_subscription_key(gc, user_pass_id):
    if not user_pass_id:
        return None
    gc.execute(f"""
        SELECT pass_subscription_key FROM {GOLD_DB}.fact_passes
        WHERE source_user_pass_id = %s LIMIT 1
    """, (user_pass_id,))
    row = gc.fetchone()
    return _get(row, "pass_subscription_key", 0) if row else None


def lookup_event_key_by_reservation(gc, reservation_id):
    if not reservation_id:
        return None
    gc.execute(f"""
        SELECT event_key FROM {GOLD_DB}.fact_reservation
        WHERE source_reservation_id = %s AND event_key IS NOT NULL LIMIT 1
    """, (reservation_id,))
    row = gc.fetchone()
    return _get(row, "event_key", 0) if row else None


# ============================================================
# CLAIM NULL PAYMENT ROW
# ============================================================

def _delete_orphan_null_row(gc, gold_conn, anet_id: int, fk_pairs: list) -> None:
    for col, val in fk_pairs:
        if val is None:
            continue
        gc.execute(f"""
            DELETE FROM {GOLD_DB}.fact_payment
            WHERE {col} = %s
              AND source_transaction_id IS NULL
            ORDER BY payment_key DESC
            LIMIT 1
        """, (val,))
        if gc.rowcount > 0:
            gold_conn.commit()
            log.info(f"[CLAIM] Deleted orphaned NULL row matched via {col}={val} "
                     f"(keyed row source_transaction_id={anet_id} already exists)")
            break


def _claim_null_payment_row(gc, gold_conn, anet_id: int,
                             canonical_session_key=None,
                             reservation_key=None,
                             permit_subscription_key=None,
                             pass_subscription_key=None) -> bool:
    from mysql.connector import errors as _mysql_errors

    fk_pairs = [
        ("canonical_session_key",   canonical_session_key),
        ("reservation_key",         reservation_key),
        ("permit_subscription_key", permit_subscription_key),
        ("pass_subscription_key",   pass_subscription_key),
    ]

    gc.execute(f"""
        SELECT payment_key FROM {GOLD_DB}.fact_payment
        WHERE source_transaction_id = %s LIMIT 1
    """, (anet_id,))
    if gc.fetchone() is not None:
        _delete_orphan_null_row(gc, gold_conn, anet_id, fk_pairs)
        return False

    for col, val in fk_pairs:
        if val is None:
            continue
        try:
            gc.execute(f"""
                UPDATE {GOLD_DB}.fact_payment
                SET source_transaction_id = %s
                WHERE {col} = %s
                  AND source_transaction_id IS NULL
                ORDER BY payment_key DESC
                LIMIT 1
            """, (anet_id, val))
        except _mysql_errors.IntegrityError as exc:
            if exc.errno != 1062:
                raise
            gold_conn.rollback()
            log.warning(f"[CLAIM] IntegrityError on UPDATE (anet_id={anet_id}, "
                        f"{col}={val}); deleting orphan NULL row")
            _delete_orphan_null_row(gc, gold_conn, anet_id, fk_pairs)
            return False

        if gc.rowcount > 0:
            gold_conn.commit()
            log.info(f"[CLAIM] Promoted NULL payment row via {col}={val} "
                     f"→ source_transaction_id={anet_id}")
            return True

    return False


# ============================================================
# DDL GUARD
# CHANGED: 3 new columns added to CREATE TABLE and ALTER guard.
# ============================================================

def ensure_fact_payment(gc, gold_conn) -> None:
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.fact_payment (
            payment_key             BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_transaction_id   BIGINT NULL,
            payment_ts_utc          TIMESTAMP NULL,
            date_key                INT NULL,
            facility_key            BIGINT NULL,
            payment_time_key        INT NULL,
            payment_method_key      BIGINT NULL,
            processor_key           BIGINT NULL,
            canonical_session_key   BIGINT NULL,
            reservation_key         BIGINT NULL,
            event_key               BIGINT NULL,
            permit_subscription_key BIGINT NULL,
            pass_subscription_key   BIGINT NULL,
            transaction_type        VARCHAR(50) NULL,
            amount                  DECIMAL(12,2) NULL,
            approved_flag           BOOLEAN NULL,
            processor_txn_id        VARCHAR(255) NULL,
            reason_key              BIGINT NULL,
            sales_tax               DECIMAL(12,2) NULL,
            transaction_date        TIMESTAMP NULL,
            cc_refund_amount        DECIMAL(12,2) NULL,
            city_surcharge          DECIMAL(12,2) NULL,
            posted_gross_amount     DECIMAL(12,2) NULL,
            discount_amount         DECIMAL(12,2) NULL,
            base_parking_amount     DECIMAL(12,2) NULL,
            validate_amount         DECIMAL(12,2) NULL,
            void_amount             DECIMAL(10,2) NULL,
            release_parking_amount  DECIMAL(10,2) NULL,
            processing_fees         DECIMAL(8,2)  NULL,
            oversize_fees           DECIMAL(8,2)  NULL,
            is_offline_payment      TINYINT(1)    NOT NULL DEFAULT 0,
            card_type               VARCHAR(50)   NULL,
            net_parking_amount      DECIMAL(12,2) NULL,
            refund_date             TIMESTAMP     NULL,
            permit_prorate          DECIMAL(12,2) NULL,
            tax_exempt_flag         ENUM('0','1','2','3','4','5','6','7','8','9') NULL
                COMMENT 'Mapped from tickets.paid_type; NULL for non-ticket sources',
            sales_tax_exemption     DECIMAL(12,2) NULL DEFAULT 0
                COMMENT 'Tax exemption amount; hardcoded 0 in this load',
            sales_tax_collected     DECIMAL(12,2) NULL DEFAULT 0
                COMMENT 'Tax collected amount; hardcoded 0 in this load',
            validate_refund_amount  DECIMAL(10,2) NULL
                COMMENT 'validation_refunds.total for this ticket; NULL if no refund or non-ticket source',
            vr_anet_trans_id        INT NULL
                COMMENT 'validation_refunds.anet_transaction_id; NULL if no refund or non-ticket source',
            vr_refund_status        ENUM('PENDING','FAILED','REFUNDED') NULL
                COMMENT 'validation_refunds.transaction_status; NULL if no refund or non-ticket source',
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_transaction_id (source_transaction_id),
            INDEX idx_date_key        (date_key),
            INDEX idx_facility_key    (facility_key),
            INDEX idx_session_key     (canonical_session_key),
            INDEX idx_reservation_key (reservation_key),
            INDEX idx_pass_sub_key    (pass_subscription_key)
        )
    """)
    gold_conn.commit()

    for col_name, col_def in [
        ("event_key",              "BIGINT NULL AFTER reservation_key"),
        ("pass_subscription_key",  "BIGINT NULL AFTER permit_subscription_key"),
        ("is_offline_payment",     "TINYINT(1) NOT NULL DEFAULT 0 AFTER oversize_fees"),
        ("card_type",              "VARCHAR(50) NULL AFTER is_offline_payment"),
        ("net_parking_amount",     "DECIMAL(12,2) NULL AFTER card_type"),
        ("refund_date",            "TIMESTAMP NULL AFTER net_parking_amount"),
        ("permit_prorate",         "DECIMAL(12,2) NULL AFTER refund_date"),
        ("release_parking_amount", "DECIMAL(10,2) NULL AFTER void_amount"),
        # ── NEW columns ──────────────────────────────────────────────────────
        ("tax_exempt_flag",
         "ENUM('0','1','2','3','4','5','6','7','8','9') NULL "
         "COMMENT 'Mapped from tickets.paid_type; NULL for non-ticket sources' "
         "AFTER permit_prorate"),
        ("sales_tax_exemption",
         "DECIMAL(12,2) NULL DEFAULT 0 "
         "COMMENT 'Tax exemption amount; hardcoded 0 in this load' "
         "AFTER tax_exempt_flag"),
        ("sales_tax_collected",
         "DECIMAL(12,2) NULL DEFAULT 0 "
         "COMMENT 'Tax collected amount; hardcoded 0 in this load' "
         "AFTER sales_tax_exemption"),
        # ── NEW columns ──────────────────────────────────────────────────────
        ("validate_refund_amount",
         "DECIMAL(10,2) NULL "
         "COMMENT 'validation_refunds.total for this ticket; NULL if no refund or non-ticket source' "
         "AFTER sales_tax_collected"),
        ("vr_anet_trans_id",
         "INT NULL "
         "COMMENT 'validation_refunds.anet_transaction_id; NULL if no refund or non-ticket source' "
         "AFTER validate_refund_amount"),
        ("vr_refund_status",
         "ENUM('PENDING','FAILED','REFUNDED') NULL "
         "COMMENT 'validation_refunds.transaction_status; NULL if no refund or non-ticket source' "
         "AFTER vr_anet_trans_id"),
    ]:
        gc.execute(f"""
            SELECT COUNT(*) AS cnt FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'fact_payment' AND column_name = %s
        """, (GOLD_DB, col_name))
        row = gc.fetchone()
        exists = (_get(row, "cnt", 0) or 0) > 0
        if not exists:
            gc.execute(f"ALTER TABLE {GOLD_DB}.fact_payment ADD COLUMN {col_name} {col_def}")
            gold_conn.commit()
            log.info(f"Added missing column fact_payment.{col_name}")


# ============================================================
# UPSERT SQL — 38 columns (was 35)
# CHANGED: tax_exempt_flag, sales_tax_exemption, sales_tax_collected added.
# ============================================================

UPSERT_SQL = f"""
    INSERT INTO {GOLD_DB}.fact_payment (
        source_transaction_id,
        payment_ts_utc, date_key, facility_key, payment_time_key,
        payment_method_key, processor_key,
        canonical_session_key, reservation_key, event_key,
        permit_subscription_key, pass_subscription_key,
        transaction_type, amount, approved_flag,
        processor_txn_id, reason_key,
        sales_tax, transaction_date, cc_refund_amount, city_surcharge,
        posted_gross_amount, discount_amount, base_parking_amount,
        validate_amount, void_amount, release_parking_amount,
        processing_fees, oversize_fees,
        is_offline_payment,
        card_type, net_parking_amount, refund_date, permit_prorate,
        tax_exempt_flag, sales_tax_exemption, sales_tax_collected,
        validate_refund_amount, vr_anet_trans_id, vr_refund_status,
        created_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        payment_ts_utc          = IF(VALUES(amount)>0, VALUES(payment_ts_utc),   payment_ts_utc),
        date_key                = IF(VALUES(amount)>0, VALUES(date_key),          date_key),
        facility_key            = COALESCE(VALUES(facility_key),                  facility_key),
        payment_time_key        = IF(VALUES(amount)>0, VALUES(payment_time_key),  payment_time_key),
        payment_method_key      = COALESCE(VALUES(payment_method_key),            payment_method_key),
        processor_key           = COALESCE(VALUES(processor_key),                 processor_key),
        canonical_session_key   = COALESCE(VALUES(canonical_session_key),         canonical_session_key),
        reservation_key         = COALESCE(VALUES(reservation_key),               reservation_key),
        event_key               = COALESCE(VALUES(event_key),                     event_key),
        permit_subscription_key = COALESCE(VALUES(permit_subscription_key),       permit_subscription_key),
        pass_subscription_key   = COALESCE(VALUES(pass_subscription_key),         pass_subscription_key),
        transaction_type        = IF(VALUES(amount)>0, VALUES(transaction_type),  transaction_type),
        amount                  = VALUES(amount),
        approved_flag           = VALUES(approved_flag),
        processor_txn_id        = COALESCE(VALUES(processor_txn_id),              processor_txn_id),
        reason_key              = COALESCE(VALUES(reason_key),                    reason_key),
        sales_tax               = VALUES(sales_tax),
        transaction_date        = IF(VALUES(amount)>0, VALUES(transaction_date),  transaction_date),
        cc_refund_amount        = VALUES(cc_refund_amount),
        city_surcharge          = VALUES(city_surcharge),
        posted_gross_amount     = VALUES(posted_gross_amount),
        discount_amount         = VALUES(discount_amount),
        base_parking_amount     = VALUES(base_parking_amount),
        validate_amount         = VALUES(validate_amount),
        void_amount             = VALUES(void_amount),
        release_parking_amount  = COALESCE(VALUES(release_parking_amount), release_parking_amount),
        processing_fees         = VALUES(processing_fees),
        oversize_fees           = VALUES(oversize_fees),
        is_offline_payment      = VALUES(is_offline_payment),
        card_type               = COALESCE(VALUES(card_type),              card_type),
        net_parking_amount      = VALUES(net_parking_amount),
        refund_date             = COALESCE(VALUES(refund_date),            refund_date),
        permit_prorate          = COALESCE(VALUES(permit_prorate),         permit_prorate),
        tax_exempt_flag         = COALESCE(VALUES(tax_exempt_flag),        tax_exempt_flag),
        sales_tax_exemption     = VALUES(sales_tax_exemption),
        sales_tax_collected     = VALUES(sales_tax_collected),
        validate_refund_amount  = COALESCE(VALUES(validate_refund_amount), validate_refund_amount),
        vr_anet_trans_id        = COALESCE(VALUES(vr_anet_trans_id),       vr_anet_trans_id),
        vr_refund_status        = COALESCE(VALUES(vr_refund_status),       vr_refund_status)
"""


def do_upsert(gc, gold_conn, rows, max_retries: int = 3):
    if not rows:
        return
    import time
    from mysql.connector import errors as _mysql_errors
    for attempt in range(1, max_retries + 1):
        try:
            gc.executemany(UPSERT_SQL, rows)
            gold_conn.commit()
            return
        except _mysql_errors.InternalError as e:
            if e.errno == 1213 and attempt < max_retries:
                gold_conn.rollback()
                wait = attempt * 2
                log.warning(f"[do_upsert] Deadlock on attempt {attempt}, retrying in {wait}s...")
                import time as _t; _t.sleep(wait)
            else:
                gold_conn.rollback()
                raise


# ============================================================
# BRONZE FETCH HELPERS
# CHANGED: fetch_tickets_for_anet adds t.paid_type to SELECT.
# ============================================================

def fetch_anet_rows(bc, anet_ids: List[int]) -> List[dict]:
    if not anet_ids:
        return []
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT at.id, at.total, at.anet_trans_id, at.response_code, at.method,
               COALESCE(ast.category, '') AS status_category,
               at.anet_type_id, at.created_at,
               at.card_type
        FROM anet_transactions at
        LEFT JOIN anet_statuses ast ON ast.id = at.anet_status_id
        WHERE at.id IN ({ph}) ORDER BY at.id ASC
    """, tuple(anet_ids))
    rows = bc.fetchall()
    if rows and not isinstance(rows[0], dict):
        keys = ["id", "total", "anet_trans_id", "response_code", "method",
                "status_category", "anet_type_id", "created_at", "card_type"]
        rows = [dict(zip(keys, r)) for r in rows]
    return rows


def fetch_tickets_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_net = col_exists(bc, "tickets", "net_parking_amount")
    has_rfd = col_exists(bc, "tickets", "refund_date")
    has_rel = col_exists(bc, "tickets", "release_parking_amount")
    net_expr = "t.net_parking_amount"     if has_net else "NULL"
    rfd_expr = "t.refund_date"            if has_rfd else "NULL"
    rel_expr = "t.release_parking_amount" if has_rel else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    # CHANGED: t.paid_type added to SELECT for tax_exempt_flag mapping
    bc.execute(f"""
        SELECT t.id, t.facility_id, t.anet_transaction_id, t.is_offline_payment,
               t.created_at, t.grand_total, t.parking_amount, t.processing_fee,
               t.tax_fee, t.paid_amount, t.surcharge_fee, t.refund_amount,
               t.discount_amount, t.ticket_number, t.payment_gateway,
               t.reservation_id, t.permit_request_id, t.user_pass_id,
               t.is_extended, t.is_overstay, t.deleted_at,
               {net_expr} AS net_parking_amount,
               {rfd_expr} AS refund_date,
               {rel_expr} AS release_parking_amount,
               t.paid_type
        FROM tickets t
        WHERE t.anet_transaction_id IN ({ph}) AND t.deleted_at IS NULL
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "is_offline_payment",
            "created_at", "grand_total", "parking_amount", "processing_fee",
            "tax_fee", "paid_amount", "surcharge_fee", "refund_amount",
            "discount_amount", "ticket_number", "payment_gateway",
            "reservation_id", "permit_request_id", "user_pass_id",
            "is_extended", "is_overstay", "deleted_at",
            "net_parking_amount", "refund_date", "release_parking_amount",
            "paid_type"]       # CHANGED
    result = {}
    for r in raw:
        t = r if isinstance(r, dict) else dict(zip(keys, r))
        result.setdefault(int(t["anet_transaction_id"]), []).append(t)
    return result


def fetch_reservations_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_net = col_exists(bc, "reservations", "net_parking_amount")
    has_rfd = col_exists(bc, "reservations", "refund_date")
    net_expr = "r.net_parking_amount" if has_net else "NULL"
    rfd_expr = "r.refund_date"        if has_rfd else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT r.id, r.facility_id, r.anet_transaction_id, r.created_at,
               r.total, r.parking_amount, r.reservation_amount, r.discount,
               r.tax_fee, r.processing_fee, r.refund_amount, r.event_id,
               r.user_id, r.partner_id,
               {net_expr} AS net_parking_amount,
               {rfd_expr} AS refund_date
        FROM reservations r
        WHERE r.anet_transaction_id IN ({ph}) AND r.deleted_at IS NULL
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "created_at",
            "total", "parking_amount", "reservation_amount", "discount",
            "tax_fee", "processing_fee", "refund_amount", "event_id",
            "user_id", "partner_id", "net_parking_amount", "refund_date"]
    result = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        result[int(rec["anet_transaction_id"])] = rec
    return result


def fetch_permits_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_net   = col_exists(bc, "permit_requests", "net_parking_amount")
    has_prate = col_exists(bc, "permit_requests", "permit_prorate")
    has_rfd   = col_exists(bc, "permit_requests", "refund_date")
    net_expr   = "pr.net_parking_amount" if has_net   else "NULL"
    prate_expr = "pr.permit_prorate"     if has_prate else "NULL"
    rfd_expr   = "pr.refund_date"        if has_rfd   else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT pr.id, pr.facility_id, pr.anet_transaction_id, pr.created_at,
               pr.permit_final_amount, pr.permit_rate,
               {net_expr}   AS net_parking_amount,
               pr.discount_amount, pr.tax_fee, pr.processing_fee,
               pr.additional_fee, pr.surcharge_fee, pr.refund_amount,
               {prate_expr} AS permit_prorate,
               {rfd_expr}   AS refund_date
        FROM permit_requests pr
        WHERE pr.anet_transaction_id IN ({ph}) AND pr.deleted_at IS NULL
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "created_at",
            "permit_final_amount", "permit_rate", "net_parking_amount",
            "discount_amount", "tax_fee", "processing_fee", "additional_fee",
            "surcharge_fee", "refund_amount", "permit_prorate", "refund_date"]
    result = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        result[int(rec["anet_transaction_id"])] = rec
    return result


def fetch_passes_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_net = col_exists(bc, "user_passes", "net_parking_amount")
    has_rfd = col_exists(bc, "user_passes", "refund_date")
    net_expr = "up.net_parking_amount" if has_net else "NULL"
    rfd_expr = "up.refund_date"        if has_rfd else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT up.id, up.facility_id, up.anet_transaction_id, up.created_at,
               up.total, up.parking_amount, up.discount_amount,
               up.tax_fee, up.processing_fee, up.refund_amount,
               {net_expr} AS net_parking_amount,
               {rfd_expr} AS refund_date
        FROM user_passes up
        WHERE up.anet_transaction_id IN ({ph}) AND up.deleted_at IS NULL
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "created_at",
            "total", "parking_amount", "discount_amount", "tax_fee",
            "processing_fee", "refund_amount", "net_parking_amount", "refund_date"]
    result = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        result[int(rec["anet_transaction_id"])] = rec
    return result


def fetch_overstays_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_add         = col_exists(bc, "overstay_tickets", "additional_fee")
    has_sur         = col_exists(bc, "overstay_tickets", "surcharge_fee")
    has_net_parking = col_exists(bc, "overstay_tickets", "net_parking_amount")
    has_refund_date = col_exists(bc, "overstay_tickets", "refund_date")
    add_expr         = "additional_fee"     if has_add         else "0"
    sur_expr         = "surcharge_fee"      if has_sur         else "0"
    net_parking_expr = "net_parking_amount" if has_net_parking else "0"
    refund_date_expr = "refund_date"        if has_refund_date else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT id, facility_id, anet_transaction_id,
               is_offline_payment, payment_date, created_at,
               grand_total, parking_amount, discount_amount,
               tax_fee, processing_fee,
               {add_expr}         AS additional_fee,
               {sur_expr}         AS surcharge_fee,
               penalty_fee, reservation_id,
               {net_parking_expr} AS net_parking_amount,
               {refund_date_expr} AS refund_date
        FROM overstay_tickets
        WHERE anet_transaction_id IN ({ph})
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "is_offline_payment",
            "payment_date", "created_at", "grand_total", "parking_amount",
            "discount_amount", "tax_fee", "processing_fee", "additional_fee",
            "surcharge_fee", "penalty_fee", "reservation_id",
            "net_parking_amount", "refund_date"]
    result = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        result[int(rec["anet_transaction_id"])] = rec
    return result


def fetch_extends_for_anet(bc, anet_ids: List[int]) -> dict:
    if not anet_ids:
        return {}
    has_net = col_exists(bc, "ticket_extends", "net_parking_amount")
    has_rfd = col_exists(bc, "ticket_extends", "refund_date")
    net_expr = "te.net_parking_amount" if has_net else "NULL"
    rfd_expr = "te.refund_date"        if has_rfd else "NULL"
    ph = ",".join(["%s"] * len(anet_ids))
    bc.execute(f"""
        SELECT te.id, te.facility_id, te.anet_transaction_id, te.created_at,
               te.grand_total, te.parking_amounts, te.discount_amount,
               te.tax_fee, te.processing_fee, te.additional_fee, te.surcharge_fee,
               te.oversize_fee, te.refund_amount,
               {net_expr} AS net_parking_amount,
               {rfd_expr} AS refund_date,
               te.ticket_id
        FROM ticket_extends te
        WHERE te.anet_transaction_id IN ({ph}) AND te.deleted_at IS NULL
    """, tuple(anet_ids))
    raw = bc.fetchall()
    keys = ["id", "facility_id", "anet_transaction_id", "created_at",
            "grand_total", "parking_amounts", "discount_amount",
            "tax_fee", "processing_fee", "additional_fee", "surcharge_fee",
            "oversize_fee", "refund_amount", "net_parking_amount",
            "refund_date", "ticket_id"]
    result = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        result[int(rec["anet_transaction_id"])] = rec
    return result


# ============================================================
# ROW BUILDER — 41-element tuple matching UPSERT_SQL
# CHANGED: paid_type=None parameter added (previous version).
#          validate_refund_amount, vr_anet_trans_id, vr_refund_status added (NEW).
# ============================================================

def _vr_kwargs_for_ticket(bc, ticket_number) -> dict:
    """
    Convenience wrapper used in targeted and watermark loops.
    Returns kwargs dict for build_row's three vr parameters.
    Uses the batch fetcher with a single-element list to keep
    the lookup logic in one place.
    """
    if not ticket_number:
        return {}
    vr_map = fetch_validation_refunds_by_ticket_numbers(bc, [ticket_number])
    data   = vr_map.get(ticket_number)
    if data is None:
        return {}
    vra, vr_anet, vr_status = data
    return {
        "validate_refund_amount": vra,
        "vr_anet_trans_id":       vr_anet,
        "vr_refund_status":       vr_status,
    }


def build_row(bc, gc, anet, src_id, src_type,
              fac_id, reservation_id, permit_id, pass_id,
              is_offline, grand_total, parking, discount,
              tax, proc_fee, surcharge, refund, paid,
              processor_key,
              oversize_fees=0.0,
              canonical_session_key_override=None,
              net_parking_amount=0.0,
              refund_date=None,
              permit_prorate=None,
              release_parking_amount=None,
              paid_type=None,          # CHANGED: new parameter
              # NEW: validation_refund fields — populated for TICKET source only
              validate_refund_amount=None,
              vr_anet_trans_id=None,
              vr_refund_status=None):

    anet_id     = int(anet["id"])
    anet_total  = float(anet["total"] or 0)
    stat_cat    = str(anet["status_category"] or "").strip().lower()
    method_raw  = anet["method"]
    created_at  = safe_dt(anet["created_at"]) or datetime.now()
    proc_txn_id = anet["anet_trans_id"]
    txn_type_id = anet["anet_type_id"]
    card_type   = anet.get("card_type") if isinstance(anet, dict) else None

    amount = anet_total - float(refund or 0)
    if stat_cat == "refund":
        amount = -abs(anet_total)

    approved    = approved_from_status(stat_cat)
    method_type = normalize_payment_method_type(method_raw, offline=bool(is_offline))
    pm_key      = lookup_payment_method_key(gc, processor_key, method_type) if processor_key and method_type else None

    if canonical_session_key_override is not None:
        canon_key = canonical_session_key_override
    elif src_type == "TICKET":
        canon_key = lookup_canonical_session_key(gc, src_id)
    else:
        canon_key = None

    resv_key  = lookup_reservation_key(gc, reservation_id) if reservation_id else None
    event_key = lookup_event_key_by_reservation(gc, reservation_id) if reservation_id else None
    perm_key  = lookup_permit_subscription_key(gc, permit_id) if permit_id else None
    pass_key  = lookup_pass_subscription_key(gc, pass_id) if pass_id else None
    fac_key   = lookup_facility_key(gc, fac_id)

    if canon_key is None and resv_key:
        canon_key = lookup_canonical_session_by_reservation(bc, gc, reservation_id)

    # CHANGED: tax_exempt_flag from tickets.paid_type; NULL for all other sources
    tax_exempt_flag = str(paid_type) if paid_type is not None else None

    # NEW: normalise vr_refund_status to the valid ENUM set
    _VR_VALID = {"PENDING", "FAILED", "REFUNDED"}
    if isinstance(vr_refund_status, str):
        vr_refund_status = vr_refund_status.strip().upper()
        if vr_refund_status not in _VR_VALID:
            vr_refund_status = None
    vra = float(validate_refund_amount) if validate_refund_amount is not None else None

    return (
        anet_id, created_at, dk(created_at), fac_key, tk(created_at),
        pm_key, processor_key,
        canon_key, resv_key, event_key,
        perm_key, pass_key,
        str(txn_type_id) if txn_type_id is not None else src_type,
        float(amount), approved,
        str(proc_txn_id) if proc_txn_id else None, None,
        float(tax or 0), created_at,
        float(refund or 0), float(surcharge or 0),
        float(grand_total or 0), float(discount or 0),
        float(parking or 0), float(paid or 0),
        0.0,
        float(release_parking_amount or 0) if release_parking_amount is not None else None,
        float(proc_fee or 0), float(oversize_fees or 0),
        int(is_offline or 0),
        card_type,
        float(net_parking_amount or 0),
        safe_dt(refund_date),
        float(permit_prorate or 0) if permit_prorate is not None else None,
        # Existing 3 from previous version
        tax_exempt_flag,   # tax_exempt_flag  — from tickets.paid_type for TICKET; else NULL
        0.0,               # sales_tax_exemption — hardcoded 0
        0.0,               # sales_tax_collected  — hardcoded 0
        # NEW: 3 validation_refund fields
        vra,               # validate_refund_amount
        vr_anet_trans_id,  # vr_anet_trans_id
        vr_refund_status,  # vr_refund_status
        datetime.now(),
    )


# ============================================================
# NEW: VALIDATION REFUND HELPERS
# ============================================================

_VR_VALID_STATUSES = {"PENDING", "FAILED", "REFUNDED"}


def fetch_validation_refunds_by_ticket_numbers(bc, ticket_numbers: list) -> dict:
    """
    Batch-fetches validation_refunds for a list of ticket_numbers.
    Returns dict: ticket_number → (total, anet_transaction_id, transaction_status)
    Latest row per ticket_number is kept (ORDER BY id DESC).
    """
    if not ticket_numbers:
        return {}
    ph = ",".join(["%s"] * len(ticket_numbers))
    bc.execute(f"""
        SELECT reference_key, total, anet_transaction_id, transaction_status
        FROM validation_refunds
        WHERE reference_key IN ({ph}) AND reference_key IS NOT NULL
        ORDER BY id DESC
    """, tuple(ticket_numbers))
    raw = bc.fetchall()
    keys = ["reference_key", "total", "anet_transaction_id", "transaction_status"]
    result: dict = {}
    for r in raw:
        rec = r if isinstance(r, dict) else dict(zip(keys, r))
        ref_key = rec["reference_key"]
        if ref_key in result:
            continue   # keep first (latest) due to ORDER BY id DESC
        vra    = float(rec["total"]) if rec["total"] is not None else None
        vr_id  = int(rec["anet_transaction_id"]) if rec["anet_transaction_id"] is not None else None
        st_raw = str(rec["transaction_status"]).strip().upper() if rec["transaction_status"] else None
        result[ref_key] = (vra, vr_id, st_raw if st_raw in _VR_VALID_STATUSES else None)
    return result


def handle_validation_refund_update(bc, gc, gold_conn, validation_refund_ids: list) -> int:
    """
    Called when validation_refund_ids arrive in CDC conf.
    For each refund, updates validate_refund_amount, vr_anet_trans_id,
    vr_refund_status in the fact_payment row corresponding to the
    BASE ticket (extension_overstay_flag = 0) whose ticket_number
    matches validation_refunds.reference_key.

    Non-ticket payment rows (reservations, permits, passes, extends,
    overstays) are intentionally left untouched.
    """
    if not validation_refund_ids:
        return 0
    ph = ",".join(["%s"] * len(validation_refund_ids))
    bc.execute(f"""
        SELECT id, reference_key, total, anet_transaction_id, transaction_status
        FROM validation_refunds
        WHERE id IN ({ph}) AND reference_key IS NOT NULL
    """, tuple(validation_refund_ids))
    raw = bc.fetchall()
    keys = ["id", "reference_key", "total", "anet_transaction_id", "transaction_status"]
    refunds = raw if (raw and isinstance(raw[0], dict)) else [
        dict(zip(keys, r)) for r in raw
    ]

    updated = 0
    for vr in refunds:
        ticket_number = vr["reference_key"]
        if not ticket_number:
            continue
        vra     = float(vr["total"]) if vr["total"] is not None else None
        vr_anet = int(vr["anet_transaction_id"]) if vr["anet_transaction_id"] is not None else None
        st_raw  = str(vr["transaction_status"]).strip().upper() if vr["transaction_status"] else None
        vr_stat = st_raw if st_raw in _VR_VALID_STATUSES else None

        # Resolve canonical_session_key for the BASE ticket row
        gc.execute(f"""
            SELECT canonical_session_key
            FROM {GOLD_DB}.fact_parking_session
            WHERE ticket_number = %s AND extension_overstay_flag = 0
            LIMIT 1
        """, (ticket_number,))
        sess_row = gc.fetchone()
        if not sess_row:
            log.warning(f"[VR:PAYMENT] No base session found for ticket_number={ticket_number}")
            continue
        canon_key = _get(sess_row, "canonical_session_key", 0)
        if not canon_key:
            continue

        gc.execute(f"""
            UPDATE {GOLD_DB}.fact_payment
            SET validate_refund_amount = %s,
                vr_anet_trans_id       = %s,
                vr_refund_status       = %s
            WHERE canonical_session_key = %s
              AND source_transaction_id IS NOT NULL
        """, (vra, vr_anet, vr_stat, canon_key))
        updated += gc.rowcount

    gold_conn.commit()
    log.info(f"[VR:PAYMENT] Updated {updated} fact_payment rows with vr columns")
    return updated


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


def get_last_anet_id(gc, gold_conn, job_name: str) -> int:
    ensure_watermark_table(gc, gold_conn)
    gc.execute(f"""
        SELECT last_anet_id FROM {GOLD_DB}.etl_watermarks
        WHERE job_name = %s LIMIT 1
    """, (job_name,))
    row = gc.fetchone()
    return int(_get(row, "last_anet_id", 0) or 0) if row else 0


def set_last_anet_id(gc, gold_conn, job_name: str, last_id: int) -> None:
    gc.execute(f"""
        INSERT INTO {GOLD_DB}.etl_watermarks (job_name, last_anet_id)
        VALUES (%s, %s) ON DUPLICATE KEY UPDATE last_anet_id = VALUES(last_anet_id)
    """, (job_name, int(last_id)))
    gold_conn.commit()


# ============================================================
# TARGETED MODE (CDC)
# CHANGED: paid_type passed to build_row for TICKET source only.
# ============================================================

def load_targeted(bc, gc, gold_conn, transaction_ids: List[int]) -> Tuple[int, int]:
    ensure_fact_payment(gc, gold_conn)
    if not transaction_ids:
        return 0, 0

    log.info(f"[TARGETED] {len(transaction_ids)} anet IDs")

    try:
        processor_key = lookup_processor_key(gc, PROCESSOR_NAME)
    except Exception:
        processor_key = None

    anet_rows         = fetch_anet_rows(bc, transaction_ids)
    anet_ids          = [int(a["id"]) for a in anet_rows]
    tickets_by_anet   = fetch_tickets_for_anet(bc, anet_ids)
    reservs_by_anet   = fetch_reservations_for_anet(bc, anet_ids)
    permits_by_anet   = fetch_permits_for_anet(bc, anet_ids)
    passes_by_anet    = fetch_passes_for_anet(bc, anet_ids)
    overstays_by_anet = fetch_overstays_for_anet(bc, anet_ids)
    extends_by_anet   = fetch_extends_for_anet(bc, anet_ids)

    rows_out  = []
    processed = 0

    for anet in anet_rows:
        anet_id        = int(anet["id"])
        ticket_written = bool(tickets_by_anet.get(anet_id))

        for t in tickets_by_anet.get(anet_id, []):
            canon_key = lookup_canonical_session_key(gc, int(t["id"]))
            resv_key  = lookup_reservation_key(gc, t.get("reservation_id")) if t.get("reservation_id") else None
            perm_key  = lookup_permit_subscription_key(gc, t.get("permit_request_id")) if t.get("permit_request_id") else None
            pass_key  = lookup_pass_subscription_key(gc, t.get("user_pass_id")) if t.get("user_pass_id") else None
            _claim_null_payment_row(gc, gold_conn, anet_id,
                                    canonical_session_key=canon_key,
                                    reservation_key=resv_key,
                                    permit_subscription_key=perm_key,
                                    pass_subscription_key=pass_key)

        r = reservs_by_anet.get(anet_id)
        if r and not ticket_written:
            canon_key = lookup_canonical_session_by_reservation(bc, gc, int(r["id"]))
            resv_key  = lookup_reservation_key(gc, int(r["id"]))
            _claim_null_payment_row(gc, gold_conn, anet_id,
                                    canonical_session_key=canon_key, reservation_key=resv_key)

        p = permits_by_anet.get(anet_id)
        if p and not ticket_written:
            perm_key = lookup_permit_subscription_key(gc, int(p["id"]))
            _claim_null_payment_row(gc, gold_conn, anet_id, permit_subscription_key=perm_key)

        up = passes_by_anet.get(anet_id)
        if up and not ticket_written:
            pass_key = lookup_pass_subscription_key(gc, int(up["id"]))
            _claim_null_payment_row(gc, gold_conn, anet_id, pass_subscription_key=pass_key)

        ov = overstays_by_anet.get(anet_id)
        if ov and not ticket_written:
            canon_key = lookup_canonical_session_key_by_flag(gc, int(ov["id"]), OVERSTAY_FLAG)
            resv_key  = lookup_reservation_key(gc, ov.get("reservation_id")) if ov.get("reservation_id") else None
            _claim_null_payment_row(gc, gold_conn, anet_id,
                                    canonical_session_key=canon_key, reservation_key=resv_key)

        ex = extends_by_anet.get(anet_id)
        if ex and not ticket_written:
            canon_key = lookup_canonical_session_key_by_flag(gc, int(ex["id"]), EXTEND_FLAG)
            _claim_null_payment_row(gc, gold_conn, anet_id, canonical_session_key=canon_key)

        # CHANGED: paid_type passed for TICKET source
        for t in tickets_by_anet.get(anet_id, []):
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(t["id"]), src_type="TICKET",
                fac_id=t["facility_id"], reservation_id=t["reservation_id"],
                permit_id=t["permit_request_id"], pass_id=t["user_pass_id"],
                is_offline=t["is_offline_payment"], grand_total=t["grand_total"],
                parking=t["parking_amount"], discount=t["discount_amount"],
                tax=t["tax_fee"], proc_fee=float(t["processing_fee"] or 0),
                surcharge=t["surcharge_fee"], refund=t["refund_amount"],
                paid=t["paid_amount"], processor_key=processor_key,
                net_parking_amount=float(t.get("net_parking_amount") or 0),
                refund_date=t.get("refund_date"),
                release_parking_amount=float(t.get("release_parking_amount") or 0),
                paid_type=t.get("paid_type"),    # CHANGED
                **_vr_kwargs_for_ticket(bc, t.get("ticket_number")),  # NEW
            ))
            processed += 1

        r = reservs_by_anet.get(anet_id)
        if r and not ticket_written:
            base_parking = float(r.get("parking_amount") or r.get("reservation_amount") or 0)
            canon_key    = lookup_canonical_session_by_reservation(bc, gc, int(r["id"]))
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(r["id"]), src_type="RESERVATION",
                fac_id=r["facility_id"], reservation_id=int(r["id"]),
                permit_id=None, pass_id=None, is_offline=0,
                grand_total=r["total"], parking=base_parking, discount=r["discount"],
                tax=r["tax_fee"], proc_fee=float(r["processing_fee"] or 0),
                surcharge=0, refund=r["refund_amount"], paid=r["total"],
                processor_key=processor_key, canonical_session_key_override=canon_key,
                net_parking_amount=float(r.get("net_parking_amount") or 0),
                refund_date=r.get("refund_date"),
                # paid_type=None (default) — not applicable for reservations
            ))
            processed += 1

        p = permits_by_anet.get(anet_id)
        if p and not ticket_written:
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(p["id"]), src_type="PERMIT",
                fac_id=p["facility_id"], reservation_id=None,
                permit_id=int(p["id"]), pass_id=None, is_offline=0,
                grand_total=p["permit_final_amount"], parking=p["permit_rate"],
                discount=p["discount_amount"], tax=p["tax_fee"],
                proc_fee=float(p["processing_fee"] or 0)+float(p["additional_fee"] or 0),
                surcharge=p["surcharge_fee"], refund=p["refund_amount"],
                paid=p["permit_final_amount"], processor_key=processor_key,
                net_parking_amount=float(p.get("net_parking_amount") or 0),
                refund_date=p.get("refund_date"), permit_prorate=p.get("permit_prorate"),
                # paid_type=None (default) — not applicable for permits
            ))
            processed += 1

        up = passes_by_anet.get(anet_id)
        if up and not ticket_written:
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(up["id"]), src_type="PASS",
                fac_id=up["facility_id"], reservation_id=None,
                permit_id=None, pass_id=int(up["id"]), is_offline=0,
                grand_total=up["total"], parking=up["parking_amount"],
                discount=up["discount_amount"], tax=up["tax_fee"],
                proc_fee=float(up["processing_fee"] or 0),
                surcharge=0, refund=up["refund_amount"], paid=up["total"],
                processor_key=processor_key,
                net_parking_amount=float(up.get("net_parking_amount") or 0),
                refund_date=up.get("refund_date"),
                # paid_type=None (default) — not applicable for passes
            ))
            processed += 1

        ov = overstays_by_anet.get(anet_id)
        if ov and not ticket_written:
            resv_id   = ov.get("reservation_id")
            canon_key = lookup_canonical_session_key_by_flag(gc, int(ov["id"]), OVERSTAY_FLAG)
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(ov["id"]), src_type="OVERSTAY",
                fac_id=ov["facility_id"], reservation_id=resv_id,
                permit_id=None, pass_id=None,
                is_offline=int(ov.get("is_offline_payment") or 0),
                grand_total=ov["grand_total"], parking=ov["parking_amount"],
                discount=ov["discount_amount"], tax=ov["tax_fee"],
                proc_fee=float(ov.get("processing_fee") or 0)+float(ov.get("additional_fee") or 0),
                surcharge=ov.get("surcharge_fee", 0), refund=0, paid=ov["grand_total"],
                processor_key=processor_key,
                oversize_fees=float(ov.get("penalty_fee") or 0),
                canonical_session_key_override=canon_key,
                net_parking_amount=float(ov.get("net_parking_amount") or 0),
                refund_date=ov.get("refund_date"),
                # paid_type=None (default) — not applicable for overstays
            ))
            processed += 1

        ex = extends_by_anet.get(anet_id)
        if ex and not ticket_written:
            t_id = ex.get("ticket_id")
            parent_rsv_id = parent_permit_id = parent_pass_id = None
            if t_id:
                bc.execute("""
                    SELECT reservation_id, permit_request_id, user_pass_id
                    FROM tickets WHERE id = %s LIMIT 1
                """, (t_id,))
                pr = bc.fetchone()
                if pr:
                    parent_rsv_id    = _get(pr, "reservation_id", 0)
                    parent_permit_id = _get(pr, "permit_request_id", 1)
                    parent_pass_id   = _get(pr, "user_pass_id", 2)
            canon_key = lookup_canonical_session_key_by_flag(gc, int(ex["id"]), EXTEND_FLAG)
            rows_out.append(build_row(
                bc, gc, anet, src_id=int(ex["id"]), src_type="EXTEND",
                fac_id=ex["facility_id"], reservation_id=parent_rsv_id,
                permit_id=parent_permit_id, pass_id=parent_pass_id, is_offline=0,
                grand_total=ex["grand_total"], parking=ex.get("parking_amounts", 0),
                discount=ex.get("discount_amount", 0), tax=ex["tax_fee"],
                proc_fee=float(ex.get("processing_fee") or 0)+float(ex.get("additional_fee") or 0),
                surcharge=ex.get("surcharge_fee", 0), refund=ex.get("refund_amount", 0),
                paid=ex["grand_total"], processor_key=processor_key,
                oversize_fees=float(ex.get("oversize_fee") or 0),
                canonical_session_key_override=canon_key,
                net_parking_amount=float(ex.get("net_parking_amount") or 0),
                refund_date=ex.get("refund_date"),
                # paid_type=None (default) — not applicable for extends
            ))
            processed += 1

    do_upsert(gc, gold_conn, rows_out)
    log.info(f"[TARGETED] Done processed={processed}")
    return len(anet_rows), processed


# ============================================================
# WATERMARK MODE
# CHANGED: paid_type passed to build_row for TICKET source.
# ============================================================

def load_watermark(bc, gc, gold_conn, batch_size: int = 1000) -> Tuple[int, int]:
    ensure_fact_payment(gc, gold_conn)
    job_name     = "gold_upsert_fact_payment"
    last_anet_id = get_last_anet_id(gc, gold_conn, job_name)

    rows_read = rows_inserted = 0

    try:
        processor_key = lookup_processor_key(gc, PROCESSOR_NAME)
    except Exception:
        processor_key = None

    while True:
        bc.execute("""
            SELECT at.id, at.total, at.anet_trans_id, at.response_code, at.method,
                   COALESCE(ast.category, '') AS status_category,
                   at.anet_type_id, at.created_at, at.card_type
            FROM anet_transactions at
            LEFT JOIN anet_statuses ast ON ast.id = at.anet_status_id
            WHERE at.id > %s ORDER BY at.id ASC LIMIT %s
        """, (last_anet_id, batch_size))
        raw = bc.fetchall()
        if not raw:
            break

        anet_rows = raw if isinstance(raw[0], dict) else [
            dict(zip(["id", "total", "anet_trans_id", "response_code", "method",
                      "status_category", "anet_type_id", "created_at", "card_type"], r))
            for r in raw
        ]
        anet_ids          = [int(a["id"]) for a in anet_rows]
        tickets_by_anet   = fetch_tickets_for_anet(bc, anet_ids)
        reservs_by_anet   = fetch_reservations_for_anet(bc, anet_ids)
        permits_by_anet   = fetch_permits_for_anet(bc, anet_ids)
        passes_by_anet    = fetch_passes_for_anet(bc, anet_ids)
        overstays_by_anet = fetch_overstays_for_anet(bc, anet_ids)
        extends_by_anet   = fetch_extends_for_anet(bc, anet_ids)

        out = []
        for anet in anet_rows:
            anet_id        = int(anet["id"])
            ticket_written = bool(tickets_by_anet.get(anet_id))

            for t in tickets_by_anet.get(anet_id, []):
                canon_key = lookup_canonical_session_key(gc, int(t["id"]))
                resv_key  = lookup_reservation_key(gc, t.get("reservation_id")) if t.get("reservation_id") else None
                perm_key  = lookup_permit_subscription_key(gc, t.get("permit_request_id")) if t.get("permit_request_id") else None
                pass_key  = lookup_pass_subscription_key(gc, t.get("user_pass_id")) if t.get("user_pass_id") else None
                _claim_null_payment_row(gc, gold_conn, anet_id,
                                        canonical_session_key=canon_key,
                                        reservation_key=resv_key,
                                        permit_subscription_key=perm_key,
                                        pass_subscription_key=pass_key)

            r = reservs_by_anet.get(anet_id)
            if r and not ticket_written:
                canon_key = lookup_canonical_session_by_reservation(bc, gc, int(r["id"]))
                resv_key  = lookup_reservation_key(gc, int(r["id"]))
                _claim_null_payment_row(gc, gold_conn, anet_id,
                                        canonical_session_key=canon_key, reservation_key=resv_key)

            p = permits_by_anet.get(anet_id)
            if p and not ticket_written:
                _claim_null_payment_row(gc, gold_conn, anet_id,
                                        permit_subscription_key=lookup_permit_subscription_key(gc, int(p["id"])))

            up = passes_by_anet.get(anet_id)
            if up and not ticket_written:
                _claim_null_payment_row(gc, gold_conn, anet_id,
                                        pass_subscription_key=lookup_pass_subscription_key(gc, int(up["id"])))

            ov = overstays_by_anet.get(anet_id)
            if ov and not ticket_written:
                canon_key = lookup_canonical_session_key_by_flag(gc, int(ov["id"]), OVERSTAY_FLAG)
                resv_key  = lookup_reservation_key(gc, ov.get("reservation_id")) if ov.get("reservation_id") else None
                _claim_null_payment_row(gc, gold_conn, anet_id,
                                        canonical_session_key=canon_key, reservation_key=resv_key)

            ex = extends_by_anet.get(anet_id)
            if ex and not ticket_written:
                canon_key = lookup_canonical_session_key_by_flag(gc, int(ex["id"]), EXTEND_FLAG)
                _claim_null_payment_row(gc, gold_conn, anet_id, canonical_session_key=canon_key)

            # CHANGED: paid_type passed for TICKET source
            for t in tickets_by_anet.get(anet_id, []):
                out.append(build_row(bc, gc, anet, int(t["id"]), "TICKET",
                    t["facility_id"], t["reservation_id"], t["permit_request_id"], t["user_pass_id"],
                    t["is_offline_payment"], t["grand_total"], t["parking_amount"],
                    t["discount_amount"], t["tax_fee"], float(t["processing_fee"] or 0),
                    t["surcharge_fee"], t["refund_amount"], t["paid_amount"], processor_key,
                    net_parking_amount=float(t.get("net_parking_amount") or 0),
                    refund_date=t.get("refund_date"),
                    release_parking_amount=float(t.get("release_parking_amount") or 0),
                    paid_type=t.get("paid_type"),    # CHANGED
                    **_vr_kwargs_for_ticket(bc, t.get("ticket_number")),  # NEW
                ))

            r = reservs_by_anet.get(anet_id)
            if r and not ticket_written:
                bp = float(r.get("parking_amount") or r.get("reservation_amount") or 0)
                ck = lookup_canonical_session_by_reservation(bc, gc, int(r["id"]))
                out.append(build_row(bc, gc, anet, int(r["id"]), "RESERVATION",
                    r["facility_id"], int(r["id"]), None, None, 0,
                    r["total"], bp, r["discount"], r["tax_fee"],
                    float(r["processing_fee"] or 0), 0, r["refund_amount"], r["total"],
                    processor_key, canonical_session_key_override=ck,
                    net_parking_amount=float(r.get("net_parking_amount") or 0),
                    refund_date=r.get("refund_date")))

            p = permits_by_anet.get(anet_id)
            if p and not ticket_written:
                out.append(build_row(bc, gc, anet, int(p["id"]), "PERMIT",
                    p["facility_id"], None, int(p["id"]), None, 0,
                    p["permit_final_amount"], p["permit_rate"], p["discount_amount"], p["tax_fee"],
                    float(p["processing_fee"] or 0)+float(p["additional_fee"] or 0),
                    p["surcharge_fee"], p["refund_amount"], p["permit_final_amount"], processor_key,
                    net_parking_amount=float(p.get("net_parking_amount") or 0),
                    refund_date=p.get("refund_date"), permit_prorate=p.get("permit_prorate")))

            up = passes_by_anet.get(anet_id)
            if up and not ticket_written:
                out.append(build_row(bc, gc, anet, int(up["id"]), "PASS",
                    up["facility_id"], None, None, int(up["id"]), 0,
                    up["total"], up["parking_amount"], up["discount_amount"], up["tax_fee"],
                    float(up["processing_fee"] or 0), 0, up["refund_amount"], up["total"],
                    processor_key,
                    net_parking_amount=float(up.get("net_parking_amount") or 0),
                    refund_date=up.get("refund_date")))

            ov = overstays_by_anet.get(anet_id)
            if ov and not ticket_written:
                resv_id   = ov.get("reservation_id")
                canon_key = lookup_canonical_session_key_by_flag(gc, int(ov["id"]), OVERSTAY_FLAG)
                out.append(build_row(bc, gc, anet, int(ov["id"]), "OVERSTAY",
                    ov["facility_id"], resv_id, None, None,
                    int(ov.get("is_offline_payment") or 0),
                    ov["grand_total"], ov["parking_amount"], ov["discount_amount"], ov["tax_fee"],
                    float(ov.get("processing_fee") or 0)+float(ov.get("additional_fee") or 0),
                    ov.get("surcharge_fee", 0), 0, ov["grand_total"], processor_key,
                    oversize_fees=float(ov.get("penalty_fee") or 0),
                    canonical_session_key_override=canon_key,
                    net_parking_amount=float(ov.get("net_parking_amount") or 0),
                    refund_date=ov.get("refund_date")))

            ex = extends_by_anet.get(anet_id)
            if ex and not ticket_written:
                t_id = ex.get("ticket_id")
                parent_rsv_id = parent_permit_id = parent_pass_id = None
                if t_id:
                    bc.execute("SELECT reservation_id, permit_request_id, user_pass_id FROM tickets WHERE id = %s LIMIT 1", (t_id,))
                    pr = bc.fetchone()
                    if pr:
                        parent_rsv_id    = _get(pr, "reservation_id", 0)
                        parent_permit_id = _get(pr, "permit_request_id", 1)
                        parent_pass_id   = _get(pr, "user_pass_id", 2)
                canon_key = lookup_canonical_session_key_by_flag(gc, int(ex["id"]), EXTEND_FLAG)
                out.append(build_row(bc, gc, anet, int(ex["id"]), "EXTEND",
                    ex["facility_id"], parent_rsv_id, parent_permit_id, parent_pass_id, 0,
                    ex["grand_total"], ex.get("parking_amounts", 0), ex.get("discount_amount", 0),
                    ex["tax_fee"],
                    float(ex.get("processing_fee") or 0)+float(ex.get("additional_fee") or 0),
                    ex.get("surcharge_fee", 0), ex.get("refund_amount", 0), ex["grand_total"],
                    processor_key, oversize_fees=float(ex.get("oversize_fee") or 0),
                    canonical_session_key_override=canon_key,
                    net_parking_amount=float(ex.get("net_parking_amount") or 0),
                    refund_date=ex.get("refund_date")))

        do_upsert(gc, gold_conn, out)
        rows_read     += len(anet_rows)
        rows_inserted += len(out)
        last_anet_id   = int(anet_rows[-1]["id"])
        set_last_anet_id(gc, gold_conn, job_name, last_anet_id)
        log.info("fact_payment [WATERMARK] last_anet_id=%s read=%s inserted=%s",
                 last_anet_id, rows_read, rows_inserted)

    return rows_read, rows_inserted


# ============================================================
# ENTRYPOINT
# ============================================================

def run_load(**context):
    conf            = (context.get("dag_run") and context["dag_run"].conf) or {}
    transaction_ids       = conf.get("transaction_ids", [])
    validation_refund_ids = conf.get("validation_refund_ids", [])

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        # NEW: handle validation_refund CDC events — update vr columns in fact_payment
        if validation_refund_ids:
            log.info(f"Running validation_refund update for ids={validation_refund_ids}")
            ensure_fact_payment(gc, gold_conn)
            handle_validation_refund_update(bc, gc, gold_conn, list(map(int, validation_refund_ids)))
            if not transaction_ids:
                return   # pure validation-refund trigger — nothing else to do

        if transaction_ids:
            log.info(f"Running in CDC-TARGETED mode for transaction_ids={transaction_ids}")
            load_targeted(bc, gc, gold_conn, list(map(int, transaction_ids)))
        elif not validation_refund_ids:
            log.info("Running in WATERMARK mode (no transaction_ids in conf)")
            load_watermark(bc, gc, gold_conn)
    finally:
        bc.close()
        gc.close()
        bronze_conn.close()
        gold_conn.close()


with DAG(
    dag_id="gold_upsert_fact_payment",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact", "payment"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_payment",
        python_callable=run_load,
    )