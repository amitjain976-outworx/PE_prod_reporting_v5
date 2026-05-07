"""
DAG: gold_upsert_fact_reservation
CDC incremental upsert for fact_reservation.

FIX (previous): Two connections (bronze/gold), no cross-server JOINs.
FIX 2 (previous): ensure_fact_reservation column guards for source_reservation_id
  AND source_transaction_id; unique key guards.
FIX 3 (previous): source_reservation_id REMOVED from fact_payment.

CHANGES (this version — FIX):
  - REMOVED synthetic negative ID (-(int(rid))) from upsert_fact_payment_for_reservation.
      Previously: source_transaction_id = -(int(rid)) when anet is absent.
      Now: source_transaction_id = NULL when anet is absent, matching the
           initial load (enterprise_replica_etl.py) exactly.
  - NULL anet handling in upsert_fact_payment_for_reservation:
      When source_transaction_id IS NULL:
        1. Look for an existing fact_payment row WHERE reservation_key = <this key>
           AND source_transaction_id IS NULL.
        2. If found  → UPDATE that row (avoids duplicate NULL rows).
        3. If not found → INSERT with source_transaction_id = NULL.
           MySQL UNIQUE KEY allows multiple NULLs, so this is safe.
      When anet arrives later, gold_upsert_fact_payment DAG calls
      _claim_null_payment_row(), which promotes the NULL row to a keyed row
      and then updates all financial columns via ON DUPLICATE KEY UPDATE.

All other logic is unchanged.
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROCESSOR_NAME = "Authorize.Net"


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


def _get(row, key, idx=0):
    if not row:
        return None
    return row[key] if isinstance(row, dict) else row[idx]


# ============================================================
# GOLD LOOKUPS  (all use gc)
# ============================================================

def lookup_facility_key(gc, facility_id):
    if not facility_id:
        return None
    gc.execute(f"""
        SELECT facility_key FROM {GOLD_DB}.dim_facility
        WHERE facility_id = %s AND is_current = 1 LIMIT 1
    """, (facility_id,))
    return _get(gc.fetchone(), "facility_key")


def lookup_parker_key(gc, user_id):
    if not user_id:
        return None
    gc.execute(f"""
        SELECT parker_key FROM {GOLD_DB}.dim_parker
        WHERE customer_id = %s LIMIT 1
    """, (user_id,))
    return _get(gc.fetchone(), "parker_key")


def lookup_partner_account_key(gc, partner_id):
    if not partner_id:
        return None
    gc.execute(f"""
        SELECT partner_account_key FROM {GOLD_DB}.dim_partner_account
        WHERE account_id_source = %s AND is_current = 1 LIMIT 1
    """, (partner_id,))
    return _get(gc.fetchone(), "partner_account_key")


def lookup_vehicle_key(gc, vehicle_id):
    if not vehicle_id:
        return None
    gc.execute(f"""
        SELECT vehicle_key FROM {GOLD_DB}.dim_vehicle
        WHERE vehicle_id = %s LIMIT 1
    """, (vehicle_id,))
    return _get(gc.fetchone(), "vehicle_key")


def lookup_rateplan_key(gc, rate_id):
    if not rate_id:
        return None
    gc.execute(f"""
        SELECT rateplan_key FROM {GOLD_DB}.dim_rateplan
        WHERE pricing_id = %s AND is_current = 1 LIMIT 1
    """, (rate_id,))
    return _get(gc.fetchone(), "rateplan_key")


def lookup_promo_key(gc, promo_code):
    if not promo_code:
        return None
    gc.execute(f"""
        SELECT promo_key FROM {GOLD_DB}.dim_promo_code
        WHERE promo_code = %s AND is_current = 1 LIMIT 1
    """, (promo_code,))
    return _get(gc.fetchone(), "promo_key")


def lookup_processor_key(gc, processor_name):
    gc.execute(f"""
        SELECT processor_key FROM {GOLD_DB}.dim_processor
        WHERE processor_name = %s OR provider = %s LIMIT 1
    """, (processor_name, processor_name))
    return _get(gc.fetchone(), "processor_key")


def lookup_payment_method_key(gc, processor_key, method_type):
    if not processor_key or not method_type:
        return None
    gc.execute(f"""
        SELECT payment_method_key FROM {GOLD_DB}.dim_payment_method
        WHERE payment_method_id = %s AND method_type = %s LIMIT 1
    """, (processor_key, method_type))
    return _get(gc.fetchone(), "payment_method_key")


def lookup_event_key(bc, gc, event_id, partner_id):
    """
    FIX: two-step lookup to avoid cross-server JOIN.
    """
    if event_id:
        gc.execute(f"""
            SELECT event_key FROM {GOLD_DB}.dim_event
            WHERE event_id = %s LIMIT 1
        """, (event_id,))
        ek = _get(gc.fetchone(), "event_key")
        if ek:
            return ek

    if partner_id:
        try:
            bc.execute("""
                SELECT id FROM events
                WHERE partner_id = %s AND deleted_at IS NULL
                LIMIT 10
            """, (partner_id,))
            rows = bc.fetchall()
            if rows:
                event_ids = [int(r["id"] if isinstance(r, dict) else r[0]) for r in rows]
                ph = ",".join(["%s"] * len(event_ids))
                gc.execute(f"""
                    SELECT event_key FROM {GOLD_DB}.dim_event
                    WHERE event_id IN ({ph}) LIMIT 1
                """, tuple(event_ids))
                ek = _get(gc.fetchone(), "event_key")
                if ek:
                    return ek
        except Exception as e:
            log.warning(f"lookup_event_key via partner_id failed: {e}")

    return None


# def lookup_canonical_session_by_reservation(bc, gc, reservation_id):
#     """
#     FIX: fetch ticket IDs from bronze first, then look up canonical_session_key in gold.
#     """
#     if not reservation_id:
#         return None
#     try:
#         bc.execute("""
#             SELECT id FROM tickets
#             WHERE reservation_id = %s
#             ORDER BY id DESC LIMIT 10
#         """, (reservation_id,))
#         rows = bc.fetchall()
#         if not rows:
#             return None
#         ticket_ids = [int(r["id"] if isinstance(r, dict) else r[0]) for r in rows]
#         ph = ",".join(["%s"] * len(ticket_ids))
#         gc.execute(f"""
#             SELECT canonical_session_key FROM {GOLD_DB}.fact_parking_session
#             WHERE source_ticket_id IN ({ph})
#             ORDER BY canonical_session_key DESC LIMIT 1
#         """, tuple(ticket_ids))
#         row = gc.fetchone()
#         return _get(row, "canonical_session_key") if row else None
#     except Exception as e:
#         log.warning(f"lookup_canonical_session_by_reservation failed: {e}")
#     return None

def lookup_canonical_session_by_reservation(bc, gc, reservation_id):
    """
    FIX: source_ticket_id removed from fact_parking_session.
    Use composite key (extension_overstay_flag=0, source_id) instead.
    extension_overstay_flag=0 means the row came from the tickets table.
    """
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
            WHERE extension_overstay_flag = 0
              AND source_id IN ({ph})
            ORDER BY canonical_session_key DESC LIMIT 1
        """, tuple(ticket_ids))
        row = gc.fetchone()
        return _get(row, "canonical_session_key") if row else None
    except Exception as e:
        log.warning(f"lookup_canonical_session_by_reservation failed: {e}")
    return None


# ============================================================
# STATUS LOGIC
# ============================================================

def resolve_status(r):
    if r.get("cancelled_at"):
        return "cancelled"
    checkin_lower = str(r.get("checkin_status") or "").lower()
    if checkin_lower in ("no_show", "noshow"):
        return "no_show"
    if r.get("deleted_at"):
        return "expired"
    if str(r.get("is_charged") or "") == "1":
        return "used"
    return "booked"


# ============================================================
# FETCH SOURCE ROW FROM BRONZE
# source_reservation_id = reservations.id
# source_transaction_id = reservations.anet_transaction_id
# ============================================================

def fetch_reservation(bc, rid):
    bc.execute("""
        SELECT
            r.id, r.facility_id, r.event_id, r.partner_id, r.user_id, r.vehicle_id,
            r.rate_id, r.created_at, r.start_timestamp, r.end_timestamp,
            r.cancelled_at, r.checkin_status, r.deleted_at, r.is_charged,
            r.promocode, r.booking_source, r.license_plate, r.ticketech_code,
            r.thirdparty_integration_id,
            r.total, r.discount, r.processing_fee, r.tax_fee,
            r.refund_amount, r.parking_amount,
            r.anet_transaction_id
        FROM reservations r
        WHERE r.id = %s
          AND r.deleted_at IS NULL
        LIMIT 1
    """, (rid,))
    raw = bc.fetchone()
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    keys = ["id", "facility_id", "event_id", "partner_id", "user_id", "vehicle_id",
            "rate_id", "created_at", "start_timestamp", "end_timestamp",
            "cancelled_at", "checkin_status", "deleted_at", "is_charged",
            "promocode", "booking_source", "license_plate", "ticketech_code",
            "thirdparty_integration_id", "total", "discount", "processing_fee",
            "tax_fee", "refund_amount", "parking_amount", "anet_transaction_id"]
    return dict(zip(keys, raw))


# ============================================================
# DDL GUARDS
# ============================================================

def _col_exists(gc, table_name, col_name):
    gc.execute(f"""
        SELECT COUNT(*) AS cnt FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
          AND column_name  = %s
    """, (GOLD_DB, table_name, col_name))
    row = gc.fetchone()
    return (_get(row, "cnt") or 0) > 0


def _key_exists(gc, table_name, index_name):
    gc.execute(f"""
        SELECT COUNT(*) AS cnt FROM information_schema.statistics
        WHERE table_schema = %s
          AND table_name   = %s
          AND index_name   = %s
    """, (GOLD_DB, table_name, index_name))
    row = gc.fetchone()
    return (_get(row, "cnt") or 0) > 0


def ensure_fact_reservation(gc, gold_conn):
    gc.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.fact_reservation (
            reservation_key       BIGINT NOT NULL AUTO_INCREMENT,
            source_reservation_id BIGINT NULL,
            source_transaction_id BIGINT NULL,
            facility_key          BIGINT NULL,
            event_key             BIGINT NULL,
            partner_account_key   BIGINT NULL,
            parker_key            BIGINT NULL,
            vehicle_key           BIGINT NULL,
            rateplan_key          BIGINT NULL,
            created_ts            TIMESTAMP NULL,
            start_ts              TIMESTAMP NULL,
            end_ts                TIMESTAMP NULL,
            status                VARCHAR(50) NULL,
            promo_key             BIGINT NULL,
            booking_source        VARCHAR(250) NULL,
            license_plate         VARCHAR(10) NULL,
            booking_id            VARCHAR(250) NULL,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (reservation_key),
            UNIQUE KEY uk_source_reservation_id          (source_reservation_id),
            UNIQUE KEY uk_source_transaction_id          (source_transaction_id),
            KEY idx_fact_reservation_facility_key        (facility_key),
            KEY idx_fact_reservation_event_key           (event_key),
            KEY idx_fact_reservation_partner_key         (partner_account_key),
            KEY idx_fact_reservation_parker_key          (parker_key),
            KEY idx_fact_reservation_vehicle_key         (vehicle_key),
            KEY idx_fact_reservation_rate_plan_key       (rateplan_key),
            KEY idx_fact_reservation_promo_key           (promo_key),
            KEY idx_fact_reservation_booking_id          (booking_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    gold_conn.commit()

    for col_name, col_def in [
        ("source_reservation_id", "BIGINT NULL AFTER reservation_key"),
        ("source_transaction_id", "BIGINT NULL AFTER source_reservation_id"),
        ("partner_account_key",   "BIGINT NULL AFTER event_key"),
        ("booking_source",        "VARCHAR(250) NULL AFTER promo_key"),
        ("license_plate",         "VARCHAR(10)  NULL AFTER booking_source"),
        ("booking_id",            "VARCHAR(250) NULL AFTER license_plate"),
        ("created_at",            "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at",            "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ]:
        if not _col_exists(gc, "fact_reservation", col_name):
            gc.execute(f"ALTER TABLE {GOLD_DB}.fact_reservation ADD COLUMN {col_name} {col_def}")
            gold_conn.commit()
            log.info(f"✅ Added missing column fact_reservation.{col_name}")

    for key_name, key_col in [
        ("uk_source_reservation_id", "source_reservation_id"),
        ("uk_source_transaction_id", "source_transaction_id"),
    ]:
        if not _key_exists(gc, "fact_reservation", key_name):
            try:
                gc.execute(f"""
                    ALTER TABLE {GOLD_DB}.fact_reservation
                    ADD UNIQUE KEY {key_name} ({key_col})
                """)
                gold_conn.commit()
                log.info(f"✅ Added missing unique key {key_name} on fact_reservation")
            except Exception as e:
                log.warning(f"Could not add {key_name} (may already exist): {e}")
                gold_conn.rollback()


def ensure_fact_payment_minimal(gc, gold_conn):
    """
    fact_payment DDL:
      - source_transaction_id = anet_transaction_id  → PRIMARY unique key for upsert
      - source_reservation_id REMOVED
      - release_parking_amount DECIMAL(10,2) NULL (tickets only; NULL for reservations)
    """
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
            approved_flag           TINYINT(1) NULL,
            card_type               VARCHAR(50) NULL,
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
            processing_fees         DECIMAL(8,2) NULL,
            oversize_fees           DECIMAL(8,2) NULL,
            net_parking_amount      DECIMAL(12,2) NULL,
            refund_date             TIMESTAMP NULL,
            permit_prorate          DECIMAL(12,2) NULL,
            is_offline_payment      TINYINT(1) NOT NULL DEFAULT 0,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_transaction_id (source_transaction_id)
        )
    """)
    gold_conn.commit()

    for col_name, col_def in [
        ("card_type",               "VARCHAR(50) NULL AFTER approved_flag"),
        ("canonical_session_key",   "BIGINT NULL"),
        ("reservation_key",         "BIGINT NULL"),
        ("event_key",               "BIGINT NULL"),
        ("pass_subscription_key",   "BIGINT NULL"),
        ("permit_subscription_key", "BIGINT NULL"),
        ("discount_amount",         "DECIMAL(12,2) NULL"),
        ("base_parking_amount",     "DECIMAL(12,2) NULL"),
        ("release_parking_amount",  "DECIMAL(10,2) NULL AFTER void_amount"),
        ("net_parking_amount",      "DECIMAL(12,2) NULL"),
        ("refund_date",             "TIMESTAMP NULL"),
        ("permit_prorate",          "DECIMAL(12,2) NULL"),
        ("processing_fees",         "DECIMAL(8,2) NULL"),
        ("oversize_fees",           "DECIMAL(8,2) NULL"),
        ("is_offline_payment",      "TINYINT(1) NOT NULL DEFAULT 0"),
    ]:
        if not _col_exists(gc, "fact_payment", col_name):
            gc.execute(f"ALTER TABLE {GOLD_DB}.fact_payment ADD COLUMN {col_name} {col_def}")
            gold_conn.commit()
            log.info(f"✅ Added missing column fact_payment.{col_name}")

    if not _key_exists(gc, "fact_payment", "uk_source_transaction_id"):
        try:
            gc.execute(f"""
                ALTER TABLE {GOLD_DB}.fact_payment
                ADD UNIQUE KEY uk_source_transaction_id (source_transaction_id)
            """)
            gold_conn.commit()
            log.info("✅ Added missing unique key uk_source_transaction_id on fact_payment")
        except Exception as e:
            log.warning(f"Could not add uk_source_transaction_id (may already exist): {e}")
            gold_conn.rollback()


# ============================================================
# FACT_PAYMENT UPSERT (reservation-grain)
#
# FIX: source_transaction_id = anet_transaction_id if present, else NULL.
#      Synthetic negative ID (-(int(rid))) has been removed to match initial load.
#
# NULL anet handling:
#   When source_transaction_id IS NULL:
#     1. Look for existing row WHERE reservation_key = <key> AND
#        source_transaction_id IS NULL.
#     2. If found → UPDATE (avoids duplicate NULL rows per reservation).
#     3. If not found → INSERT with source_transaction_id = NULL.
#   When anet arrives later, gold_upsert_fact_payment DAG claims the NULL row.
#
# release_parking_amount = None (reservations do not carry this; tickets do).
# ============================================================

def upsert_fact_payment_for_reservation(bc, gc, gold_conn, rid, r_dict, reservation_key, event_key):
    created_at    = safe_dt(r_dict.get("created_at")) or datetime.now()
    facility_key  = lookup_facility_key(gc, r_dict.get("facility_id"))
    processor_key = lookup_processor_key(gc, PROCESSOR_NAME)

    grand_total = float(r_dict.get("total")         or 0)
    discount    = float(r_dict.get("discount")       or 0)
    proc_fee    = float(r_dict.get("processing_fee") or 0)
    tax         = float(r_dict.get("tax_fee")        or 0)
    refund      = float(r_dict.get("refund_amount")  or 0)
    parking     = float(r_dict.get("parking_amount") or 0)
    amount      = grand_total - refund

    payment_method_key = lookup_payment_method_key(gc, processor_key, "CARD")
    canon_sess         = lookup_canonical_session_by_reservation(bc, gc, rid)

    date_key = int(created_at.strftime("%Y%m%d"))
    time_key = int(created_at.strftime("%H%M%S"))

    # FIX: use anet_transaction_id if present, else NULL (no synthetic negative ID)
    anet_txn_id = r_dict.get("anet_transaction_id")
    source_transaction_id = int(anet_txn_id) if anet_txn_id is not None else None

    # Shared column/value list for both INSERT branches
    _cols = f"""
        INSERT INTO {GOLD_DB}.fact_payment (
            source_transaction_id,
            payment_ts_utc, date_key, facility_key, payment_time_key,
            payment_method_key, processor_key,
            canonical_session_key, reservation_key, event_key,
            permit_subscription_key, pass_subscription_key,
            transaction_type, amount, approved_flag, processor_txn_id, reason_key,
            sales_tax, transaction_date, cc_refund_amount, city_surcharge,
            posted_gross_amount, discount_amount, base_parking_amount,
            validate_amount, void_amount, release_parking_amount,
            processing_fees, oversize_fees,
            is_offline_payment, created_at
        )
    """
    _vals = (
        created_at, date_key, facility_key, time_key,
        payment_method_key, processor_key,
        canon_sess, reservation_key, event_key,
        None, None,                                          # permit/pass subscription keys
        "RESERVATION", amount, 1 if amount > 0 else 0, None, None,
        tax, created_at, refund, 0.0,
        grand_total, discount, parking,
        0.0, 0.0, None,                                     # validate, void, release_parking
        proc_fee, 0.0,
        0, datetime.now(),
    )
    _on_dup = """
        ON DUPLICATE KEY UPDATE
            payment_ts_utc        = VALUES(payment_ts_utc),
            date_key              = VALUES(date_key),
            facility_key          = COALESCE(VALUES(facility_key),            facility_key),
            payment_method_key    = COALESCE(VALUES(payment_method_key),      payment_method_key),
            processor_key         = COALESCE(VALUES(processor_key),           processor_key),
            canonical_session_key = COALESCE(VALUES(canonical_session_key),   canonical_session_key),
            reservation_key       = COALESCE(VALUES(reservation_key),         reservation_key),
            event_key             = COALESCE(VALUES(event_key),               event_key),
            transaction_type      = VALUES(transaction_type),
            amount                = VALUES(amount),
            approved_flag         = VALUES(approved_flag),
            sales_tax             = VALUES(sales_tax),
            transaction_date      = VALUES(transaction_date),
            cc_refund_amount      = VALUES(cc_refund_amount),
            posted_gross_amount   = VALUES(posted_gross_amount),
            discount_amount       = VALUES(discount_amount),
            base_parking_amount   = VALUES(base_parking_amount),
            processing_fees       = VALUES(processing_fees)
    """

    if source_transaction_id is not None:
        # Standard ON DUPLICATE KEY UPDATE keyed by source_transaction_id
        gc.execute(
            _cols + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) " + _on_dup,
            (source_transaction_id,) + _vals,
        )
    else:
        # No anet yet: find existing NULL row by reservation_key, UPDATE or INSERT
        existing_payment_key = None
        if reservation_key:
            gc.execute(f"""
                SELECT payment_key FROM {GOLD_DB}.fact_payment
                WHERE reservation_key = %s
                  AND source_transaction_id IS NULL
                ORDER BY payment_key DESC LIMIT 1
            """, (reservation_key,))
            row = gc.fetchone()
            if row:
                existing_payment_key = _get(row, "payment_key")

        if existing_payment_key:
            # UPDATE the existing NULL row
            gc.execute(f"""
                UPDATE {GOLD_DB}.fact_payment SET
                    payment_ts_utc        = %s,
                    date_key              = %s,
                    facility_key          = COALESCE(%s, facility_key),
                    payment_time_key      = %s,
                    payment_method_key    = COALESCE(%s, payment_method_key),
                    processor_key         = COALESCE(%s, processor_key),
                    canonical_session_key = COALESCE(%s, canonical_session_key),
                    reservation_key       = COALESCE(%s, reservation_key),
                    event_key             = COALESCE(%s, event_key),
                    transaction_type      = %s,
                    amount                = %s,
                    approved_flag         = %s,
                    sales_tax             = %s,
                    transaction_date      = %s,
                    cc_refund_amount      = %s,
                    city_surcharge        = %s,
                    posted_gross_amount   = %s,
                    discount_amount       = %s,
                    base_parking_amount   = %s,
                    processing_fees       = %s,
                    is_offline_payment    = %s
                WHERE payment_key = %s
            """, (
                created_at, date_key, facility_key, time_key,
                payment_method_key, processor_key,
                canon_sess, reservation_key, event_key,
                "RESERVATION", amount, 1 if amount > 0 else 0,
                tax, created_at, refund, 0.0,
                grand_total, discount, parking,
                proc_fee, 0,
                existing_payment_key,
            ))
        else:
            # INSERT with source_transaction_id = NULL
            gc.execute(
                _cols + " VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                _vals,
            )

    gold_conn.commit()
    log.info(
        f"[RESERVATION] fact_payment upserted "
        f"source_transaction_id={source_transaction_id} "
        f"reservation_key={reservation_key} amount={amount}"
    )


# ============================================================
# MAIN CALLABLE
# ============================================================

def upsert_fact_reservation(**context):
    reservation_ids = context["dag_run"].conf.get("reservation_ids", [])

    if not reservation_ids:
        log.info("No reservation_ids received — nothing to do")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True)
    gc = gold_conn.cursor(dictionary=True)

    try:
        ensure_fact_reservation(gc, gold_conn)
        ensure_fact_payment_minimal(gc, gold_conn)

        for rid in reservation_ids:
            rid = int(rid)
            log.info(f"\n🔄 Processing reservation_id={rid}")

            # 1. Fetch source row from BRONZE
            r_dict = fetch_reservation(bc, rid)
            if not r_dict:
                log.warning(f"⚠️ reservation_id={rid} not found or deleted — skipping")
                continue

            # 2. Resolve dimension keys from GOLD
            facility_key        = lookup_facility_key(gc, r_dict.get("facility_id"))
            event_key           = lookup_event_key(bc, gc, r_dict.get("event_id"), r_dict.get("partner_id"))
            partner_account_key = lookup_partner_account_key(gc, r_dict.get("partner_id"))
            parker_key          = lookup_parker_key(gc, r_dict.get("user_id"))
            vehicle_key         = lookup_vehicle_key(gc, r_dict.get("vehicle_id"))
            rateplan_key        = lookup_rateplan_key(gc, r_dict.get("rate_id"))
            promo_key           = lookup_promo_key(gc, r_dict.get("promocode"))

            # 3. Resolve booking_source from BRONZE thirdparty_integrations
            ti_id   = r_dict.get("thirdparty_integration_id")
            ti_name = None
            if ti_id:
                bc.execute("""
                    SELECT name FROM thirdparty_integrations WHERE id = %s LIMIT 1
                """, (ti_id,))
                ti_row  = bc.fetchone()
                ti_name = _get(ti_row, "name") if ti_row else None
            booking_source = ti_name if ti_name else r_dict.get("booking_source")

            # 4. Derived fields
            lp_raw     = str(r_dict.get("license_plate") or "")[:10] or None
            booking_id = (str(r_dict.get("ticketech_code"))
                          if r_dict.get("ticketech_code") is not None else None)
            status     = resolve_status(r_dict)

            # source_transaction_id = anet_transaction_id (NULL when absent)
            anet_txn_id = r_dict.get("anet_transaction_id")
            source_transaction_id = int(anet_txn_id) if anet_txn_id is not None else None

            # 5. Upsert fact_reservation in GOLD (key = source_reservation_id = rid)
            gc.execute(f"""
                INSERT INTO {GOLD_DB}.fact_reservation (
                    source_reservation_id,
                    source_transaction_id,
                    facility_key, event_key, partner_account_key,
                    parker_key, vehicle_key, rateplan_key,
                    created_ts, start_ts, end_ts, status,
                    promo_key, booking_source, license_plate, booking_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    source_transaction_id = COALESCE(VALUES(source_transaction_id), source_transaction_id),
                    facility_key          = VALUES(facility_key),
                    event_key             = VALUES(event_key),
                    partner_account_key   = VALUES(partner_account_key),
                    parker_key            = VALUES(parker_key),
                    vehicle_key           = VALUES(vehicle_key),
                    rateplan_key          = VALUES(rateplan_key),
                    created_ts            = VALUES(created_ts),
                    start_ts              = VALUES(start_ts),
                    end_ts                = VALUES(end_ts),
                    status                = VALUES(status),
                    promo_key             = VALUES(promo_key),
                    booking_source        = VALUES(booking_source),
                    license_plate         = VALUES(license_plate),
                    booking_id            = VALUES(booking_id)
            """, (
                rid,
                source_transaction_id,
                facility_key, event_key, partner_account_key,
                parker_key, vehicle_key, rateplan_key,
                safe_dt(r_dict.get("created_at")),
                safe_dt(r_dict.get("start_timestamp")),
                safe_dt(r_dict.get("end_timestamp")),
                status,
                promo_key, booking_source, lp_raw, booking_id,
            ))
            gold_conn.commit()

            # 6. Retrieve surrogate key
            gc.execute(f"""
                SELECT reservation_key FROM {GOLD_DB}.fact_reservation
                WHERE source_reservation_id = %s LIMIT 1
            """, (rid,))
            rk_row          = gc.fetchone()
            reservation_key = _get(rk_row, "reservation_key") if rk_row else None

            log.info(
                f"✅ fact_reservation upserted: reservation_key={reservation_key} "
                f"source_reservation_id={rid} source_transaction_id={source_transaction_id}"
            )

            # 7. Upsert the reservation-grain payment row
            upsert_fact_payment_for_reservation(
                bc, gc, gold_conn, rid, r_dict, reservation_key, event_key
            )

    except Exception as e:
        gold_conn.rollback()
        log.error(f"❌ Error: {e}", exc_info=True)
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
    dag_id="gold_upsert_fact_reservation",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_reservation",
        python_callable=upsert_fact_reservation
    )