"""
DAG: gold_upsert_fact_parking_session
CDC incremental upsert for fact_parking_session.

CHANGES (this version):
  - policy_key BIGINT NULL ADDED.
      Mapped from tickets.policy_id → dim_policy.policy_key for SOURCE 0 (tickets) only.
      NULL for overstay_tickets (flag=1) and ticket_extends (flag=2).
  - lookup_policy_key() helper ADDED.
  - tickets SELECT includes t.policy_id.
  - Base ticket INSERT and ON DUPLICATE KEY UPDATE include policy_key.
  - _do_payment_upsert: amount = NULL (not 0) when no anet_transaction exists.

  NEW (validation_refund_flag):
  - validation_refund_flag TINYINT(1) NOT NULL DEFAULT 0 ADDED.
      Set to 1 when validation_refunds.reference_key matches the ticket's
      ticket_number.  Applied to all three sources (flag=0/1/2) that share
      the same ticket_number.  Uses GREATEST() in ON DUPLICATE KEY UPDATE so
      the flag is never downgraded from 1 back to 0.
  - lookup_validation_refund_flag() helper ADDED.
  - handle_validation_refund_update() ADDED — triggered when
    validation_refund_ids arrive in conf (CDC event from validation_refunds
    table).  Performs a targeted bulk UPDATE without re-processing tickets.
  - upsert_fact_parking_session: accepts validation_refund_ids from conf in
    addition to ticket_ids; runs both handlers if both are present.
"""

import logging
from datetime import datetime, date as date_type, time as time_type

from airflow import DAG
from airflow.operators.python import PythonOperator
from db_config import get_bronze_connection, get_gold_connection, GOLD_DB

PROCESSOR_NAME = "Authorize.Net"


# ============================================================
# GENERIC HELPERS
# ============================================================

def safe_dt(value):
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


def _g(row, key, idx):
    if isinstance(row, dict):
        return row.get(key)
    return row[idx] if row and len(row) > idx else None


def get_date_part(value):
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


def get_time_part(value):
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
# GOLD DIM LOOKUPS
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


def lookup_vehicle_key(gc, vehicle_id):
    if not vehicle_id:
        return None
    gc.execute(f"""
        SELECT vehicle_key FROM {GOLD_DB}.dim_vehicle
        WHERE vehicle_id = %s LIMIT 1
    """, (vehicle_id,))
    row = gc.fetchone()
    return _g(row, "vehicle_key", 0) if row else None


def lookup_parker_key(gc, user_id):
    if not user_id:
        return None
    gc.execute(f"""
        SELECT parker_key FROM {GOLD_DB}.dim_parker
        WHERE customer_id = %s LIMIT 1
    """, (user_id,))
    row = gc.fetchone()
    return _g(row, "parker_key", 0) if row else None


def lookup_partner_account_key(gc, partner_id):
    if not partner_id:
        return None
    gc.execute(f"""
        SELECT partner_account_key FROM {GOLD_DB}.dim_partner_account
        WHERE account_id_source = %s AND is_current = 1 LIMIT 1
    """, (partner_id,))
    row = gc.fetchone()
    return _g(row, "partner_account_key", 0) if row else None


def lookup_rate_plan_key(gc, rate_id):
    if not rate_id:
        return None
    gc.execute(f"""
        SELECT rateplan_key FROM {GOLD_DB}.dim_rateplan
        WHERE pricing_id = %s AND is_current = 1 LIMIT 1
    """, (rate_id,))
    row = gc.fetchone()
    return _g(row, "rateplan_key", 0) if row else None


def lookup_promo_key(gc, promo_code):
    if not promo_code:
        return None
    gc.execute(f"""
        SELECT promo_key FROM {GOLD_DB}.dim_promo_code
        WHERE promo_code = %s AND is_current = 1 LIMIT 1
    """, (promo_code,))
    row = gc.fetchone()
    return _g(row, "promo_key", 0) if row else None


def lookup_date_key(gc, value):
    d = get_date_part(value)
    if d is None:
        return None
    gc.execute(f"""
        SELECT date_key FROM {GOLD_DB}.dim_date
        WHERE full_date = %s LIMIT 1
    """, (d,))
    row = gc.fetchone()
    return _g(row, "date_key", 0) if row else None


def lookup_time_key(gc, value):
    t = get_time_part(value)
    if t is None:
        return None
    gc.execute(f"""
        SELECT time_key FROM {GOLD_DB}.dim_time
        WHERE full_time = %s LIMIT 1
    """, (t,))
    row = gc.fetchone()
    return _g(row, "time_key", 0) if row else None


def lookup_processor_key(gc, processor_name):
    gc.execute(f"""
        SELECT processor_key FROM {GOLD_DB}.dim_processor
        WHERE processor_name = %s LIMIT 1
    """, (processor_name,))
    row = gc.fetchone()
    return _g(row, "processor_key", 0) if row else None


def lookup_payment_method_key(gc, processor_key, method_type):
    if processor_key is None or not method_type:
        return None
    gc.execute(f"""
        SELECT payment_method_key FROM {GOLD_DB}.dim_payment_method
        WHERE payment_method_id = %s AND method_type = %s LIMIT 1
    """, (processor_key, method_type))
    row = gc.fetchone()
    return _g(row, "payment_method_key", 0) if row else None


def lookup_canonical_session_key(gc, source_id, extension_overstay_flag):
    if not source_id:
        return None
    gc.execute(f"""
        SELECT canonical_session_key
        FROM {GOLD_DB}.fact_parking_session
        WHERE extension_overstay_flag = %s
          AND source_id               = %s
        LIMIT 1
    """, (extension_overstay_flag, source_id))
    row = gc.fetchone()
    return _g(row, "canonical_session_key", 0) if row else None


def lookup_reservation_key(gc, reservation_id):
    if not reservation_id:
        return None
    gc.execute(f"""
        SELECT reservation_key FROM {GOLD_DB}.fact_reservation
        WHERE source_reservation_id = %s LIMIT 1
    """, (reservation_id,))
    row = gc.fetchone()
    return _g(row, "reservation_key", 0) if row else None


def lookup_event_key_by_reservation(gc, reservation_id):
    if not reservation_id:
        return None
    gc.execute(f"""
        SELECT event_key FROM {GOLD_DB}.fact_reservation
        WHERE source_reservation_id = %s AND event_key IS NOT NULL LIMIT 1
    """, (reservation_id,))
    row = gc.fetchone()
    return _g(row, "event_key", 0) if row else None


def lookup_permit_subscription_key(gc, permit_request_id):
    if not permit_request_id:
        return None
    gc.execute(f"""
        SELECT permit_subscription_key FROM {GOLD_DB}.fact_permit_subscription
        WHERE source_permit_id = %s LIMIT 1
    """, (permit_request_id,))
    row = gc.fetchone()
    return _g(row, "permit_subscription_key", 0) if row else None


def lookup_pass_subscription_key(gc, user_pass_id):
    if not user_pass_id:
        return None
    gc.execute(f"""
        SELECT pass_subscription_key FROM {GOLD_DB}.fact_passes
        WHERE source_user_pass_id = %s LIMIT 1
    """, (user_pass_id,))
    row = gc.fetchone()
    return _g(row, "pass_subscription_key", 0) if row else None


# ── NEW: dim_policy lookup ────────────────────────────────────────────────────
def lookup_policy_key(gc, policy_id):
    """
    Resolves policy_key from dim_policy using tickets.policy_id.
    Returns None when policy_id is NULL or no matching row exists.
    Only called for SOURCE 0 (tickets table); always NULL for overstay/extend.
    """
    if not policy_id:
        return None
    gc.execute(f"""
        SELECT policy_key FROM {GOLD_DB}.dim_policy
        WHERE policy_id = %s AND is_current = 1 LIMIT 1
    """, (policy_id,))
    row = gc.fetchone()
    return _g(row, "policy_key", 0) if row else None


# ── NEW: validation_refund_flag lookup ───────────────────────────────────────
def lookup_validation_refund_flag(bc, ticket_number):
    """
    Returns 1 if ticket_number has at least one row in validation_refunds
    (reference_key = ticket_number), 0 otherwise.
    Called once per parent ticket; result is reused for extensions/overstays
    that share the same ticket_number.
    """
    if not ticket_number:
        return 0
    bc.execute(
        "SELECT COUNT(*) AS cnt FROM validation_refunds "
        "WHERE reference_key = %s LIMIT 1",
        (ticket_number,),
    )
    row = bc.fetchone()
    cnt = row["cnt"] if isinstance(row, dict) else row[0]
    return 1 if (cnt or 0) > 0 else 0


# ============================================================
# FLAG + FK RESOLUTION  (reserv_permit_pass_flag)
# ============================================================

def resolve_flags_and_fks(gc, ticket):
    rsv_id    = ticket.get("reservation_id")
    permit_id = ticket.get("permit_request_id")
    upass_id  = ticket.get("user_pass_id")

    if rsv_id:
        flag                    = 1
        reservation_key         = lookup_reservation_key(gc, rsv_id)
        event_key               = lookup_event_key_by_reservation(gc, rsv_id)
        permit_subscription_key = None
        pass_subscription_key   = None
    elif permit_id:
        flag                    = 2
        reservation_key         = None
        event_key               = None
        permit_subscription_key = lookup_permit_subscription_key(gc, permit_id)
        pass_subscription_key   = None
    elif upass_id:
        flag                    = 3
        reservation_key         = None
        event_key               = None
        permit_subscription_key = None
        pass_subscription_key   = lookup_pass_subscription_key(gc, upass_id)
    else:
        flag                    = 0
        reservation_key         = None
        event_key               = None
        permit_subscription_key = None
        pass_subscription_key   = None

    return flag, reservation_key, event_key, permit_subscription_key, pass_subscription_key


# ============================================================
# SESSION STATUS
# ============================================================

def resolve_session_status(ticket):
    del_at   = ticket.get("deleted_at")
    can_at   = ticket.get("cancelled_at")
    is_ckout = ticket.get("is_checkout")
    ckout_dt = ticket.get("checkout_datetime")
    ckout_t  = ticket.get("checkout_time")
    is_ckin  = ticket.get("is_checkin")
    ckin_t   = ticket.get("checkin_time")
    ckin_dt  = ticket.get("check_in_datetime")
    est      = ticket.get("estimated_checkout")

    if del_at or can_at:
        return "void"
    if is_ckout == 1 or ckout_dt or ckout_t:
        return "closed"
    if is_ckin == 1 or ckin_t or ckin_dt:
        return "open"
    if est:
        return "estimated"
    return "unknown"


# ============================================================
# FACT_PAYMENT UPSERT
# CHANGED: amount = NULL (not 0) when source_txn_id is None (no anet row).
# ============================================================

def _do_payment_upsert(gc, logger, source_txn_id, created_at,
                       facility_key, canonical_session_key,
                       reservation_key, permit_subscription_key,
                       pass_subscription_key, is_offline,
                       grand_total, parking_amount, processing_fee,
                       tax_fee, paid_amount, surcharge_fee,
                       refund_amount, discount_amount, source_kind):

    processor_key      = lookup_processor_key(gc, PROCESSOR_NAME)
    method_type        = "CASH" if is_offline else "CARD"
    payment_method_key = lookup_payment_method_key(gc, processor_key, method_type)

    # CHANGED: NULL when no anet — do not fall back to source-table totals.
    # if source_txn_id is not None:
    #     amount        = float(paid_amount or grand_total or 0)
    #     approved_flag = 1 if amount > 0 else 0
    # else:
    #     amount        = None
    #     approved_flag = 0



    # CHANGED: NULL when no anet — do not fall back to source-table totals.
    if source_txn_id is not None:
        amount         = float(paid_amount or grand_total or 0)
        approved_flag  = 1 if amount > 0 else 0
        payment_ts_utc = created_at   # anet row exists — use anet_transactions.created_at
    else:
        amount         = None
        approved_flag  = 0
        payment_ts_utc = None         # no anet row — payment_ts_utc must be NULL    

    date_key = int(created_at.strftime("%Y%m%d"))
    time_key = int(created_at.strftime("%H%M%S"))

    _COLS = f"""
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
            validate_amount, void_amount,
            processing_fees, oversize_fees,
            is_offline_payment, created_at
        )
    """
    # _vals = (
    #     created_at, date_key, facility_key, time_key,
    _vals = (
        payment_ts_utc, date_key, facility_key, time_key,
        payment_method_key, processor_key,
        canonical_session_key, reservation_key, None,
        permit_subscription_key, pass_subscription_key,
        source_kind, amount, approved_flag,
        None, None,
        float(tax_fee        or 0), created_at,
        float(refund_amount  or 0), float(surcharge_fee or 0),
        float(grand_total    or 0), float(discount_amount or 0),
        float(parking_amount or 0), float(paid_amount    or 0) if paid_amount is not None else None,
        0.0,
        float(processing_fee or 0), 0.0,
        int(is_offline or 0), datetime.now(),
    )
    _ON_DUP = """
        ON DUPLICATE KEY UPDATE
            amount                  = VALUES(amount),
            canonical_session_key   = COALESCE(VALUES(canonical_session_key), canonical_session_key),
            approved_flag           = VALUES(approved_flag),
            facility_key            = COALESCE(VALUES(facility_key),            facility_key),
            payment_method_key      = COALESCE(VALUES(payment_method_key),      payment_method_key),
            processor_key           = COALESCE(VALUES(processor_key),           processor_key),
            reservation_key         = COALESCE(VALUES(reservation_key),         reservation_key),
            permit_subscription_key = COALESCE(VALUES(permit_subscription_key), permit_subscription_key),
            pass_subscription_key   = COALESCE(VALUES(pass_subscription_key),   pass_subscription_key),
            sales_tax               = VALUES(sales_tax),
            transaction_date        = VALUES(transaction_date),
            cc_refund_amount        = VALUES(cc_refund_amount),
            city_surcharge          = VALUES(city_surcharge),
            posted_gross_amount     = VALUES(posted_gross_amount),
            discount_amount         = VALUES(discount_amount),
            base_parking_amount     = VALUES(base_parking_amount),
            validate_amount         = VALUES(validate_amount),
            processing_fees         = VALUES(processing_fees),
            is_offline_payment      = VALUES(is_offline_payment)
    """

    if source_txn_id is not None:
        gc.execute(
            _COLS + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) " + _ON_DUP,
            (source_txn_id,) + _vals,
        )
    else:
        existing_payment_key = None
        for col, val in [
            ("canonical_session_key",   canonical_session_key),
            ("reservation_key",         reservation_key),
            ("permit_subscription_key", permit_subscription_key),
            ("pass_subscription_key",   pass_subscription_key),
        ]:
            if val:
                gc.execute(f"""
                    SELECT payment_key FROM {GOLD_DB}.fact_payment
                    WHERE {col} = %s AND source_transaction_id IS NULL
                    ORDER BY payment_key DESC LIMIT 1
                """, (val,))
                row = gc.fetchone()
                if row:
                    existing_payment_key = (
                        row["payment_key"] if isinstance(row, dict) else row[0]
                    )
                    break

        if existing_payment_key:
            gc.execute(f"""
                UPDATE {GOLD_DB}.fact_payment SET
                    payment_ts_utc          = %s,
                    date_key                = %s,
                    facility_key            = COALESCE(%s, facility_key),
                    payment_time_key        = %s,
                    payment_method_key      = COALESCE(%s, payment_method_key),
                    processor_key           = COALESCE(%s, processor_key),
                    canonical_session_key   = COALESCE(%s, canonical_session_key),
                    reservation_key         = COALESCE(%s, reservation_key),
                    permit_subscription_key = COALESCE(%s, permit_subscription_key),
                    pass_subscription_key   = COALESCE(%s, pass_subscription_key),
                    transaction_type        = %s,
                    amount                  = %s,
                    approved_flag           = %s,
                    sales_tax               = %s,
                    transaction_date        = %s,
                    cc_refund_amount        = %s,
                    city_surcharge          = %s,
                    posted_gross_amount     = %s,
                    discount_amount         = %s,
                    base_parking_amount     = %s,
                    validate_amount         = %s,
                    processing_fees         = %s,
                    is_offline_payment      = %s
                WHERE payment_key = %s
            """, (
                created_at, date_key, facility_key, time_key,
                payment_method_key, processor_key,
                canonical_session_key, reservation_key,
                permit_subscription_key, pass_subscription_key,
                source_kind, amount, approved_flag,
                float(tax_fee        or 0), created_at,
                float(refund_amount  or 0), float(surcharge_fee   or 0),
                float(grand_total    or 0), float(discount_amount or 0),
                float(parking_amount or 0), float(paid_amount     or 0) if paid_amount is not None else None,
                float(processing_fee or 0), int(is_offline or 0),
                existing_payment_key,
            ))
        else:
            gc.execute(
                _COLS + " VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                _vals,
            )

    logger.info(
        f"[PAYMENT:{source_kind}] upserted source_txn_id={source_txn_id} rows={gc.rowcount}"
    )


# ============================================================
# PER-SOURCE PAYMENT HELPERS
# ============================================================

def upsert_payment_ticket(bc, gc, logger, ticket_id,
                          reservation_key, permit_subscription_key, pass_subscription_key):
    bc.execute("""
        SELECT id, facility_id, reservation_id, permit_request_id,
               is_offline_payment, created_at, grand_total, parking_amount,
               processing_fee, tax_fee, paid_amount, surcharge_fee,
               refund_amount, discount_amount, anet_transaction_id
        FROM tickets
        WHERE id = %s LIMIT 1
    """, (ticket_id,))
    row = bc.fetchone()
    if not row:
        logger.warning(f"[PAYMENT:TICKET] ticket_id={ticket_id} not found")
        return
    r = row if isinstance(row, dict) else dict(zip(
        ["id", "facility_id", "reservation_id", "permit_request_id",
         "is_offline_payment", "created_at", "grand_total", "parking_amount",
         "processing_fee", "tax_fee", "paid_amount", "surcharge_fee",
         "refund_amount", "discount_amount", "anet_transaction_id"], row))

    created_at    = safe_dt(r["created_at"]) or datetime.now()
    facility_key  = lookup_facility_key(gc, r["facility_id"])
    canon_key     = lookup_canonical_session_key(gc, int(r["id"]), 0)
    anet_txn_id   = r.get("anet_transaction_id")
    source_txn_id = int(anet_txn_id) if anet_txn_id else None

    _do_payment_upsert(
        gc, logger,
        source_txn_id           = source_txn_id,
        created_at              = created_at,
        facility_key            = facility_key,
        canonical_session_key   = canon_key,
        reservation_key         = reservation_key,
        permit_subscription_key = permit_subscription_key,
        pass_subscription_key   = pass_subscription_key,
        is_offline              = r["is_offline_payment"],
        grand_total             = r["grand_total"],
        parking_amount          = r["parking_amount"],
        processing_fee          = r["processing_fee"],
        tax_fee                 = r["tax_fee"],
        paid_amount             = r["paid_amount"],
        surcharge_fee           = r["surcharge_fee"],
        refund_amount           = r["refund_amount"],
        discount_amount         = r["discount_amount"],
        source_kind             = "TICKET",
    )


def upsert_payment_overstay(bc, gc, logger, overstay_id):
    bc.execute("""
        SELECT id, facility_id, is_offline_payment, payment_date, created_at,
               grand_total, parking_amount, discount_amount, tax_fee,
               processing_fee, 0 AS additional_fee, 0 AS surcharge_fee,
               penalty_fee, reservation_id, anet_transaction_id
        FROM overstay_tickets
        WHERE id = %s LIMIT 1
    """, (overstay_id,))
    row = bc.fetchone()
    if not row:
        logger.warning(f"[PAYMENT:OVERSTAY] overstay_id={overstay_id} not found")
        return
    r = row if isinstance(row, dict) else dict(zip(
        ["id", "facility_id", "is_offline_payment", "payment_date", "created_at",
         "grand_total", "parking_amount", "discount_amount", "tax_fee",
         "processing_fee", "additional_fee", "surcharge_fee", "penalty_fee",
         "reservation_id", "anet_transaction_id"], row))

    created_at      = safe_dt(r.get("payment_date") or r["created_at"]) or datetime.now()
    facility_key    = lookup_facility_key(gc, r["facility_id"])
    canon_key       = lookup_canonical_session_key(gc, int(r["id"]), 1)
    reservation_key = lookup_reservation_key(gc, r.get("reservation_id"))
    anet_txn_id     = r.get("anet_transaction_id")
    source_txn_id   = int(anet_txn_id) if anet_txn_id else None

    _do_payment_upsert(
        gc, logger,
        source_txn_id           = source_txn_id,
        created_at              = created_at,
        facility_key            = facility_key,
        canonical_session_key   = canon_key,
        reservation_key         = reservation_key,
        permit_subscription_key = None,
        pass_subscription_key   = None,
        is_offline              = r.get("is_offline_payment", 0),
        grand_total             = r["grand_total"],
        parking_amount          = r["parking_amount"],
        processing_fee          = float(r.get("processing_fee") or 0) + float(r.get("additional_fee") or 0),
        tax_fee                 = r["tax_fee"],
        paid_amount             = r["grand_total"],
        surcharge_fee           = r.get("surcharge_fee", 0),
        refund_amount           = 0,
        discount_amount         = r["discount_amount"],
        source_kind             = "OVERSTAY",
    )


def upsert_payment_extend(bc, gc, logger, extend_id,
                          reservation_key, permit_subscription_key, pass_subscription_key):
    bc.execute("""
        SELECT te.id, te.facility_id, te.created_at, te.grand_total,
               te.parking_amounts, te.discount_amount, te.tax_fee,
               te.processing_fee, te.additional_fee, te.surcharge_fee,
               te.oversize_fee, te.refund_amount, te.net_parking_amount,
               te.anet_transaction_id
        FROM ticket_extends te
        WHERE te.id = %s AND te.deleted_at IS NULL LIMIT 1
    """, (extend_id,))
    row = bc.fetchone()
    if not row:
        logger.warning(f"[PAYMENT:EXTEND] extend_id={extend_id} not found")
        return
    r = row if isinstance(row, dict) else dict(zip(
        ["id", "facility_id", "created_at", "grand_total", "parking_amounts",
         "discount_amount", "tax_fee", "processing_fee", "additional_fee",
         "surcharge_fee", "oversize_fee", "refund_amount", "net_parking_amount",
         "anet_transaction_id"], row))

    created_at    = safe_dt(r["created_at"]) or datetime.now()
    facility_key  = lookup_facility_key(gc, r["facility_id"])
    canon_key     = lookup_canonical_session_key(gc, int(r["id"]), 2)
    anet_txn_id   = r.get("anet_transaction_id")
    source_txn_id = int(anet_txn_id) if anet_txn_id else None

    _do_payment_upsert(
        gc, logger,
        source_txn_id           = source_txn_id,
        created_at              = created_at,
        facility_key            = facility_key,
        canonical_session_key   = canon_key,
        reservation_key         = reservation_key,
        permit_subscription_key = permit_subscription_key,
        pass_subscription_key   = pass_subscription_key,
        is_offline              = 0,
        grand_total             = r["grand_total"],
        parking_amount          = r.get("parking_amounts", 0),
        processing_fee          = float(r.get("processing_fee") or 0) + float(r.get("additional_fee") or 0),
        tax_fee                 = r["tax_fee"],
        paid_amount             = r["grand_total"],
        surcharge_fee           = r.get("surcharge_fee", 0),
        refund_amount           = r.get("refund_amount", 0),
        discount_amount         = r.get("discount_amount", 0),
        source_kind             = "EXTEND",
    )


# ============================================================
# NEW: VALIDATION REFUND UPDATE HANDLER
# Called when validation_refund_ids arrive in conf.
# Sets validation_refund_flag = 1 on all fact_parking_session
# rows whose ticket_number matches validation_refunds.reference_key.
# ============================================================

def handle_validation_refund_update(bc, gc, gold_conn, logger, validation_refund_ids):
    """
    Looks up reference_key from each validation_refund row, then
    bulk-updates fact_parking_session.validation_refund_flag = 1
    for every session row (any extension_overstay_flag value) that
    shares that ticket_number.
    """
    if not validation_refund_ids:
        return
    ph = ",".join(["%s"] * len(validation_refund_ids))
    bc.execute(
        f"SELECT DISTINCT reference_key FROM validation_refunds "
        f"WHERE id IN ({ph}) AND reference_key IS NOT NULL",
        tuple(validation_refund_ids),
    )
    rows = bc.fetchall()
    ticket_numbers = [
        (r["reference_key"] if isinstance(r, dict) else r[0])
        for r in rows
        if (r["reference_key"] if isinstance(r, dict) else r[0])
    ]
    if not ticket_numbers:
        logger.info("[VR:SESSION] No valid reference_keys found — nothing to update")
        return
    ph2 = ",".join(["%s"] * len(ticket_numbers))
    gc.execute(
        f"UPDATE {GOLD_DB}.fact_parking_session "
        f"SET validation_refund_flag = 1 "
        f"WHERE ticket_number IN ({ph2})",
        tuple(ticket_numbers),
    )
    gold_conn.commit()
    logger.info(
        f"[VR:SESSION] validation_refund_flag=1 applied to {gc.rowcount} rows "
        f"| ticket_numbers={ticket_numbers}"
    )


# ============================================================
# MAIN TASK
# ============================================================

def upsert_fact_parking_session(**context):
    logger = logging.getLogger(__name__)

    ticket_ids            = context["dag_run"].conf.get("ticket_ids", [])
    validation_refund_ids = context["dag_run"].conf.get("validation_refund_ids", [])

    if not ticket_ids and not validation_refund_ids:
        logger.warning("No ticket_ids or validation_refund_ids received")
        return

    bronze_conn = get_bronze_connection()
    gold_conn   = get_gold_connection()
    bc = bronze_conn.cursor(dictionary=True, buffered=True)
    gc = gold_conn.cursor(dictionary=True, buffered=True)

    try:
        # ── NEW: handle validation_refund_ids (set validation_refund_flag) ────────
        if validation_refund_ids:
            handle_validation_refund_update(bc, gc, gold_conn, logger, validation_refund_ids)
            if not ticket_ids:
                return   # pure validation-refund trigger — nothing else to do

        for tid in ticket_ids:
            logger.info(f"[START] Processing ticket_id={tid}")

            # ── Fetch parent ticket ───────────────────────────────────────────
            # CHANGED: added t.policy_id to SELECT
            bc.execute("""
                SELECT
                    t.id, t.facility_id, t.vehicle_id, t.user_id,
                    t.partner_id, t.rate_id,
                    t.checkin_time, t.check_in_datetime,
                    t.checkout_time, t.checkout_datetime, t.estimated_checkout,
                    t.reservation_id, t.user_pass_id, t.permit_request_id,
                    t.affiliate_business_id,
                    t.deleted_at, t.cancelled_at,
                    t.is_checkout, t.is_checkin,
                    t.length, t.ticket_number,
                    t.license_plate, t.promocode,
                    t.is_extended, t.is_overstay, t.session_id,
                    t.is_offline_payment, t.event_user_id,
                    t.policy_id
                FROM tickets t
                WHERE t.id = %s
            """, (tid,))
            ticket = bc.fetchone()

            if not ticket:
                logger.warning(f"[SKIP] ticket_id={tid} not found in source")
                continue

            ticket_number     = ticket["ticket_number"]
            attendant_user_id = ticket.get("event_user_id")

            if not ticket_number:
                logger.warning(f"[SKIP] ticket_number is NULL for ticket_id={tid}")
                continue

            # ── Resolve reserv_permit_pass_flag + FK keys ─────────────────────
            (flag,
             reservation_key,
             event_key,
             permit_subscription_key,
             pass_subscription_key) = resolve_flags_and_fks(gc, ticket)

            entitlement_flag        = 1 if ticket["permit_request_id"] else 0
            validation_applied_flag = 1 if ticket["affiliate_business_id"] else 0
            session_status          = resolve_session_status(ticket)

            logger.info(f"[STATUS] ticket_id={tid} status={session_status} rpf={flag}")

            checkin_dt  = ticket["checkin_time"] or ticket["check_in_datetime"]
            checkout_dt = (ticket["checkout_time"]
                           or ticket["checkout_datetime"]
                           or ticket["estimated_checkout"])

            facility_key        = lookup_facility_key(gc, ticket["facility_id"])
            vehicle_key         = lookup_vehicle_key(gc, ticket["vehicle_id"])
            parker_key          = lookup_parker_key(gc, ticket["user_id"])
            partner_account_key = lookup_partner_account_key(gc, ticket["partner_id"])
            rate_plan_key       = lookup_rate_plan_key(gc, ticket["rate_id"])
            entry_date_key      = lookup_date_key(gc, checkin_dt)
            exit_date_key       = lookup_date_key(gc, checkout_dt)
            entry_time_key      = lookup_time_key(gc, checkin_dt)
            exit_time_key       = lookup_time_key(gc, checkout_dt)
            promo_code_key      = lookup_promo_key(gc, ticket["promocode"])
            duration_hours      = float(ticket["length"] or 0)

            # CHANGED: resolve policy_key from tickets.policy_id via dim_policy
            policy_key = lookup_policy_key(gc, ticket.get("policy_id"))

            # NEW: check if this ticket_number has a validation_refund row
            vr_flag = lookup_validation_refund_flag(bc, ticket_number)

            # ── Upsert BASE ticket  (extension_overstay_flag = 0) ─────────────
            # CHANGED: policy_key column added to INSERT and ON DUPLICATE KEY UPDATE
            gc.execute(f"""
                INSERT INTO {GOLD_DB}.fact_parking_session (
                    source_id,
                    extension_overstay_flag,
                    facility_key, vehicle_key, parker_key, partner_account_key,
                    rate_plan_key,
                    entry_date_key, exit_date_key, entry_time_key, exit_time_key,
                    promo_code_key, duration_hours,
                    reservation_key, event_key,
                    permit_subscription_key, pass_subscription_key,
                    reserv_permit_pass_flag,
                    entitlement_flag, validation_applied_flag,
                    session_status,
                    session_source_type_key, session_quality_score,
                    ticket_number,
                    lpr_entry_event_id, lpr_exit_event_id, session_build_version,
                    license_plate, validation_code,
                    attendant_user_id,
                    policy_key,
                    validation_refund_flag,
                    created_at
                ) VALUES (
                    %s, 0,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s,
                    NULL, NULL,
                    %s,
                    NULL, NULL, NULL,
                    %s, %s,
                    %s,
                    %s,
                    %s,
                    NOW()
                )
                ON DUPLICATE KEY UPDATE
                    facility_key            = VALUES(facility_key),
                    vehicle_key             = VALUES(vehicle_key),
                    parker_key              = VALUES(parker_key),
                    partner_account_key     = VALUES(partner_account_key),
                    rate_plan_key           = VALUES(rate_plan_key),
                    entry_date_key          = VALUES(entry_date_key),
                    exit_date_key           = VALUES(exit_date_key),
                    entry_time_key          = VALUES(entry_time_key),
                    exit_time_key           = VALUES(exit_time_key),
                    promo_code_key          = VALUES(promo_code_key),
                    duration_hours          = VALUES(duration_hours),
                    reservation_key         = VALUES(reservation_key),
                    event_key               = VALUES(event_key),
                    permit_subscription_key = VALUES(permit_subscription_key),
                    pass_subscription_key   = VALUES(pass_subscription_key),
                    reserv_permit_pass_flag = VALUES(reserv_permit_pass_flag),
                    entitlement_flag        = VALUES(entitlement_flag),
                    validation_applied_flag = VALUES(validation_applied_flag),
                    session_status          = VALUES(session_status),
                    ticket_number           = VALUES(ticket_number),
                    license_plate           = VALUES(license_plate),
                    validation_code         = VALUES(validation_code),
                    attendant_user_id       = VALUES(attendant_user_id),
                    policy_key              = COALESCE(VALUES(policy_key), policy_key),
                    validation_refund_flag  = GREATEST(VALUES(validation_refund_flag), validation_refund_flag)
            """, (
                tid,
                facility_key, vehicle_key, parker_key, partner_account_key,
                rate_plan_key,
                entry_date_key, exit_date_key, entry_time_key, exit_time_key,
                promo_code_key, duration_hours,
                reservation_key, event_key,
                permit_subscription_key, pass_subscription_key,
                flag,
                entitlement_flag, validation_applied_flag,
                session_status,
                ticket_number,
                ticket["license_plate"], ticket["promocode"],
                attendant_user_id,
                policy_key,           # CHANGED: mapped from tickets.policy_id
                vr_flag,              # NEW: validation_refund_flag
            ))
            logger.info(f"[TICKET] Upserted rows={gc.rowcount} source_id={tid} flag=0 "
                        f"policy_key={policy_key}")

            upsert_payment_ticket(bc, gc, logger, tid,
                                  reservation_key, permit_subscription_key, pass_subscription_key)

            # ── EXTENSIONS  (extension_overstay_flag = 2) ─────────────────────
            # policy_key is NOT included — always NULL for extensions.
            bc.execute("""
                SELECT id, facility_id, partner_id, length,
                       checkin_time, checkout_time, ticket_number
                FROM ticket_extends
                WHERE ticket_id   = %s
                  AND deleted_at IS NULL
                ORDER BY id
            """, (tid,))
            extend_rows = bc.fetchall()

            if extend_rows:
                logger.info(f"[EXTEND] Found {len(extend_rows)} extension(s) for ticket_id={tid}")

            for ext in extend_rows:
                ext_d = ext if isinstance(ext, dict) else dict(zip(
                    ["id", "facility_id", "partner_id", "length",
                     "checkin_time", "checkout_time", "ticket_number"], ext))

                extend_id          = int(ext_d["id"])
                ext_duration_hours = float(ext_d.get("length") or 0)
                ext_facility_key   = lookup_facility_key(gc, ext_d["facility_id"])
                ext_partner_key    = lookup_partner_account_key(
                    gc, ext_d.get("partner_id") or ticket["partner_id"])
                ext_entry_date_key = lookup_date_key(gc, ext_d["checkin_time"])
                ext_exit_date_key  = lookup_date_key(gc, ext_d["checkout_time"])
                ext_entry_time_key = lookup_time_key(gc, ext_d["checkin_time"])
                ext_exit_time_key  = lookup_time_key(gc, ext_d["checkout_time"])
                ext_session_status = "closed" if ext_d.get("checkout_time") else "open"
                ext_ticket_number  = ext_d.get("ticket_number") or ticket_number

                gc.execute(f"""
                    INSERT INTO {GOLD_DB}.fact_parking_session (
                        source_id,
                        extension_overstay_flag,
                        facility_key, vehicle_key, parker_key, partner_account_key,
                        rate_plan_key,
                        entry_date_key, exit_date_key, entry_time_key, exit_time_key,
                        promo_code_key, duration_hours,
                        reservation_key, event_key,
                        permit_subscription_key, pass_subscription_key,
                        reserv_permit_pass_flag,
                        entitlement_flag, validation_applied_flag,
                        session_status,
                        session_source_type_key, session_quality_score,
                        ticket_number,
                        lpr_entry_event_id, lpr_exit_event_id, session_build_version,
                        license_plate, validation_code,
                        attendant_user_id,
                        validation_refund_flag,
                        created_at
                    ) VALUES (
                        %s, 2,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s,
                        NULL, NULL,
                        %s,
                        NULL, NULL, NULL,
                        %s, NULL,
                        NULL,
                        %s,
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        facility_key            = VALUES(facility_key),
                        vehicle_key             = VALUES(vehicle_key),
                        parker_key              = VALUES(parker_key),
                        partner_account_key     = VALUES(partner_account_key),
                        rate_plan_key           = VALUES(rate_plan_key),
                        entry_date_key          = VALUES(entry_date_key),
                        exit_date_key           = VALUES(exit_date_key),
                        entry_time_key          = VALUES(entry_time_key),
                        exit_time_key           = VALUES(exit_time_key),
                        promo_code_key          = VALUES(promo_code_key),
                        duration_hours          = VALUES(duration_hours),
                        reservation_key         = VALUES(reservation_key),
                        event_key               = VALUES(event_key),
                        permit_subscription_key = VALUES(permit_subscription_key),
                        pass_subscription_key   = VALUES(pass_subscription_key),
                        reserv_permit_pass_flag = VALUES(reserv_permit_pass_flag),
                        entitlement_flag        = VALUES(entitlement_flag),
                        validation_applied_flag = VALUES(validation_applied_flag),
                        session_status          = VALUES(session_status),
                        ticket_number           = VALUES(ticket_number),
                        license_plate           = VALUES(license_plate),
                        validation_refund_flag  = GREATEST(VALUES(validation_refund_flag), validation_refund_flag)
                """, (
                    extend_id,
                    ext_facility_key, vehicle_key, parker_key, ext_partner_key,
                    rate_plan_key,
                    ext_entry_date_key, ext_exit_date_key,
                    ext_entry_time_key, ext_exit_time_key,
                    promo_code_key, ext_duration_hours,
                    reservation_key, event_key,
                    permit_subscription_key, pass_subscription_key,
                    flag,
                    entitlement_flag, validation_applied_flag,
                    ext_session_status,
                    ext_ticket_number,
                    ticket["license_plate"],
                    vr_flag,              # NEW: same flag as parent ticket
                ))
                logger.info(f"[EXTEND] Upserted rows={gc.rowcount} "
                            f"source_id={extend_id} flag=2")

                upsert_payment_extend(bc, gc, logger, extend_id,
                                      reservation_key, permit_subscription_key,
                                      pass_subscription_key)

            # ── OVERSTAYS  (extension_overstay_flag = 1) ──────────────────────
            # policy_key is NOT included — always NULL for overstays.
            bc.execute("""
                SELECT id, facility_id, partner_id, user_id, length,
                       check_in_datetime, checkout_datetime, estimated_checkout,
                       rate_id, reservation_id
                FROM overstay_tickets
                WHERE ticket_number = %s
                ORDER BY id
            """, (ticket_number,))
            overstay_rows = bc.fetchall()

            if overstay_rows:
                logger.info(f"[OVERSTAY] Found {len(overstay_rows)} overstay(s) "
                            f"for ticket_number={ticket_number}")

            for ost in overstay_rows:
                ost_d = ost if isinstance(ost, dict) else dict(zip(
                    ["id", "facility_id", "partner_id", "user_id", "length",
                     "check_in_datetime", "checkout_datetime", "estimated_checkout",
                     "rate_id", "reservation_id"], ost))

                overstay_id        = int(ost_d["id"])
                ost_duration_hours = float(ost_d.get("length") or 0)
                ost_rsv_id         = ost_d.get("reservation_id")
                ost_resv_key       = lookup_reservation_key(gc, ost_rsv_id) if ost_rsv_id else None
                ost_evt_key        = lookup_event_key_by_reservation(gc, ost_rsv_id) if ost_rsv_id else None
                ost_flag           = 1 if ost_rsv_id else 0
                ost_facility_key   = lookup_facility_key(gc, ost_d["facility_id"])
                ost_parker_key     = lookup_parker_key(gc, ost_d["user_id"])
                ost_partner_key    = lookup_partner_account_key(
                    gc, ost_d.get("partner_id") or ticket["partner_id"])
                ost_rate_plan_key  = lookup_rate_plan_key(gc, ost_d["rate_id"])
                ost_checkin_dt     = ost_d.get("check_in_datetime")
                ost_checkout_dt    = ost_d.get("checkout_datetime") or ost_d.get("estimated_checkout")
                ost_entry_date_key = lookup_date_key(gc, ost_checkin_dt)
                ost_exit_date_key  = lookup_date_key(gc, ost_checkout_dt)
                ost_entry_time_key = lookup_time_key(gc, ost_checkin_dt)
                ost_exit_time_key  = lookup_time_key(gc, ost_checkout_dt)
                ost_session_status = "closed" if ost_checkout_dt else "open"

                gc.execute(f"""
                    INSERT INTO {GOLD_DB}.fact_parking_session (
                        source_id,
                        extension_overstay_flag,
                        facility_key, vehicle_key, parker_key, partner_account_key,
                        rate_plan_key,
                        entry_date_key, exit_date_key, entry_time_key, exit_time_key,
                        promo_code_key, duration_hours,
                        reservation_key, event_key,
                        permit_subscription_key, pass_subscription_key,
                        reserv_permit_pass_flag,
                        entitlement_flag, validation_applied_flag,
                        session_status,
                        session_source_type_key, session_quality_score,
                        ticket_number,
                        lpr_entry_event_id, lpr_exit_event_id, session_build_version,
                        license_plate, validation_code,
                        attendant_user_id,
                        validation_refund_flag,
                        created_at
                    ) VALUES (
                        %s, 1,
                        %s, NULL, %s, %s, %s,
                        %s, %s, %s, %s,
                        NULL, %s,
                        %s, %s,
                        NULL, NULL,
                        %s,
                        0, 0,
                        %s,
                        NULL, NULL,
                        %s,
                        NULL, NULL, NULL,
                        %s, NULL,
                        NULL,
                        %s,
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        facility_key            = VALUES(facility_key),
                        parker_key              = VALUES(parker_key),
                        partner_account_key     = VALUES(partner_account_key),
                        rate_plan_key           = VALUES(rate_plan_key),
                        entry_date_key          = VALUES(entry_date_key),
                        exit_date_key           = VALUES(exit_date_key),
                        entry_time_key          = VALUES(entry_time_key),
                        exit_time_key           = VALUES(exit_time_key),
                        duration_hours          = VALUES(duration_hours),
                        reservation_key         = VALUES(reservation_key),
                        event_key               = VALUES(event_key),
                        reserv_permit_pass_flag = VALUES(reserv_permit_pass_flag),
                        session_status          = VALUES(session_status),
                        ticket_number           = VALUES(ticket_number),
                        license_plate           = VALUES(license_plate),
                        validation_refund_flag  = GREATEST(VALUES(validation_refund_flag), validation_refund_flag)
                """, (
                    overstay_id,
                    ost_facility_key, ost_parker_key, ost_partner_key,
                    ost_rate_plan_key,
                    ost_entry_date_key, ost_exit_date_key,
                    ost_entry_time_key, ost_exit_time_key,
                    ost_duration_hours,
                    ost_resv_key, ost_evt_key,
                    ost_flag,
                    ost_session_status,
                    ticket_number,
                    ticket["license_plate"],
                    vr_flag,              # NEW: same flag as parent ticket
                ))
                logger.info(f"[OVERSTAY] Upserted rows={gc.rowcount} "
                            f"source_id={overstay_id} flag=1")

                upsert_payment_overstay(bc, gc, logger, overstay_id)

            gold_conn.commit()
            logger.info(f"[DONE] ticket_id={tid} committed")

        logger.info("[SUCCESS] All records processed")

    except Exception as e:
        logger.error(f"[ERROR] {str(e)}", exc_info=True)
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
    dag_id="gold_upsert_fact_parking_session",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "fact"],
) as dag:

    PythonOperator(
        task_id="upsert_fact_parking_session",
        python_callable=upsert_fact_parking_session,
    )