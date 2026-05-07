import json
import logging
import os
import signal
import sys
import time
import threading
from collections import defaultdict

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ─────────────────────────────────────────
# STRUCTURED LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("cdc-listener")


# ─────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────
KAFKA_SERVER      = os.getenv("KAFKA_SERVER")
AIRFLOW_URL       = os.getenv("AIRFLOW_URL")
AIRFLOW_USER      = os.getenv("AIRFLOW_USER")
AIRFLOW_PASSWORD  = os.getenv("AIRFLOW_PASSWORD")

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", 1))
AIRFLOW_TIMEOUT   = int(os.getenv("AIRFLOW_TIMEOUT", 10))
AIRFLOW_RETRIES   = int(os.getenv("AIRFLOW_RETRIES", 3))
AIRFLOW_RETRY_DELAY = int(os.getenv("AIRFLOW_RETRY_DELAY", 2))  # seconds, doubles each retry

# ─────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ─────────────────────────────────────────
shutdown_event = threading.Event()

def handle_shutdown(signum, frame):
    log.info(f"🛑 Received signal {signum} — initiating graceful shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT,  handle_shutdown)


# ─────────────────────────────────────────
# DEAD LETTER LOG
# Messages that fail after all retries are
# written here so they can be replayed later.
# ─────────────────────────────────────────
DEAD_LETTER_FILE = os.getenv("DEAD_LETTER_FILE", "/tmp/cdc_dead_letter.jsonl")

def write_dead_letter(dag_id, key, ids, reason):
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dag_id":    dag_id,
        "key":       key,
        "ids":       list(ids),
        "reason":    reason,
    }
    try:
        with open(DEAD_LETTER_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
        log.warning(f"📋 Dead letter written for {dag_id} | ids={list(ids)}")
    except Exception as e:
        log.error(f"❌ Failed to write dead letter: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE CONFIG
#
# Each Kafka topic maps to one or more DAG triggers.
# Every entry is verified against the actual DAG source code (dags_updated).
#
# Key:      conf key sent to the DAG (must match context["dag_run"].conf.get(...))
# id_field: column in the CDC record whose value is sent as the ID
#
# Where the id_field cannot perfectly resolve the DAG's primary key (e.g. a
# lookup table changing that is 2+ hops away from the fact's key), the mapping
# is marked "# ⚠️ approximate" with an explanation.  These entries still fire
# the DAG so a refresh happens; the DAG's WHERE clause will simply find no row
# for a non-matching ID and skip gracefully.
# ═══════════════════════════════════════════════════════════════════════════
PIPELINE_CONFIG = {

    # ═══════════════════════════════════════════════════════════
    # DIM FACILITY  (SCD2)                 d_1_dim_facility.py
    # Conf key : facility_ids
    # Primary  : FROM facilities f WHERE f.id = %s
    # Joins    : facility_types (f.facility_type_id = ft.id)
    #            user_facilities / users  (city, state, country)
    #            role_user / roles        (operator_id)
    #            hours_of_operation       (open/close time)
    #            neighborhoods            (lat/lon; f.neighborhood_id = n.id)
    #
    # Also triggers dim_event because facilities.owner_id = events.partner_id
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.facilities": [
        {"dag_id": "gold_upsert_dim_facility",  "key": "facility_ids",  "id_field": "id"},
        {"dag_id": "gold_upsert_dim_event",     "key": "event_ids",     "id_field": "owner_id"},
    ],
    "mysqlserver1.inventory_modules.facility_types": [
        # Joined via f.facility_type_id = ft.id.
        # ⚠️ approximate: passes facility_type.id as facility_id; the DAG's WHERE
        #    clause will skip IDs that don't exist in facilities.
        {"dag_id": "gold_upsert_dim_facility",  "key": "facility_ids",  "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.hours_of_operation": [
        # hours_of_operation.facility_id is a direct FK → exact match ✓
        {"dag_id": "gold_upsert_dim_facility",  "key": "facility_ids",  "id_field": "facility_id"},
    ],
    "mysqlserver1.inventory_modules.neighborhoods": [
        # Joined via f.neighborhood_id = n.id.
        # ⚠️ approximate: neighborhoods has no facility_id column; passes n.id as
        #    facility_id.  Use id_field "id" (NOT "facility_id" — that column
        #    does not exist in the neighborhoods table).
        {"dag_id": "gold_upsert_dim_facility",  "key": "facility_ids",  "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.user_facilities": [
        # dim_facility joins user_facilities to resolve city/state/country from users.
        # user_facilities.facility_id is a direct FK → exact match ✓
        {"dag_id": "gold_upsert_dim_facility",  "key": "facility_ids",  "id_field": "facility_id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM PARTNER ACCOUNT  (SCD2)    d_2_dim_partner_account.py
    # Conf key : account_ids
    # Primary  : FROM users u WHERE u.id = %s
    #
    # DIM PARKER  (non-SCD2)                  d_5_dim_parker.py
    # Conf key : customer_ids
    # Primary  : FROM users u WHERE u.id = %s
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.users": [
        {"dag_id": "gold_upsert_dim_partner_account",  "key": "account_ids",   "id_field": "id"},
        {"dag_id": "gold_upsert_dim_parker",            "key": "customer_ids",  "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM DEVICE  (non-SCD2)                  d_3_dim_device.py
    # Conf key : device_ids
    # Primary  : FROM im30_facility_configurations fc WHERE fc.id = %s
    # Joins    : parking_devices      (fc.facility_id = pd.facility_id)
    #            parking_device_types (pd.device_type_id = pdt.id)
    #            mobile_device_version(pd.partner_id = mdv.partner_id)
    #            gates                (pd.gate_id = g.id)
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.im30_facility_configurations": [
        # Direct primary key → exact match ✓
        {"dag_id": "gold_upsert_dim_device",    "key": "device_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.parking_devices": [
        # Joined via fc.facility_id = pd.facility_id.
        # ⚠️ approximate: passes pd.facility_id as device_id; the DAG resolves
        #    the correct fc.id internally when facility_id matches.
        {"dag_id": "gold_upsert_dim_device",    "key": "device_ids",    "id_field": "facility_id"},
    ],
    "mysqlserver1.inventory_modules.parking_device_types": [
        # Joined via pd.device_type_id = pdt.id  (2 hops from fc.id).
        # ⚠️ approximate for dim_device: passes pdt.id as device_id.
        # Direct for dim_source_system: lpr_feeds.camera_type = pdt.id ✓
        {"dag_id": "gold_upsert_dim_device",        "key": "device_ids",        "id_field": "id"},
        {"dag_id": "gold_upsert_dim_source_system", "key": "source_ref_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.mobile_device_version": [
        # Joined via pd.partner_id = mdv.partner_id  (2 hops from fc.id).
        # ⚠️ approximate: passes mdv.id as device_id.
        {"dag_id": "gold_upsert_dim_device",    "key": "device_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.gates": [
        # Joined via pd.gate_id = g.id  (2 hops from fc.id).
        # ⚠️ approximate: passes g.id as device_id.
        {"dag_id": "gold_upsert_dim_device",    "key": "device_ids",    "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM EVENT  (non-SCD2)                  d_15_dim_event.py
    # Conf key : event_ids
    # Primary  : FROM events e WHERE e.id = %s
    # Joins    : event_categories (e.partner_id = ec.partner_id)
    #            dim_event_type   (det.event_code = e.id)
    #            facilities       (e.partner_id = f.owner_id)
    #
    # FACT PARKING EVENT                  f_3_fact_parking_event.py
    # Conf key : event_ids
    # Primary  : FROM events e WHERE e.id = %s
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.events": [
        {"dag_id": "gold_upsert_dim_event",             "key": "event_ids",     "id_field": "id"},
        {"dag_id": "gold_upsert_fact_parking_event",    "key": "event_ids",     "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM PARKING PRODUCT  (non-SCD2)  d_6_dim_parking_product.py
    # Conf key : product_ids
    # Primary  : FROM service_masters sm WHERE sm.id = %s
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.service_masters": [
        {"dag_id": "gold_upsert_dim_parking_product",   "key": "product_ids",   "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM PAYMENT METHOD  (non-SCD2)    d_7_dim_payment_method.py
    # Conf key : processor_ids  (DAG also accepts payment_method_ids as fallback)
    # Flow     : receives processor_id → looks up dim_processor
    #            → upserts all method_types (CARD, Google Pay, Apple Pay, CASH)
    #
    # FIX ✅: key changed from "payment_method_ids" to "processor_ids" — that is
    #          the primary conf key the DAG reads (payment_method_ids is only a
    #          backward-compat fallback).  The DAG does NOT take anet_transactions
    #          or tickets as input; it works entirely off dim_processor rows.
    #          Triggers are sourced from anet_transactions and facility_payment_type
    #          via the dim_processor pipeline (see FACT PAYMENT section below).
    # ═══════════════════════════════════════════════════════════
    # (dim_payment_method is downstream of dim_processor; it is re-triggered
    #  automatically when facility_payment_type changes via anet_transactions topic.
    #  No separate topic entry is needed here.)

    # ═══════════════════════════════════════════════════════════
    # DIM PERMIT PLAN  (SCD2)             d_8_dim_permit_plan.py
    # Conf key : permit_ids
    # Primary  : FROM permit_requests pr WHERE pr.id = %s
    # Joins    : permit_rates            (pr.permit_rate_id = prt.id)
    #            permit_rate_descriptions(prt.permit_rate_description_id = prd.id)
    #            permit_requests_renew_history (pr.id = prrh.permit_request_id)
    #
    # FACT PERMIT SUBSCRIPTION         f_5_fact_permit_subscription.py
    # Conf key : permit_ids
    # Primary  : FROM permit_requests pr WHERE pr.id = %s
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.permit_requests": [
        {"dag_id": "gold_upsert_dim_permit_plan",           "key": "permit_ids",    "id_field": "id"},
        {"dag_id": "gold_upsert_fact_permit_subscription",  "key": "permit_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.permit_rates": [
        # Joined via pr.permit_rate_id = prt.id.
        # ⚠️ approximate: passes prt.id as permit_id; exact resolution requires
        #    a reverse lookup (permit_requests WHERE permit_rate_id = prt.id).
        {"dag_id": "gold_upsert_dim_permit_plan",   "key": "permit_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.permit_rate_descriptions": [
        # Joined via prt.permit_rate_description_id = prd.id  (2 hops from pr.id).
        # ⚠️ approximate: passes prd.id as permit_id.
        {"dag_id": "gold_upsert_dim_permit_plan",   "key": "permit_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.permit_requests_renew_history": [
        # permit_requests_renew_history.permit_request_id = permit_requests.id → exact match ✓
        {"dag_id": "gold_upsert_dim_permit_plan",   "key": "permit_ids",    "id_field": "permit_request_id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM PROCESSOR  (non-SCD2)           d_9_dim_processor.py
    # Conf key : transaction_ids
    # Flow     : receive anet_transactions.id
    #            → SELECT payment_profile_id FROM anet_transactions WHERE id = tid
    #            → INSERT/UPDATE dim_processor FROM facility_payment_type WHERE id = payment_profile_id
    #
    # anet_transactions → dim_processor is consolidated in the FACT PAYMENT section.
    # ═══════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════
    # DIM PROMO CODE  (SCD2)             d_10_dim_promo_code.py
    # Conf key : promo_code_ids
    # Primary  : FROM promo_codes pc WHERE pc.id = %s
    # Joins    : promo_types (pc.promo_type_id = pt.id)
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.promo_codes": [
        {"dag_id": "gold_upsert_dim_promo_code",    "key": "promo_code_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.promo_types": [
        # Joined via pc.promo_type_id = pt.id.
        # ⚠️ approximate: passes pt.id as promo_code_id; exact resolution requires
        #    a reverse lookup (promo_codes WHERE promo_type_id = pt.id).
        {"dag_id": "gold_upsert_dim_promo_code",    "key": "promo_code_ids",    "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM RATEPLAN  (SCD2)               d_11_dim_rateplan.py
    # Conf key : pricing_ids
    # Primary  : FROM rates r WHERE r.id = %s
    #
    # DIM PASS  (SCD2)                   d_16_dim_pass.py
    # Conf key : rate_ids
    # Primary  : FROM rates r WHERE r.id = %s AND r.rate_type_id = 7
    # NOTE ✅  : Both DAGs share the rates topic but use different conf keys.
    #            dim_pass silently skips rows where rate_type_id != 7.
    #
    # FACT RESERVATION                   f_6_fact_reservation.py
    # Conf key : reservation_ids
    # Primary  : FROM reservations r WHERE r.id = %s
    #
    # DIM VEHICLE (from reservations)    d_14_dim_vehicle.py
    # Conf key : reservation_vehicle_ids
    # Field    : reservations.vehicle_id
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.rates": [
        {"dag_id": "gold_upsert_dim_rateplan",  "key": "pricing_ids",   "id_field": "id"},
        # NEW ✅: dim_pass also sources from rates (rate_type_id = 7 filtered inside DAG)
        {"dag_id": "gold_upsert_dim_pass",      "key": "rate_ids",      "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.reservations": [
        # reservations.rate_id → rates.id: pass as pricing_id ✓
        {"dag_id": "gold_upsert_dim_rateplan",      "key": "pricing_ids",               "id_field": "rate_id"},
        # reservations.vehicle_id → dim_vehicle conf key ✓
        {"dag_id": "gold_upsert_dim_vehicle",       "key": "reservation_vehicle_ids",   "id_field": "vehicle_id"},
        # reservations.id → fact_reservation (also upserts fact_payment reservation-grain row) ✓
        {"dag_id": "gold_upsert_fact_reservation",  "key": "reservation_ids",           "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM REASON  (non-SCD2)              d_12_dim_reason.py
    # 4 separate conf keys to avoid cross-table ID collision.
    # Each source table has its own key; the DAG reads all four
    # from conf and processes each independently.
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.ticket_citation_infraction_reasons": [
        {"dag_id": "gold_upsert_dim_reason",    "key": "ticket_reason_ids",     "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.warning_infraction_reasons": [
        {"dag_id": "gold_upsert_dim_reason",    "key": "warning_reason_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.warning_infractions": [
        {"dag_id": "gold_upsert_dim_reason",    "key": "warning_infraction_ids","id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.infraction_reasons": [
        {"dag_id": "gold_upsert_dim_reason",    "key": "infraction_reason_ids", "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM SOURCE SYSTEM  (non-SCD2)    d_13_dim_source_system.py
    # Conf key : source_ref_ids
    # Primary  : FROM lpr_feeds lf WHERE lf.id = %s
    # Joins    : parking_device_types (pdt.id = lf.camera_type)
    #            facilities           (f.id = lf.facility_id)
    #            facility_types       (ft.id = f.facility_type_id)
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.lpr_feeds": [
        # Direct primary key → exact match ✓
        {"dag_id": "gold_upsert_dim_source_system",     "key": "source_ref_ids",        "id_field": "id"},
        # lpr_feeds.ticket_id → fact_parking_session ✓
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "ticket_ids",            "id_field": "ticket_id"},
        # lpr_feeds.id → dim_vehicle (DAG resolves via permit_vehicles join) ✓
        {"dag_id": "gold_upsert_dim_vehicle",           "key": "lpr_feed_vehicle_ids",  "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # DIM VEHICLE  (non-SCD2)            d_14_dim_vehicle.py
    # 5 separate conf keys to avoid cross-table ID collision.
    # ─ ticket_vehicle_ids     : tickets.id (DAG uses as vehicle_id via t.id)
    # ─ reservation_vehicle_ids: reservations.vehicle_id  (from reservations topic)
    # ─ user_pass_vehicle_ids  : user_passes.vehicle_id
    # ─ permit_vehicle_ids     : permit_vehicles.id
    # ─ lpr_feed_vehicle_ids   : lpr_feeds.id             (from lpr_feeds topic)
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.permit_vehicles": [
        {"dag_id": "gold_upsert_dim_vehicle",   "key": "permit_vehicle_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.user_passes": [
        # user_passes.vehicle_id → dim_vehicle conf key ✓
        {"dag_id": "gold_upsert_dim_vehicle",   "key": "user_pass_vehicle_ids", "id_field": "vehicle_id"},
        # NEW ✅: user_passes.id → fact_passes (primary source table for that DAG)
        {"dag_id": "gold_upsert_fact_passes",   "key": "user_pass_ids",         "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.mst_vehicle_types": [
        # Joined via pv.vehicle_type_id = mvt.id  (1 hop from permit_vehicles).
        # ⚠️ approximate: passes mvt.id as permit_vehicle_id.
        {"dag_id": "gold_upsert_dim_vehicle",   "key": "permit_vehicle_ids",    "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # FACT PARKING SESSION                f_1_fact_parking_session.py
    # Conf key : ticket_ids
    # Primary  : FROM tickets t WHERE t.id = %s
    # Also processes ticket_extends and overstay_tickets internally
    # (triggered by their own topics below, passing ticket_id FK).
    #
    # FACT VALIDATION REDEMPTION     f_2_fact_validation_redemption.py
    # Conf key : ticket_ids
    # Query    : FROM tickets WHERE t.id IN (ticket_ids)
    #              AND t.promocode IS NOT NULL
    #
    # DIM VEHICLE (from tickets)         d_14_dim_vehicle.py
    # Conf key : ticket_vehicle_ids
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.tickets": [
        {"dag_id": "gold_upsert_fact_parking_session",          "key": "ticket_ids",            "id_field": "id"},
        {"dag_id": "gold_upsert_fact_validation_redemption",    "key": "ticket_ids",            "id_field": "id"},
        {"dag_id": "gold_upsert_dim_vehicle",                   "key": "ticket_vehicle_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.lpr_sessions": [
        # lpr_sessions.ticket_id is a direct FK → tickets.id ✓
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "ticket_ids",    "id_field": "ticket_id"},
    ],
    "mysqlserver1.inventory_modules.ticket_extends": [
        # ticket_extends.ticket_id is a direct FK → tickets.id ✓
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "ticket_ids",    "id_field": "ticket_id"},
    ],
    "mysqlserver1.inventory_modules.overstay_tickets": [
        # overstay_tickets.ticket_id is a direct FK → tickets.id ✓
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "ticket_ids",    "id_field": "ticket_id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # FACT PAYMENT                         f_4_fact_payment.py
    # Conf key : transaction_ids
    # Primary  : FROM anet_transactions at WHERE at.id = %s
    # Joins    : anet_statuses (at.anet_status_id = ast.id)
    #
    # DIM PROCESSOR                        d_9_dim_processor.py
    # Conf key : transaction_ids
    # Flow     : anet_transactions.id → payment_profile_id
    #            → facility_payment_type lookup
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.anet_transactions": [
        # processor: anet_transactions.id used to resolve payment_profile_id ✓
        {"dag_id": "gold_upsert_dim_processor",         "key": "transaction_ids",       "id_field": "id"},
        # parking session: anet_transactions.ticket_id → tickets.id ✓
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "ticket_ids",            "id_field": "ticket_id"},
        # payment fact: anet_transactions.id is the primary key ✓
        {"dag_id": "gold_upsert_fact_payment",          "key": "transaction_ids",       "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.anet_statuses": [
        # Joined via at.anet_status_id = ast.id.
        # ⚠️ approximate: anet_statuses.id ≠ anet_transactions.id; passing
        #    ast.id as transaction_id may not match.  Exact resolution requires
        #    a reverse lookup (anet_transactions WHERE anet_status_id = ast.id).
        {"dag_id": "gold_upsert_fact_payment",  "key": "transaction_ids",   "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # FACT VALIDATION REDEMPTION     f_2_fact_validation_redemption.py
    # Conf key : ticket_ids
    # tickets already handled above.
    # promotions / promotions_days are indirect triggers.
    # ⚠️ approximate: DAG filters WHERE t.id IN (ticket_ids), so
    #   passing promotion.id as ticket_id will simply match nothing;
    #   a correct implementation would require a reverse lookup of
    #   tickets WHERE promocode = promotion.name — not feasible at
    #   listener level without a DB call.
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.promotions": [
        {"dag_id": "gold_upsert_fact_validation_redemption",    "key": "ticket_ids",    "id_field": "id"},
    ],
    "mysqlserver1.inventory_modules.promotions_days": [
        # promotions_days.promotion_id → promotions.id (still approximate as above)
        {"dag_id": "gold_upsert_fact_validation_redemption",    "key": "ticket_ids",    "id_field": "promotion_id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # FACT PAYMENT SWEEP TRANSACTIONS
    #              f_x_fact_payment_sweep_transactions.py
    # Conf key : sweep_ids
    # Primary  : FROM payment_sweep_transactions WHERE id = %s
    #
    # Key resolution chain inside the DAG:
    #   pst.facility_id         → dim_facility.facility_key
    #   pst.partner_id          → dim_partner_account.partner_account_key
    #   pst.transaction_id      → fact_payment.processor_txn_id
    #                           → payment_key / canonical_session_key /
    #                             payment_method_key
    #   pst.transaction_at      → dim_date (start_date_key) + dim_time (start_time_key)
    #   pst.funded_at           → dim_date (end_date_key)   + dim_time (end_time_key)
    #
    # Note: payment_sweep_transactions.transaction_id is UNIQUE in source,
    # so one CDC record = one row in the fact table.
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.payment_sweep_transactions": [
        # Direct primary key → exact match ✓
        {"dag_id": "gold_upsert_fact_payment_sweep_transactions",
         "key":    "sweep_ids",
         "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # VALIDATION REFUNDS → fact_parking_session + fact_payment
    # validation_refunds.reference_key = ticket_number.
    # Sends validation_refund_ids to both DAGs so each can look
    # up the refund record and update the corresponding columns.
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.validation_refunds": [
        # fact_parking_session: sets validation_refund_flag = 1
        # on all rows whose ticket_number = reference_key.
        {"dag_id": "gold_upsert_fact_parking_session",  "key": "validation_refund_ids",  "id_field": "id"},
        # fact_payment: updates validate_refund_amount, vr_anet_trans_id,
        # vr_refund_status on the base-ticket payment row.
        {"dag_id": "gold_upsert_fact_payment",          "key": "validation_refund_ids",  "id_field": "id"},
    ],

    # ═══════════════════════════════════════════════════════════
    # FACT RESERVATION                   f_6_fact_reservation.py
    # thirdparty_integrations.name is resolved live inside the DAG
    # (COALESCE(thirdparty_integrations.name, r.booking_source)).
    # A change there must re-trigger fact_reservation for any
    # reservation that references that integration.
    # ⚠️ approximate: passes thirdparty_integrations.id as reservation_id;
    #    the DAG's WHERE r.id = %s will find no row — a correct mapping
    #    would require a reverse lookup of reservations WHERE
    #    thirdparty_integration_id = ti.id, which is not feasible at
    #    listener level without a DB call.  The DAG skips gracefully on
    #    no-match; this entry ensures at least a refresh attempt fires.
    # NEW ✅: added to keep booking_source in fact_reservation consistent.
    # ═══════════════════════════════════════════════════════════
    "mysqlserver1.inventory_modules.thirdparty_integrations": [
        {"dag_id": "gold_upsert_fact_reservation",  "key": "reservation_ids",   "id_field": "id"},
    ],

}

TOPICS = list(PIPELINE_CONFIG.keys())


# ─────────────────────────────────────────
# DAG TRIGGER WITH RETRY + BACKOFF
# ─────────────────────────────────────────
def trigger_dag(dag_id, key, ids):
    url     = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": {key: list(ids)}}

    for attempt in range(1, AIRFLOW_RETRIES + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=AIRFLOW_TIMEOUT
            )

            if response.status_code in (200, 201):
                log.info(f"✅ Triggered {dag_id} | key={key} | ids={list(ids)} | attempt={attempt}")
                return True

            elif response.status_code == 409:
                # Already running — not an error
                log.warning(f"⚠️ {dag_id} already running (409) — skipping duplicate trigger")
                return True

            else:
                log.warning(
                    f"⚠️ Trigger failed {dag_id} | status={response.status_code} "
                    f"| attempt={attempt}/{AIRFLOW_RETRIES}"
                )

        except requests.exceptions.Timeout:
            log.warning(f"⏱️ Timeout triggering {dag_id} | attempt={attempt}/{AIRFLOW_RETRIES}")

        except requests.exceptions.ConnectionError:
            log.warning(f"🔌 Connection error triggering {dag_id} | attempt={attempt}/{AIRFLOW_RETRIES}")

        except Exception as e:
            log.error(f"❌ Unexpected error triggering {dag_id}: {e} | attempt={attempt}/{AIRFLOW_RETRIES}")

        # Exponential backoff before retry
        if attempt < AIRFLOW_RETRIES:
            delay = AIRFLOW_RETRY_DELAY * (2 ** (attempt - 1))
            log.info(f"⏳ Retrying in {delay}s...")
            time.sleep(delay)

    # All retries exhausted
    log.error(f"💀 All {AIRFLOW_RETRIES} retries exhausted for {dag_id} | ids={list(ids)}")
    write_dead_letter(dag_id, key, ids, reason=f"All {AIRFLOW_RETRIES} retries failed")
    return False


# ─────────────────────────────────────────
# KAFKA CONNECTION WITH RETRY
# ─────────────────────────────────────────
def create_consumer():
    while not shutdown_event.is_set():
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="latest",
                enable_auto_commit=False,   # ✅ Manual commit — only after successful trigger
                group_id="cdc-listener-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            log.info("✅ Connected to Kafka!")
            return consumer
        except KafkaError as e:
            log.warning(f"Kafka not ready: {e} — retrying in 5s...")
            time.sleep(5)
    return None


# ─────────────────────────────────────────
# BATCH STORAGE
# Keyed by (dag_id, key) — each pair has
# its own independent batch and counter.
# ─────────────────────────────────────────
batches  = defaultdict(set)
# Track the last Kafka message per batch key
# so we can commit offset only after trigger success
last_msg = {}

for topic, targets in PIPELINE_CONFIG.items():
    for cfg in targets:
        batch_key = (cfg["dag_id"], cfg["key"])
        batches[batch_key]  # initialise


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
log.info("🚀 CDC LISTENER STARTING")
log.info(f"Kafka:   {KAFKA_SERVER}")
log.info(f"Airflow: {AIRFLOW_URL}")
log.info(f"Topics:  {len(TOPICS)}")
log.info(f"Batch size: {BATCH_SIZE}")

consumer = create_consumer()
if consumer is None:
    log.error("❌ Could not connect to Kafka — exiting")
    sys.exit(1)


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────
log.info("▶️  Listening for CDC events...")

try:
    for msg in consumer:

        if shutdown_event.is_set():
            log.info("🛑 Shutdown signal received — stopping consumer loop")
            break

        try:
            topic  = msg.topic
            record = msg.value

            if topic not in PIPELINE_CONFIG or not record:
                consumer.commit()
                continue

            targets = PIPELINE_CONFIG[topic]

            for config in targets:
                dag_id    = config["dag_id"]
                key       = config["key"]
                id_field  = config["id_field"]
                batch_key = (dag_id, key)

                record_id = record.get(id_field)

                if not record_id:
                    log.warning(f"⚠️ id_field '{id_field}' missing in record for {dag_id} | topic={topic}")
                    continue

                batches[batch_key].add(record_id)
                last_msg[batch_key] = msg

                log.info(
                    f"📥 Received | topic={topic} | dag={dag_id} | "
                    f"key={key} | id={record_id} | "
                    f"batch_size={len(batches[batch_key])}"
                )

                if len(batches[batch_key]) >= BATCH_SIZE:
                    success = trigger_dag(dag_id, key, batches[batch_key])

                    if success:
                        # ✅ Only commit Kafka offset after confirmed trigger
                        consumer.commit()
                        log.info(f"✅ Kafka offset committed for {dag_id}")
                    else:
                        log.error(f"❌ Trigger failed — offset NOT committed, message will be reprocessed")

                    batches[batch_key].clear()
                    last_msg.pop(batch_key, None)

        except json.JSONDecodeError as e:
            log.error(f"❌ JSON decode error on topic={msg.topic}: {e}")
            consumer.commit()  # skip bad message

        except Exception as e:
            log.error(f"❌ Unexpected error processing message: {e}", exc_info=True)

except Exception as e:
    log.error(f"❌ Fatal consumer error: {e}", exc_info=True)

finally:
    log.info("🔒 Closing Kafka consumer...")
    try:
        consumer.close()
    except Exception:
        pass
    log.info("👋 CDC Listener stopped.")