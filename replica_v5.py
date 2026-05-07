"""
Enterprise-grade replica-to-target ETL.
Loads ALL dimension and fact tables in batches with full audit/control support.

CHANGES vs previous version:
  All fact-table source queries: removed deleted_at IS NULL filters so that every
  row present in the source DB is replicated. Soft-deleted rows still map to
  status='void'/'deleted'/'cancelled'/'expired' via the del_at/cancelled_at values.

  fact_parking_session:
    - validation_refund_flag TINYINT(1) NOT NULL DEFAULT 0  [NEW]
      Set to 1 when validation_refunds.reference_key matches the row's ticket_number
      (checked for all three session sources: tickets, overstay_tickets, ticket_extends).

  fact_payment:
    - validate_refund_amount  DECIMAL(10,2) NULL  [NEW]
      Mapped from validation_refunds.total via reference_key = ticket_number.
    - vr_anet_trans_id  INT NULL  [NEW]
      Mapped from validation_refunds.anet_transaction_id (same join).
    - vr_refund_status  ENUM('PENDING','FAILED','REFUNDED') NULL  [NEW]
      Mapped from validation_refunds.transaction_status (same join).
    All three columns are NULL for non-ticket sources (reservations, permits,
    passes, overstay_tickets, ticket_extends).
"""
from __future__ import annotations

import os
import sys
import time
import logging
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Callable, Iterable, Optional
import mysql.connector
from mysql.connector import Error

# # ---------------------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------------------
# SOURCE_DB = os.getenv("SOURCE_DB", "inventory_modules")
# TARGET_DB = os.getenv("TARGET_DB", "pe_reporting_slave_v7")
# SOURCE_CONFIG = {
#     "host":               os.getenv("SOURCE_HOST", "127.0.0.1"),
#     "port":               int(os.getenv("SOURCE_PORT", "3306")),
#     "user":               os.getenv("SOURCE_USER", "root"),
#     "password":           os.getenv("SOURCE_PASSWORD", "root"),
#     "database":           SOURCE_DB,
#     "autocommit":         False,
#     "connection_timeout": 600,
# }
# TARGET_CONFIG = {
#     "host":               os.getenv("TARGET_HOST", "127.0.0.1"),
#     "port":               int(os.getenv("TARGET_PORT", "3306")),
#     "user":               os.getenv("TARGET_USER", "root"),
#     "password":           os.getenv("TARGET_PASSWORD", "root"),
#     "database":           TARGET_DB,
#     "autocommit":         False,
#     "connection_timeout": 600,
# }
# BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "2000"))
# RETRY_COUNT             = int(os.getenv("RETRY_COUNT", "3"))
# RETRY_SLEEP_SECONDS     = int(os.getenv("RETRY_SLEEP_SECONDS", "5"))
# FULL_RELOAD             = os.getenv("FULL_RELOAD", "true").lower() in {"1", "true", "yes", "y"}
# REPLICA_MAX_LAG_SECONDS = int(os.getenv("REPLICA_MAX_LAG_SECONDS", "30"))
# LOCK_NAME               = os.getenv("ETL_LOCK_NAME", "enterprise_replica_etl_lock")
# READ_ONLY_SOURCE        = False



# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SOURCE_DB = os.getenv("SOURCE_DB", "inventory_modules")
TARGET_DB = os.getenv("TARGET_DB", "pe_reporting_gold_v5")

SOURCE_CONFIG = {
    "host":               os.getenv("SOURCE_HOST", "34.139.219.62"), 
    "port":               int(os.getenv("SOURCE_PORT", "3306")),
    "user":               os.getenv("SOURCE_USER", "root"),
    "password":           os.getenv("SOURCE_PASSWORD", "StrongRootPass@123"), 
    "database":           SOURCE_DB,
    "autocommit":         False,
    "connection_timeout": 600,
}

TARGET_CONFIG = {
    "host":               os.getenv("TARGET_HOST", "localhost"),
    "port":               int(os.getenv("TARGET_PORT", "3306")),
    "user":               os.getenv("TARGET_USER", "root"),
    "password":           os.getenv("TARGET_PASSWORD", "K@z@R0ck5"),
    "database":           TARGET_DB,
    "autocommit":         False,
    "connection_timeout": 600,
}

BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "2000"))
RETRY_COUNT             = int(os.getenv("RETRY_COUNT", "3"))
RETRY_SLEEP_SECONDS     = int(os.getenv("RETRY_SLEEP_SECONDS", "5"))
FULL_RELOAD             = os.getenv("FULL_RELOAD", "true").lower() in {"1", "true", "yes", "y"}
REPLICA_MAX_LAG_SECONDS = int(os.getenv("REPLICA_MAX_LAG_SECONDS", "30"))
LOCK_NAME               = os.getenv("ETL_LOCK_NAME", "enterprise_replica_etl_lock")
READ_ONLY_SOURCE        = False   
# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enterprise_etl")

# ---------------------------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------------------------
def connect(cfg: dict) -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(**cfg)

def retry_sql(fn: Callable[[], Any], label: str = "SQL") -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            return fn()
        except Error as exc:
            last_exc = exc
            if attempt == RETRY_COUNT:
                raise
            wait = RETRY_SLEEP_SECONDS * attempt
            log.warning("%s attempt %s/%s failed: %s – retry in %ss",
                        label, attempt, RETRY_COUNT, exc, wait)
            time.sleep(wait)
    if last_exc:
        raise last_exc

def safe_decimal(value, precision: int = 10, scale: int = 2) -> float:
    if value in (None, "", "NULL"):
        return 0.0
    try:
        val = float(str(value).replace("%", ""))
        max_val = (10 ** (precision - scale)) - (10 ** -scale)
        return round(max(min(val, max_val), -max_val), scale)
    except Exception:
        log.warning("Invalid decimal value: %s → 0", value)
        return 0.0

def safe_dt(value) -> Optional[datetime]:
    if value in (None, "", "0000-00-00 00:00:00", "0000-00-00"):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%Y %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    log.warning("Cannot parse date: %s", value)
    return None

def dk(dt_value) -> Optional[int]:
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        dt_value = dt_value.date()
    elif not isinstance(dt_value, date):
        return None
    return int(dt_value.strftime("%Y%m%d"))

def tk(dt_value) -> Optional[int]:
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        t = dt_value.time()
    elif isinstance(dt_value, dtime):
        t = dt_value
    else:
        return None
    return t.hour * 10000 + t.minute * 100 + t.second

def fmt_dur(sec: float) -> str:
    if sec < 60:
        return f"{sec:.1f}s"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

def chunked(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

# ---------------------------------------------------------------------------
# TABLE / COLUMN EXISTENCE CHECKS
# ---------------------------------------------------------------------------
def table_exists(tcur, db: str, table_name: str) -> bool:
    tcur.execute(
        "SELECT COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (db, table_name),
    )
    return int(tcur.fetchone()[0]) > 0

def col_exists(cur, db: str, table: str, col: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
        (db, table, col),
    )
    return int(cur.fetchone()[0]) > 0

# ---------------------------------------------------------------------------
# ETL CONTROL / AUDIT
# ---------------------------------------------------------------------------
def ensure_control_tables(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.etl_control (
            load_name          VARCHAR(120) PRIMARY KEY,
            last_pk            BIGINT       NOT NULL DEFAULT 0,
            last_status        VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
            last_started_at    DATETIME     NULL,
            last_finished_at   DATETIME     NULL,
            last_rows_read     BIGINT       NOT NULL DEFAULT 0,
            last_rows_inserted BIGINT       NOT NULL DEFAULT 0,
            last_message       VARCHAR(500) NULL,
            updated_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.etl_run_log (
            run_id        BIGINT       AUTO_INCREMENT PRIMARY KEY,
            load_name     VARCHAR(120) NOT NULL,
            started_at    DATETIME     NOT NULL,
            finished_at   DATETIME     NULL,
            status        VARCHAR(20)  NOT NULL,
            rows_read     BIGINT       NOT NULL DEFAULT 0,
            rows_inserted BIGINT       NOT NULL DEFAULT 0,
            last_pk       BIGINT       NOT NULL DEFAULT 0,
            message       VARCHAR(1000) NULL,
            created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_rlog_name    (load_name),
            INDEX idx_rlog_status  (status),
            INDEX idx_rlog_started (started_at)
        )
    """)
    tconn.commit()

def ensure_etl_row(tcur, tconn, load_name: str) -> None:
    tcur.execute(
        f"INSERT INTO {TARGET_DB}.etl_control (load_name) VALUES (%s) "
        f"ON DUPLICATE KEY UPDATE load_name=load_name",
        (load_name,),
    )
    tconn.commit()

def get_watermark(tcur, load_name: str) -> int:
    tcur.execute(f"SELECT last_pk FROM {TARGET_DB}.etl_control WHERE load_name=%s", (load_name,))
    row = tcur.fetchone()
    return int(row[0]) if row else 0

def set_watermark(tcur, tconn, load_name: str, *, last_pk: int,
                  status: str, rows_read: int, rows_inserted: int, message: str = "") -> None:
    tcur.execute(f"""
        INSERT INTO {TARGET_DB}.etl_control
            (load_name,last_pk,last_status,last_started_at,last_finished_at,
             last_rows_read,last_rows_inserted,last_message)
        VALUES (%s,%s,%s,NOW(),NOW(),%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            last_pk=VALUES(last_pk), last_status=VALUES(last_status),
            last_finished_at=VALUES(last_finished_at),
            last_rows_read=VALUES(last_rows_read),
            last_rows_inserted=VALUES(last_rows_inserted),
            last_message=VALUES(last_message)
    """, (load_name, int(last_pk), status, int(rows_read), int(rows_inserted), message[:500]))
    tconn.commit()

def start_run(tcur, tconn, load_name: str) -> int:
    tcur.execute(
        f"INSERT INTO {TARGET_DB}.etl_run_log (load_name,started_at,status) VALUES (%s,NOW(),'RUNNING')",
        (load_name,),
    )
    tconn.commit()
    return int(tcur.lastrowid)

def finish_run(tcur, tconn, run_id: int, *, status: str,
               rows_read: int, rows_inserted: int, last_pk: int, message: str = "") -> None:
    tcur.execute(f"""
        UPDATE {TARGET_DB}.etl_run_log
        SET finished_at=NOW(), status=%s, rows_read=%s, rows_inserted=%s, last_pk=%s, message=%s
        WHERE run_id=%s
    """, (status, int(rows_read), int(rows_inserted), int(last_pk), message[:1000], int(run_id)))
    tconn.commit()

def acquire_lock(tcur, tconn) -> None:
    tcur.execute("SELECT GET_LOCK(%s, 0)", (LOCK_NAME,))
    ok = tcur.fetchone()[0]
    if int(ok) != 1:
        raise RuntimeError(f"Cannot acquire ETL lock '{LOCK_NAME}' – another run is active.")
    tconn.commit()

def release_lock(tcur, tconn) -> None:
    try:
        tcur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
        tconn.commit()
    except Exception:
        pass

# ---------------------------------------------------------------------------
# SOURCE SAFETY
# ---------------------------------------------------------------------------
def get_replica_lag(source_conn) -> Optional[int]:
    cur = source_conn.cursor(dictionary=True)
    try:
        for stmt in ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS"):
            try:
                cur.execute(stmt)
                row = cur.fetchone()
                if not row:
                    continue
                for k in ("Seconds_Behind_Source", "Seconds_Behind_Master"):
                    if k in row and row[k] is not None:
                        return int(row[k])
            except Error:
                continue
    finally:
        cur.close()
    return None

def assert_read_only(source_conn) -> None:
    if not READ_ONLY_SOURCE:
        return
    cur = source_conn.cursor()
    try:
        cur.execute("SELECT @@read_only")
        row = cur.fetchone()
        if int(row[0]) != 1:
            raise RuntimeError("Source is not read_only. Aborting.")
    finally:
        cur.close()

# ---------------------------------------------------------------------------
# SOURCE STREAMING
# ---------------------------------------------------------------------------
def keyset_batches(scur, *, table: str, pk_col: str, cols: str,
                   where: str = "1=1", start_after: int = 0,
                   batch_size: int = BATCH_SIZE):
    last_pk = int(start_after)
    while True:
        scur.execute(
            f"SELECT {cols} FROM {table} WHERE {where} AND {pk_col}>%s ORDER BY {pk_col} LIMIT %s",
            (last_pk, batch_size),
        )
        rows = scur.fetchall()
        if not rows:
            break
        yield rows
        last_pk = int(rows[-1][0])

def fetch_all(cur, sql: str, params: tuple = ()) -> list:
    cur.execute(sql, params)
    return cur.fetchall()

# ---------------------------------------------------------------------------
# TARGET MAP CACHE
# ---------------------------------------------------------------------------
class MapCache:
    def __init__(self, tcur):
        self._cur = tcur
        self._maps: dict[str, dict] = {}

    def load(self, name: str, sql: str) -> dict:
        if name not in self._maps:
            self._cur.execute(sql)
            self._maps[name] = {r[1]: r[0] for r in self._cur.fetchall() if r[1] is not None}
        return self._maps[name]

    def get(self, name: str, key, default=None):
        return self._maps.get(name, {}).get(key, default)

# ---------------------------------------------------------------------------
# DML HELPER
# ---------------------------------------------------------------------------
def bulk_insert(tcur, tconn, sql: str, rows: list) -> int:
    if not rows:
        return 0
    try:
        tcur.executemany(sql, rows)
        tconn.commit()
        return tcur.rowcount or len(rows)
    except Exception:
        try:
            tcur.fetchall()
        except Exception:
            pass
        raise

def truncate(tcur, tconn, table: str) -> None:
    tcur.execute(f"TRUNCATE TABLE {TARGET_DB}.{table}")
    tconn.commit()

# ---------------------------------------------------------------------------
# VALIDATION REFUNDS HELPERS
# ---------------------------------------------------------------------------
# These two helpers load lookup structures from validation_refunds once and
# are consumed by both fact_parking_session and fact_payment loaders.

def load_vr_set(scur) -> set:
    """
    Returns a set of reference_key values (= ticket_number strings) that have
    at least one row in validation_refunds.  Used to set validation_refund_flag.
    """
    log.info("Loading validation_refunds reference_key set …")
    scur.execute(f"""
        SELECT DISTINCT reference_key
        FROM {SOURCE_DB}.validation_refunds
        WHERE reference_key IS NOT NULL
    """)
    return {r[0] for r in scur.fetchall()}


_VR_VALID_STATUSES = {"PENDING", "FAILED", "REFUNDED"}

def load_vr_map(scur) -> dict:
    """
    Returns a dict:  reference_key → (total, anet_transaction_id, transaction_status)

    When a ticket_number has multiple validation_refund rows the one with the
    highest id (latest) is kept, matching the ORDER BY id DESC query below.
    transaction_status is normalised to uppercase and validated against the
    ENUM definition; any value outside ('PENDING','FAILED','REFUNDED') is stored
    as NULL so MySQL never rejects the row.
    """
    log.info("Loading validation_refunds detail map …")
    scur.execute(f"""
        SELECT reference_key, total, anet_transaction_id, transaction_status
        FROM {SOURCE_DB}.validation_refunds
        WHERE reference_key IS NOT NULL
        ORDER BY id DESC
    """)
    vr_map: dict = {}
    for r in scur.fetchall():
        ref_key, total, anet_trans_id, trans_status = r
        if ref_key in vr_map:          # keep latest (first seen because of DESC order)
            continue
        vr_amount = float(total) if total is not None else None
        vr_anet   = int(anet_trans_id) if anet_trans_id is not None else None
        vr_status_raw = str(trans_status).strip().upper() if trans_status is not None else None
        vr_status = vr_status_raw if vr_status_raw in _VR_VALID_STATUSES else None
        vr_map[ref_key] = (vr_amount, vr_anet, vr_status)
    log.info("validation_refunds map: %s unique reference_keys", len(vr_map))
    return vr_map


# ===========================================================================
# DIMENSIONS  (unchanged from previous version)
# ===========================================================================

def ensure_dim_date(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_date (
            date_key     INT PRIMARY KEY,
            full_date    DATE NOT NULL,
            day_of_month TINYINT,
            day_name     VARCHAR(10),
            day_of_week  TINYINT,
            week_of_year TINYINT,
            month_number TINYINT,
            month_name   VARCHAR(15),
            quarter      TINYINT,
            year         SMALLINT,
            is_weekend   TINYINT(1),
            is_holiday   TINYINT(1) DEFAULT 0,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    tconn.commit()

def load_dim_date(tcur, tconn) -> None:
    ensure_dim_date(tcur, tconn)
    truncate(tcur, tconn, "dim_date")
    rows, d, end = [], date(2020, 1, 1), date(2035, 12, 31)
    while d <= end:
        dow = ((d.isoweekday()) % 7) + 1
        rows.append((
            int(d.strftime("%Y%m%d")), d,
            d.day, d.strftime("%A"), dow,
            int(d.strftime("%U")), d.month, d.strftime("%B"),
            (d.month - 1) // 3 + 1, d.year,
            1 if d.weekday() >= 5 else 0, 0,
        ))
        d += timedelta(days=1)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_date
            (date_key,full_date,day_of_month,day_name,day_of_week,week_of_year,
             month_number,month_name,quarter,year,is_weekend,is_holiday)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, chunk)
    log.info("dim_date loaded: %s rows", len(rows))

def ensure_dim_time(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_time (
            time_key    INT PRIMARY KEY,
            full_time   TIME NOT NULL,
            hour        TINYINT,
            minute      TINYINT,
            second      TINYINT,
            am_pm       VARCHAR(2),
            time_bucket VARCHAR(20),
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    tconn.commit()

def load_dim_time(tcur, tconn) -> None:
    ensure_dim_time(tcur, tconn)
    truncate(tcur, tconn, "dim_time")
    rows = []
    for h in range(24):
        for m in range(60):
            for s in range(60):
                bucket = (
                    "Morning"   if 5  <= h <= 11 else
                    "Afternoon" if 12 <= h <= 16 else
                    "Evening"   if 17 <= h <= 20 else
                    "Night"
                )
                rows.append((h * 10000 + m * 100 + s, dtime(h, m, s), h, m, s,
                              "AM" if h < 12 else "PM", bucket))
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_time
            (time_key,full_time,hour,minute,second,am_pm,time_bucket)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, chunk)
    log.info("dim_time loaded: %s rows", len(rows))

def ensure_dim_partner_account(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_partner_account (
            partner_account_key  BIGINT AUTO_INCREMENT PRIMARY KEY,
            account_id_source    INT NOT NULL,
            account_name         VARCHAR(255),
            account_type         VARCHAR(50),
            partner_id           INT,
            country              VARCHAR(100),
            status               VARCHAR(50),
            created_at           DATETIME,
            updated_at           DATETIME,
            effective_start_date DATETIME NOT NULL,
            effective_end_date   DATETIME NOT NULL,
            is_current           TINYINT DEFAULT 1,
            etl_created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            etl_updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            record_hash          CHAR(32),
            INDEX idx_partner_business (account_id_source),
            INDEX idx_partner_current  (account_id_source, is_current)
        )
    """)
    tconn.commit()

def load_dim_partner_account(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_partner_account(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_partner_account")
    import hashlib
    total, last_pk = 0, start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.users", pk_col="id",
            cols="id,name,user_type,is_partner,created_by,country,status,created_at,updated_at",
            where="deleted_at IS NULL AND is_partner = 1",
            start_after=start_after):
        out = []
        for r in rows:
            (uid, name, user_type, is_partner, created_by,
             country, status_raw, created_at_raw, updated_at_raw) = r
            if int(is_partner or 0) == 1 and str(user_type or "") == "3":
                partner_id = int(uid)
            else:
                partner_id = int(created_by) if created_by is not None else int(uid)
            status     = "ACTIVE" if int(status_raw or 0) == 1 else "INACTIVE"
            created_at = safe_dt(created_at_raw)
            updated_at = safe_dt(updated_at_raw)
            hash_src = "|".join([
                str(name or ""), str(user_type or ""), str(partner_id or ""),
                str(country or ""), str(status or ""),
            ])
            record_hash = hashlib.md5(hash_src.encode()).hexdigest()
            out.append((
                int(uid), name, user_type, partner_id, country, status,
                created_at, updated_at,
                datetime.now(), datetime(2038, 1, 19, 3, 14, 7), 1, record_hash,
            ))
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_partner_account
            (account_id_source, account_name, account_type, partner_id, country, status,
             created_at, updated_at, effective_start_date, effective_end_date,
             is_current, record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                account_name=VALUES(account_name), account_type=VALUES(account_type),
                partner_id=VALUES(partner_id), country=VALUES(country),
                status=VALUES(status), updated_at=VALUES(updated_at),
                record_hash=VALUES(record_hash)
        """, out)
        total += len(out); last_pk = int(rows[-1][0])
        log.info("dim_partner_account pk<=%s total=%s", last_pk, total)
    return last_pk

def ensure_dim_parker(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_parker (
            parker_key           BIGINT AUTO_INCREMENT PRIMARY KEY,
            customer_id          BIGINT,
            customer_type        INT,
            signup_channel       VARCHAR(255),
            parker_name          VARCHAR(255),
            loyalty_tier         TINYINT,
            account_status       VARCHAR(50),
            home_city            VARCHAR(255),
            phone_number         VARCHAR(32),
            email                VARCHAR(255),
            effective_start_date TIMESTAMP,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_customer_id (customer_id)
        )
    """)
    tconn.commit()

def load_dim_parker(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_parker(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_parker")
    total, last_pk = 0, start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.users", pk_col="id",
            cols="id,user_type,social_type,user_prefrences,is_loyalty,status,city,created_at,name,phone,email",
            where="deleted_at IS NULL AND COALESCE(is_partner,0)=0 AND user_type=5",
            start_after=start_after):
        out = [(
            r[0], r[1],
            "SOCIAL" if r[2] is not None else "APP" if r[3] is not None else "DIRECT",
            r[8], 1 if int(r[4] or 0) == 1 else 0,
            "ACTIVE" if int(r[5] or 0) == 1 else "INACTIVE",
            r[6], r[9], r[10], safe_dt(r[7]),
        ) for r in rows]
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_parker
            (customer_id,customer_type,signup_channel,parker_name,
             loyalty_tier,account_status,home_city,phone_number,email,effective_start_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                customer_type=VALUES(customer_type), signup_channel=VALUES(signup_channel),
                parker_name=VALUES(parker_name), loyalty_tier=VALUES(loyalty_tier),
                account_status=VALUES(account_status), home_city=VALUES(home_city),
                phone_number=VALUES(phone_number), email=VALUES(email)
        """, out)
        total += len(out); last_pk = int(rows[-1][0])
        log.info("dim_parker pk<=%s total=%s", last_pk, total)
    return last_pk

def ensure_dim_facility(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_facility (
            facility_key            BIGINT AUTO_INCREMENT PRIMARY KEY,
            facility_id             INT NOT NULL,
            facility_name           VARCHAR(255),
            facility_type           VARCHAR(50),
            city                    VARCHAR(255),
            state                   VARCHAR(255),
            country                 VARCHAR(255),
            capacity                INT,
            operator_id             INT,
            garage_code             VARCHAR(50),
            logo                    VARCHAR(255),
            open_time               TIME,
            close_time              TIME,
            latitude                DECIMAL(11,7),
            longitude               DECIMAL(11,7),
            location                VARCHAR(255),
            effective_start_date    DATETIME NOT NULL,
            effective_end_date      DATETIME NOT NULL,
            dw_effective_start_date DATETIME NOT NULL,
            dw_effective_end_date   DATETIME NOT NULL,
            is_current              BOOLEAN NOT NULL DEFAULT TRUE,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            record_hash             CHAR(32),
            INDEX idx_facility_business_key   (facility_id),
            INDEX idx_facility_current_lookup (facility_id, is_current)
        )
    """)
    tconn.commit()

def load_dim_facility(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_facility(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_facility")
    import hashlib
    total, last_pk = 0, start_after
    while True:
        scur.execute(
            f"SELECT id FROM {SOURCE_DB}.facilities WHERE id>%s ORDER BY id LIMIT %s",
            (last_pk, BATCH_SIZE))
        ids = [r[0] for r in scur.fetchall()]
        if not ids:
            break
        lo, hi = int(ids[0]), int(ids[-1])
        scur.execute(f"""
            SELECT f.id,f.full_name,ft.facility_type,g.city,g.state,c.name,
                   CAST(f.capacity AS SIGNED),ru.user_id,f.garage_code,f.logo,
                   MIN(hoo.open_time),MAX(hoo.close_time),
                   g.latitude,g.longitude,f.entrance_location,f.created_at
            FROM {SOURCE_DB}.facilities f
            LEFT JOIN {SOURCE_DB}.facility_types ft ON f.facility_type_id=ft.id
            LEFT JOIN {SOURCE_DB}.geolocations g
                ON f.id=g.locatable_id AND g.locatable_type LIKE '%Facility'
            LEFT JOIN {SOURCE_DB}.countries c
                ON c.country_code COLLATE utf8mb3_unicode_ci=f.country_code
            LEFT JOIN {SOURCE_DB}.role_user ru ON f.owner_id=ru.user_id
            LEFT JOIN {SOURCE_DB}.hours_of_operation hoo ON f.id=hoo.facility_id
            WHERE f.id BETWEEN %s AND %s
            GROUP BY f.id,f.full_name,ft.facility_type,g.city,g.state,
                     g.latitude,g.longitude,c.name,f.entrance_location,
                     f.capacity,ru.user_id,f.created_at,f.garage_code,f.logo
            ORDER BY f.id
        """, (lo, hi))
        rows = scur.fetchall()
        out = []
        for r in rows:
            (fac_id,fac_name,fac_type,city,state,country,capacity,operator_id,
             garage_code,logo,open_time,close_time,lat,lon,location,eff_start) = r
            record_hash = hashlib.md5("".join([str(x or "") for x in
                [fac_name,fac_type,city,state,country,capacity,garage_code,logo]]).encode()).hexdigest()
            out.append((
                int(fac_id),fac_name,fac_type,city,state,country,capacity,operator_id,
                garage_code,logo,open_time,close_time,lat,lon,location,
                safe_dt(eff_start) or datetime.now(),
                datetime(9999,12,31,23,59,59), datetime.now(),
                datetime(9999,12,31,23,59,59), True, record_hash,
            ))
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_facility
            (facility_id,facility_name,facility_type,city,state,country,
             capacity,operator_id,garage_code,logo,open_time,close_time,
             latitude,longitude,location,effective_start_date,effective_end_date,
             dw_effective_start_date,dw_effective_end_date,is_current,record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                facility_name=VALUES(facility_name),facility_type=VALUES(facility_type),
                city=VALUES(city),state=VALUES(state),country=VALUES(country),
                capacity=VALUES(capacity),operator_id=VALUES(operator_id),
                garage_code=VALUES(garage_code),logo=VALUES(logo),
                open_time=VALUES(open_time),close_time=VALUES(close_time),
                latitude=VALUES(latitude),longitude=VALUES(longitude),
                location=VALUES(location),record_hash=VALUES(record_hash),updated_at=NOW()
        """, out)
        total += len(out); last_pk = hi
        log.info("dim_facility pk<=%s total=%s", last_pk, total)
    return last_pk

def ensure_dim_vehicle(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_vehicle (
            vehicle_key  BIGINT AUTO_INCREMENT PRIMARY KEY,
            vehicle_id   INT,
            vehicle_type VARCHAR(255),
            vehicle_code VARCHAR(255),
            is_ev_flag   TINYINT(1),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_vehicle (vehicle_id)
        )
    """)
    tconn.commit()

def load_dim_vehicle(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_vehicle(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_vehicle")
    rows = fetch_all(scur, f"""
        SELECT pv.vehicle_type_id,mvt.name,mvt.code,
               CASE WHEN mvt.code IS NULL THEN NULL
                    WHEN UPPER(mvt.code) LIKE '%EV%' OR UPPER(mvt.code) IN ('PHEV','HEV') THEN 1
                    ELSE 0 END,
               CURRENT_TIMESTAMP
        FROM {SOURCE_DB}.permit_vehicles pv
        LEFT JOIN {SOURCE_DB}.mst_vehicle_types mvt ON pv.vehicle_type_id=mvt.id
        WHERE pv.vehicle_type_id IS NOT NULL
        GROUP BY pv.vehicle_type_id,mvt.name,mvt.code ORDER BY pv.vehicle_type_id
    """)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_vehicle
            (vehicle_id,vehicle_type,vehicle_code,is_ev_flag,created_at)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE vehicle_type=VALUES(vehicle_type),
                vehicle_code=VALUES(vehicle_code),is_ev_flag=VALUES(is_ev_flag)
        """, chunk)
    log.info("dim_vehicle loaded: %s rows", len(rows))
    return len(rows)

def ensure_dim_rateplan(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_rateplan (
            rateplan_key         BIGINT AUTO_INCREMENT PRIMARY KEY,
            pricing_id           INT NOT NULL,
            rate_plan_name       VARCHAR(255),
            rate_type            VARCHAR(255),
            free_minutes         DECIMAL(8,2),
            max_daily_cap        DECIMAL(8,2),
            base_rate            DECIMAL(8,2),
            is_dynamic_flag      TINYINT,
            effective_start_date DATETIME NOT NULL,
            effective_end_date   DATETIME NOT NULL,
            is_current           TINYINT(1) DEFAULT 1,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            record_hash          CHAR(32),
            UNIQUE KEY uk_pricing_id (pricing_id),
            INDEX idx_rateplan_business (pricing_id),
            INDEX idx_rateplan_current  (pricing_id,is_current)
        )
    """)
    tconn.commit()

def load_dim_rateplan(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_rateplan(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_rateplan")
    import hashlib
    total, last_pk = 0, start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.rates", pk_col="id",
            cols="id,description,rate_type_id,free_hours,max_stay,price,active",
            start_after=start_after):
        out = []
        for r in rows:
            pricing_id,name,rate_type,free_min,max_cap,base,dynamic = r
            record_hash = hashlib.md5("|".join([str(x or "") for x in
                [name,rate_type,free_min,max_cap,base,dynamic]]).encode()).hexdigest()
            out.append((int(pricing_id),name,rate_type,free_min,max_cap,base,dynamic,
                        datetime.now(),datetime(2038,1,19,3,14,7),1,record_hash))
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_rateplan
            (pricing_id,rate_plan_name,rate_type,free_minutes,max_daily_cap,
             base_rate,is_dynamic_flag,effective_start_date,effective_end_date,
             is_current,record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE rate_plan_name=VALUES(rate_plan_name),
                rate_type=VALUES(rate_type),free_minutes=VALUES(free_minutes),
                max_daily_cap=VALUES(max_daily_cap),base_rate=VALUES(base_rate),
                is_dynamic_flag=VALUES(is_dynamic_flag),record_hash=VALUES(record_hash),
                updated_at=NOW()
        """, out)
        total += len(out); last_pk = int(rows[-1][0])
        log.info("dim_rateplan pk<=%s total=%s", last_pk, total)
    return last_pk

def ensure_dim_promo_code(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_promo_code (
            promo_key               BIGINT AUTO_INCREMENT PRIMARY KEY,
            promo_code_id           INT,
            promo_code              VARCHAR(255),
            promo_type              VARCHAR(50),
            discount_type           VARCHAR(20),
            discount_value          DECIMAL(10,2),
            start_date              DATE,
            end_date                DATE,
            is_active               TINYINT,
            is_tax_fees_applicable  TINYINT(1),
            facility_id             INT,
            effective_from          DATETIME,
            effective_to            DATETIME,
            effective_start_date    DATETIME NOT NULL,
            effective_end_date      DATETIME NOT NULL,
            is_current              TINYINT DEFAULT 1,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            record_hash             CHAR(32),
            INDEX idx_promo_code    (promo_code),
            INDEX idx_promo_current (promo_code,is_current),
            INDEX idx_facility_id   (facility_id),
            UNIQUE KEY uk_promo_current (promo_code,is_current)
        )
    """)
    tconn.commit()

def load_dim_promo_code(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_promo_code(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_promo_code")
    import hashlib
    rows = fetch_all(scur, f"""
        SELECT pc.id,pc.promocode,pt.name,pc.discount_type,pc.discount_value,
               pc.valid_from,pc.valid_to,pc.status,pr.is_tax_applicable,pf.facility_id,
               pc.created_at,
               CASE WHEN pc.deleted_at IS NULL THEN '2038-01-19 03:14:07' ELSE pc.deleted_at END
        FROM {SOURCE_DB}.promo_codes pc
        LEFT JOIN {SOURCE_DB}.promotions pr ON pc.promotion_id=pr.id
        LEFT JOIN {SOURCE_DB}.promo_types pt ON pc.promo_type_id=pt.id
        LEFT JOIN {SOURCE_DB}.promotion_facilities pf ON pc.promotion_id=pf.promotion_id
        WHERE pc.deleted_at IS NULL
        ORDER BY pc.promocode,pc.created_at DESC
    """)
    seen_codes: set = set()
    deduped = [r for r in rows if r[1] not in seen_codes and not seen_codes.add(r[1])]  # type: ignore[func-returns-value]
    out = []
    for r in deduped:
        (promo_id,promo_code,promo_type,disc_type,disc_value_raw,valid_from,valid_to,
         status_raw,is_tax,facility_id,eff_from,eff_to) = r
        disc_str = str(disc_value_raw or "").strip()
        if not disc_str or disc_str == "NULL":
            discount_value = 0.0
        elif "%" in disc_str:
            discount_value = safe_decimal(disc_str.replace("%",""))
        else:
            discount_value = safe_decimal(disc_str)
        record_hash = hashlib.md5("|".join([str(x or "") for x in
            [promo_type,disc_type,discount_value,valid_from,valid_to,
             status_raw,is_tax,facility_id]]).encode()).hexdigest()
        out.append((
            int(promo_id),promo_code,promo_type,disc_type,discount_value,
            safe_dt(valid_from),safe_dt(valid_to),int(status_raw or 0),int(is_tax or 0),
            facility_id,safe_dt(eff_from),
            safe_dt(eff_to) or datetime(2038,1,19,3,14,7),
            datetime.now(),datetime(2038,1,19,3,14,7),1,record_hash,
        ))
    for chunk in chunked(out, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_promo_code
            (promo_code_id,promo_code,promo_type,discount_type,discount_value,
             start_date,end_date,is_active,is_tax_fees_applicable,facility_id,
             effective_from,effective_to,effective_start_date,effective_end_date,
             is_current,record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                promo_type=VALUES(promo_type),discount_type=VALUES(discount_type),
                discount_value=VALUES(discount_value),is_active=VALUES(is_active),
                is_tax_fees_applicable=VALUES(is_tax_fees_applicable),
                facility_id=VALUES(facility_id),effective_from=VALUES(effective_from),
                effective_to=VALUES(effective_to),record_hash=VALUES(record_hash),updated_at=NOW()
        """, chunk)
    log.info("dim_promo_code loaded: %s rows (after dedup)", len(out))
    return len(out)

def ensure_dim_payment_method(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_payment_method (
            payment_method_key BIGINT AUTO_INCREMENT PRIMARY KEY,
            payment_method_id  INT NOT NULL,
            method_type        VARCHAR(50) NOT NULL,
            provider_name      VARCHAR(255),
            provider_country   VARCHAR(255) DEFAULT 'United States',
            created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_payment_method (payment_method_id,method_type)
        )
    """)
    tconn.commit()

def load_dim_payment_method(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_payment_method(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_payment_method")
    processors = fetch_all(tcur,
        f"SELECT processor_key,provider FROM {TARGET_DB}.dim_processor ORDER BY processor_key")
    out = [(int(pk), m, pv, "United States")
           for pk, pv in processors for m in ["CARD","Google Pay","Apple Pay","CASH"]]
    for chunk in chunked(out, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_payment_method
            (payment_method_id,method_type,provider_name,provider_country)
            VALUES (%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE provider_name=VALUES(provider_name),
                provider_country=VALUES(provider_country)
        """, chunk)
    log.info("dim_payment_method loaded: %s rows", len(out))
    return len(out)

def ensure_dim_processor(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_processor (
            processor_key  BIGINT AUTO_INCREMENT PRIMARY KEY,
            processor_id   INT,
            processor_name VARCHAR(100),
            provider       VARCHAR(100),
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_processor (processor_id,processor_name,provider)
        )
    """)
    tconn.commit()

def load_dim_processor(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_processor(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_processor")
    rows = fetch_all(scur, f"""
        SELECT DISTINCT id,LEFT(payment_type,100),LEFT(payment_type,100)
        FROM {SOURCE_DB}.facility_payment_type ORDER BY id
    """)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_processor (processor_id,processor_name,provider)
            VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE processor_name=VALUES(processor_name),provider=VALUES(provider)
        """, chunk)
    log.info("dim_processor loaded: %s rows", len(rows))

def ensure_dim_parking_product(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_parking_product (
            product_key BIGINT AUTO_INCREMENT PRIMARY KEY,
            product_id  BIGINT,
            name        VARCHAR(255),
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_product_id (product_id)
        )
    """)
    tconn.commit()

def load_dim_parking_product(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_parking_product(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_parking_product")
    rows = fetch_all(scur,
        f"SELECT id,LEFT(service_type,255) FROM {SOURCE_DB}.service_masters "
        f"WHERE deleted_at IS NULL ORDER BY id")
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_parking_product (product_id,name)
            VALUES (%s,%s) ON DUPLICATE KEY UPDATE name=VALUES(name)
        """, chunk)
    log.info("dim_parking_product loaded: %s rows", len(rows))

def ensure_dim_pass(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_pass (
            pass_key             BIGINT AUTO_INCREMENT PRIMARY KEY,
            pass_id              BIGINT,
            facility_key         BIGINT,
            partner_account_key  BIGINT,
            pass_status          VARCHAR(50),
            start_datetime       TIMESTAMP NULL,
            end_datetime         TIMESTAMP NULL,
            pass_name            VARCHAR(255),
            pass_type            VARCHAR(255),
            uses                 VARCHAR(50),
            price                DECIMAL(10,2),
            created_at           DATETIME,
            updated_at           DATETIME,
            effective_start_date DATETIME,
            effective_end_date   DATETIME,
            is_current           BOOLEAN,
            record_hash          CHAR(32),
            UNIQUE KEY uk_pass_id       (pass_id),
            INDEX idx_dim_pass_facility (facility_key),
            INDEX idx_dim_pass_partner  (partner_account_key),
            INDEX idx_dim_pass_current  (pass_id,is_current)
        )
    """)
    tconn.commit()

def load_dim_pass(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_pass(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_pass")
    import hashlib
    mc  = MapCache(tcur)
    fac = mc.load("fac", f"SELECT facility_key,facility_id FROM {TARGET_DB}.dim_facility WHERE is_current=1")
    acc = mc.load("acc", f"SELECT partner_account_key,account_id_source FROM {TARGET_DB}.dim_partner_account WHERE is_current=1")
    total, last_pk = 0, start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.rates", pk_col="id",
            cols="id,facility_id,partner_id,active,start_date,end_date,"
                 "description,total_usage,price,created_at,updated_at",
            where="rate_type_id=7", start_after=start_after):
        out = []
        for r in rows:
            (rate_id,fac_id,partner_id,active,start_date,end_date,
             description,total_usage,price,created_at_raw,updated_at_raw) = r
            record_hash = hashlib.md5(f"{rate_id}{fac_id}{partner_id}".encode()).hexdigest()
            out.append((
                int(rate_id),fac.get(fac_id),acc.get(partner_id),
                str(active) if active is not None else None,
                safe_dt(start_date),safe_dt(end_date),description,"PASS",
                str(total_usage) if total_usage is not None else None,
                safe_decimal(price),safe_dt(created_at_raw),safe_dt(updated_at_raw),
                safe_dt(start_date),safe_dt(end_date),True,record_hash,
            ))
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_pass
            (pass_id,facility_key,partner_account_key,pass_status,
             start_datetime,end_datetime,pass_name,pass_type,uses,price,
             created_at,updated_at,effective_start_date,effective_end_date,
             is_current,record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                facility_key=VALUES(facility_key),partner_account_key=VALUES(partner_account_key),
                pass_status=VALUES(pass_status),start_datetime=VALUES(start_datetime),
                end_datetime=VALUES(end_datetime),pass_name=VALUES(pass_name),
                uses=VALUES(uses),price=VALUES(price),updated_at=VALUES(updated_at),
                effective_end_date=VALUES(effective_end_date),record_hash=VALUES(record_hash)
        """, out)
        total += len(out); last_pk = int(rows[-1][0])
        log.info("dim_pass pk<=%s total=%s", last_pk, total)
    return last_pk

def ensure_dim_device(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_device (
            device_key       BIGINT AUTO_INCREMENT PRIMARY KEY,
            device_id        INT,
            device_type      VARCHAR(255),
            manufacturer     VARCHAR(255),
            model_number     VARCHAR(255),
            install_date     TIMESTAMP NULL,
            firmware_version INT,
            status           ENUM('0','1'),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_device_id (device_id)
        )
    """)
    tconn.commit()

def load_dim_device(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_device(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_device")
    rows = fetch_all(scur, f"""
        SELECT fc.id,pdt.name,NULL,pd.serial_number,pd.created_at,mdv.id,g.is_active
        FROM {SOURCE_DB}.im30_facility_configurations fc
        LEFT JOIN {SOURCE_DB}.parking_devices pd ON fc.facility_id=pd.facility_id
        LEFT JOIN {SOURCE_DB}.parking_device_types pdt ON pd.device_type_id=pdt.id
        LEFT JOIN {SOURCE_DB}.mobile_device_version mdv ON pd.partner_id=mdv.partner_id
        LEFT JOIN {SOURCE_DB}.gates g ON pd.gate_id=g.id
        ORDER BY fc.id
    """)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_device
            (device_id,device_type,manufacturer,model_number,install_date,firmware_version,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE device_type=VALUES(device_type),
                model_number=VALUES(model_number),install_date=VALUES(install_date),
                firmware_version=VALUES(firmware_version),status=VALUES(status)
        """, chunk)
    log.info("dim_device loaded: %s rows", len(rows))

def ensure_dim_event(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_event (
            event_key         BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_id          INT,
            facility_id       INT,
            event_name        VARCHAR(255),
            event_description TEXT,
            event_category    VARCHAR(255),
            event_start_date  DATE,
            event_end_date    DATE,
            event_start_time  DATETIME NULL,
            event_end_time    DATETIME NULL,
            event_rate        DECIMAL(10,2),
            is_active         TINYINT(1),
            created_at        DATETIME,
            updated_at        DATETIME,
            UNIQUE KEY uk_event_facility (event_id,facility_id)
        )
    """)
    tconn.commit()

def load_dim_event(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_event(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_event")
    rows = fetch_all(scur, f"""
        SELECT e.id,ef.facility_id,e.title,e.description,ec.name,
               e.start_time,e.end_time,e.event_rate,e.is_active,e.created_at,e.updated_at
        FROM {SOURCE_DB}.events e
        LEFT JOIN {SOURCE_DB}.event_facility ef ON e.id=ef.event_id
        LEFT JOIN {SOURCE_DB}.event_categories ec ON e.partner_id=ec.partner_id
        WHERE e.deleted_at IS NULL ORDER BY e.id
    """)
    out = []
    for r in rows:
        (event_id,facility_id,event_name,event_desc,event_cat,
         start_time_raw,end_time_raw,event_rate,is_active_raw,created_raw,updated_raw) = r
        start_dt = safe_dt(start_time_raw); end_dt = safe_dt(end_time_raw)
        out.append((
            int(event_id),facility_id,event_name,event_desc,event_cat,
            start_dt.date() if start_dt else None,
            end_dt.date()   if end_dt   else None,
            start_dt,end_dt,safe_decimal(event_rate),
            1 if str(is_active_raw or "")=="1" else 0,
            safe_dt(created_raw) or datetime.now(),
            safe_dt(updated_raw) or datetime.now(),
        ))
    for chunk in chunked(out, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_event
            (event_id,facility_id,event_name,event_description,event_category,
             event_start_date,event_end_date,event_start_time,event_end_time,
             event_rate,is_active,created_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                event_name=VALUES(event_name),event_description=VALUES(event_description),
                event_category=VALUES(event_category),event_start_date=VALUES(event_start_date),
                event_end_date=VALUES(event_end_date),event_start_time=VALUES(event_start_time),
                event_end_time=VALUES(event_end_time),event_rate=VALUES(event_rate),
                is_active=VALUES(is_active),updated_at=VALUES(updated_at)
        """, chunk)
    log.info("dim_event loaded: %s rows", len(out))

def ensure_dim_permit_plan(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_permit_plan (
            permit_key             BIGINT AUTO_INCREMENT PRIMARY KEY,
            permit_id              INT,
            permit_type            VARCHAR(255),
            permit_frequency_unit  VARCHAR(255),
            price                  DECIMAL(10,2),
            max_facilities_allowed INT,
            effective_start_date   DATETIME NOT NULL,
            effective_end_date     DATETIME NOT NULL,
            is_current             BOOLEAN NOT NULL DEFAULT TRUE,
            record_hash            CHAR(32),
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_permit_id       (permit_id),
            INDEX idx_permit_business_key (permit_id),
            INDEX idx_permit_lookup       (permit_id,is_current)
        )
    """)
    tconn.commit()

def load_dim_permit_plan(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_permit_plan(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_permit_plan")
    import hashlib
    rows = fetch_all(scur, f"""
        SELECT prd.id,prd.description,prd.permit_frequency_unit,
               MAX(prt.rate),COUNT(DISTINCT prt.facility_id)
        FROM {SOURCE_DB}.permit_rate_descriptions prd
        LEFT JOIN {SOURCE_DB}.permit_rates prt ON prd.id=prt.permit_rate_description_id
        WHERE prd.active_status='1'
        GROUP BY prd.id,prd.description,prd.permit_frequency_unit ORDER BY prd.id
    """)
    out = []
    for r in rows:
        permit_id,permit_type,permit_freq,price,max_fac = r
        record_hash = hashlib.md5("|".join([str(x or "") for x in
            [permit_type,permit_freq,price,max_fac]]).encode()).hexdigest()
        out.append((int(permit_id),permit_type,permit_freq,safe_decimal(price),max_fac,
                    datetime.now(),datetime(9999,12,31,23,59,59),True,record_hash))
    for chunk in chunked(out, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_permit_plan
            (permit_id,permit_type,permit_frequency_unit,price,
             max_facilities_allowed,effective_start_date,effective_end_date,
             is_current,record_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE permit_type=VALUES(permit_type),
                permit_frequency_unit=VALUES(permit_frequency_unit),
                price=VALUES(price),max_facilities_allowed=VALUES(max_facilities_allowed),
                record_hash=VALUES(record_hash)
        """, chunk)
    log.info("dim_permit_plan loaded: %s rows", len(out))

def ensure_dim_reason(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_reason (
            reason_key       BIGINT AUTO_INCREMENT PRIMARY KEY,
            reason_id_source INT,
            reason_name      VARCHAR(255),
            penalty_fee      DECIMAL(10,2),
            reason_category  VARCHAR(255),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_reason (reason_name,reason_category)
        )
    """)
    tconn.commit()

def load_dim_reason(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_reason(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_reason")
    rows = fetch_all(scur, f"""
        SELECT id,reason,penalty_fee,'ticket'  FROM {SOURCE_DB}.ticket_citation_infraction_reasons
        UNION
        SELECT id,reason,penalty_fee,'warning' FROM {SOURCE_DB}.warning_infraction_reasons
        UNION
        SELECT id,reason,penalty_fee,infraction_name FROM {SOURCE_DB}.warning_infractions
        UNION
        SELECT id,reason,penalty_fee,'master'  FROM {SOURCE_DB}.infraction_reasons
        ORDER BY 4,1
    """)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_reason
            (reason_id_source,reason_name,penalty_fee,reason_category)
            VALUES (%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE penalty_fee=VALUES(penalty_fee),
                reason_category=VALUES(reason_category)
        """, [(r[0],r[1],safe_decimal(r[2]),r[3]) for r in chunk])
    log.info("dim_reason loaded: %s rows", len(rows))

def ensure_dim_source_system(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_source_system (
            source_system_key     INT AUTO_INCREMENT PRIMARY KEY,
            source_ref_id         INT NOT NULL,
            source_name           VARCHAR(100) NOT NULL,
            api_version           VARCHAR(50),
            is_current            TINYINT DEFAULT 1,
            reporting_api_version VARCHAR(50),
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_ref_id (source_ref_id)
        )
    """)
    tconn.commit()

def load_dim_source_system(scur, tcur, tconn, *, start_after: int = 0):
    ensure_dim_source_system(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_source_system")
    rows = fetch_all(scur, f"""
        SELECT id,COALESCE(name,'UNKNOWN_DEVICE'),NULL,1,'1.0'
        FROM {SOURCE_DB}.parking_device_types ORDER BY id
    """)
    for chunk in chunked(rows, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_source_system
            (source_ref_id,source_name,api_version,is_current,reporting_api_version)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE source_name=VALUES(source_name),
                api_version=VALUES(api_version),is_current=VALUES(is_current),
                reporting_api_version=VALUES(reporting_api_version)
        """, chunk)
    log.info("dim_source_system loaded: %s rows", len(rows))

def ensure_dim_policy(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.dim_policy (
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
            updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (policy_key),
            INDEX idx_policy_id  (policy_id),
            INDEX idx_partner_id (partner_id),
            INDEX idx_is_current (is_current)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Dimension table for Business Policy'
    """)
    tconn.commit()

def load_dim_policy(scur, tcur, tconn, *, start_after: int = 0) -> int:
    ensure_dim_policy(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "dim_policy")
    rows = fetch_all(scur, f"""
        SELECT bp.id,bp.policy_name,bp.user_type,bp.consumption_channel,
               bp.discount_type,bp.discount_value,
               CASE WHEN CAST(bp.validity_start_date AS CHAR(19))='0000-00-00 00:00:00'
                    THEN NULL ELSE bp.validity_start_date END,
               CASE WHEN CAST(bp.validity_end_date AS CHAR(19))='0000-00-00 00:00:00'
                    THEN NULL ELSE bp.validity_end_date END,
               bp.partner_id,bp.created_by,bp.rm_id,bp.status
        FROM {SOURCE_DB}.business_policy bp
        WHERE bp.deleted_at IS NULL ORDER BY bp.id
    """)
    out = []
    for r in rows:
        (policy_id,policy_name,user_type,consumption_channel,
         discount_type,discount_value,validity_start,validity_end,
         partner_id,created_by,rm_id,status) = r
        out.append((
            int(policy_id),
            str(policy_name)[:100] if policy_name else None,
            int(user_type) if user_type is not None else None,
            str(consumption_channel)[:11] if consumption_channel else None,
            str(discount_type)[:11]  if discount_type  else None,
            str(discount_value)[:11] if discount_value  else None,
            safe_dt(validity_start),safe_dt(validity_end),
            int(partner_id) if partner_id is not None else None,
            int(created_by) if created_by is not None else None,
            int(rm_id)      if rm_id      is not None else None,
            int(status)     if status     is not None else 0,
            datetime.now(),1,datetime.now(),datetime.now(),
        ))
    for chunk in chunked(out, BATCH_SIZE):
        bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.dim_policy (
                policy_id,policy_name,user_type,consumption_channel,
                discount_type,discount_value,validity_start_date,validity_end_date,
                partner_id,created_by,rm_id,status,
                effective_date,is_current,created_at,updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                policy_name=VALUES(policy_name),user_type=VALUES(user_type),
                consumption_channel=VALUES(consumption_channel),
                discount_type=VALUES(discount_type),discount_value=VALUES(discount_value),
                validity_start_date=VALUES(validity_start_date),
                validity_end_date=VALUES(validity_end_date),
                partner_id=VALUES(partner_id),created_by=VALUES(created_by),
                rm_id=VALUES(rm_id),status=VALUES(status),updated_at=NOW()
        """, chunk)
    log.info("dim_policy loaded: %s rows", len(out))
    return len(out)


# ===========================================================================
# FACTS
# ===========================================================================

# ---------------------------------------------------------------------------
# fact_parking_session
# NEW: validation_refund_flag TINYINT(1) NOT NULL DEFAULT 0
#      = 1 when validation_refunds.reference_key matches ticket_number
#        for any of the three session sources (tickets / overstay / extends).
# ---------------------------------------------------------------------------
def ensure_fact_parking_session(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_parking_session (
            canonical_session_key   BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_id               BIGINT NOT NULL,
            extension_overstay_flag TINYINT NOT NULL DEFAULT 0,
            facility_key            BIGINT NULL,
            vehicle_key             BIGINT NULL,
            parker_key              BIGINT NULL,
            partner_account_key     BIGINT NULL,
            rate_plan_key           BIGINT NULL,
            promo_code_key          BIGINT NULL,
            entry_date_key          INT NULL,
            exit_date_key           INT NULL,
            entry_time_key          INT NULL,
            exit_time_key           INT NULL,
            reservation_key         BIGINT NULL,
            event_key               BIGINT NULL,
            permit_subscription_key BIGINT NULL,
            pass_subscription_key   BIGINT NULL,
            duration_hours          DECIMAL(10,2) NULL,
            reserv_permit_pass_flag TINYINT NOT NULL DEFAULT 0,
            entitlement_flag        TINYINT(1) NOT NULL DEFAULT 0,
            validation_applied_flag TINYINT(1) NOT NULL DEFAULT 0,
            session_status          VARCHAR(20) NULL,
            session_source_type_key INT NULL,
            session_quality_score   DECIMAL(5,2) NULL,
            ticket_number           VARCHAR(255) NULL,
            lpr_entry_event_id      VARCHAR(100) NULL,
            lpr_exit_event_id       VARCHAR(100) NULL,
            session_build_version   VARCHAR(50) NULL,
            license_plate           VARCHAR(45) NULL,
            validation_code         VARCHAR(255) NULL,
            attendant_user_id       INT NULL,
            policy_key              BIGINT NULL,
            validation_refund_flag  TINYINT(1) NOT NULL DEFAULT 0
                COMMENT '1 = ticket has at least one row in validation_refunds; 0 = none',
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_session      (extension_overstay_flag, source_id),
            INDEX idx_fps_ticket_number       (ticket_number),
            INDEX idx_fps_facility            (facility_key),
            INDEX idx_fps_vehicle             (vehicle_key),
            INDEX idx_fps_reservation         (reservation_key),
            INDEX idx_fps_event               (event_key),
            INDEX idx_fps_permit_sub          (permit_subscription_key),
            INDEX idx_fps_pass_sub            (pass_subscription_key)
        )
    """)
    tconn.commit()

    # Backfill: add column to pre-existing tables that were created before this change.
    if not col_exists(tcur, TARGET_DB, "fact_parking_session", "validation_refund_flag"):
        tcur.execute(f"""
            ALTER TABLE {TARGET_DB}.fact_parking_session
            ADD COLUMN validation_refund_flag TINYINT(1) NOT NULL DEFAULT 0
                COMMENT '1 = ticket has at least one row in validation_refunds; 0 = none'
                AFTER policy_key
        """)
        tconn.commit()
        log.info("Added missing column fact_parking_session.validation_refund_flag")


# INSERT template — 34 value slots (33 data cols + created_at).
_FPS_INSERT = f"""
    INSERT INTO {TARGET_DB}.fact_parking_session (
        source_id,
        extension_overstay_flag,
        facility_key, vehicle_key, parker_key, partner_account_key,
        rate_plan_key, promo_code_key,
        entry_date_key, exit_date_key, entry_time_key, exit_time_key,
        reservation_key, event_key,
        permit_subscription_key, pass_subscription_key,
        duration_hours,
        reserv_permit_pass_flag,
        entitlement_flag, validation_applied_flag,
        session_status, session_source_type_key, session_quality_score,
        ticket_number, lpr_entry_event_id, lpr_exit_event_id,
        session_build_version, license_plate, validation_code,
        attendant_user_id,
        policy_key,
        validation_refund_flag,
        created_at
    ) VALUES (
        %s,%s,
        %s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        facility_key            = VALUES(facility_key),
        vehicle_key             = VALUES(vehicle_key),
        parker_key              = VALUES(parker_key),
        partner_account_key     = VALUES(partner_account_key),
        rate_plan_key           = VALUES(rate_plan_key),
        promo_code_key          = VALUES(promo_code_key),
        entry_date_key          = VALUES(entry_date_key),
        exit_date_key           = VALUES(exit_date_key),
        entry_time_key          = VALUES(entry_time_key),
        exit_time_key           = VALUES(exit_time_key),
        reservation_key         = VALUES(reservation_key),
        event_key               = VALUES(event_key),
        permit_subscription_key = VALUES(permit_subscription_key),
        pass_subscription_key   = VALUES(pass_subscription_key),
        duration_hours          = VALUES(duration_hours),
        reserv_permit_pass_flag = VALUES(reserv_permit_pass_flag),
        entitlement_flag        = VALUES(entitlement_flag),
        validation_applied_flag = VALUES(validation_applied_flag),
        session_status          = VALUES(session_status),
        ticket_number           = VALUES(ticket_number),
        license_plate           = VALUES(license_plate),
        validation_code         = VALUES(validation_code),
        attendant_user_id       = VALUES(attendant_user_id),
        policy_key              = COALESCE(VALUES(policy_key), policy_key),
        validation_refund_flag  = VALUES(validation_refund_flag)
"""


def load_fact_parking_session(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_parking_session(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_parking_session")

    mc   = MapCache(tcur)
    fac  = mc.load("fac",   f"SELECT facility_key, facility_id    FROM {TARGET_DB}.dim_facility         WHERE is_current=1")
    veh  = mc.load("veh",   f"SELECT vehicle_key, vehicle_id      FROM {TARGET_DB}.dim_vehicle")
    par  = mc.load("par",   f"SELECT parker_key, customer_id      FROM {TARGET_DB}.dim_parker")
    acc  = mc.load("acc",   f"SELECT partner_account_key, account_id_source FROM {TARGET_DB}.dim_partner_account WHERE is_current=1")
    rate = mc.load("rate",  f"SELECT rateplan_key, pricing_id     FROM {TARGET_DB}.dim_rateplan         WHERE is_current=1")
    pro  = mc.load("promo", f"SELECT promo_key, promo_code        FROM {TARGET_DB}.dim_promo_code       WHERE is_current=1")
    resv_key = mc.load("resv_key",
        f"SELECT reservation_key, source_reservation_id FROM {TARGET_DB}.fact_reservation")
    tcur.execute(
        f"SELECT source_reservation_id, event_key FROM {TARGET_DB}.fact_reservation "
        f"WHERE event_key IS NOT NULL")
    resv_evt = {int(r[0]): r[1] for r in tcur.fetchall() if r[0] is not None}
    perm_sub = mc.load("perm_sub",
        f"SELECT permit_subscription_key, source_permit_id FROM {TARGET_DB}.fact_permit_subscription")
    pass_sub = mc.load("pass_sub",
        f"SELECT pass_subscription_key, source_user_pass_id "
        f"FROM {TARGET_DB}.fact_passes WHERE source_user_pass_id IS NOT NULL")
    if not pass_sub:
        log.warning("fact_parking_session: pass_sub map is empty — fact_passes may not be loaded")
    pol = mc.load("pol", f"SELECT policy_key, policy_id FROM {TARGET_DB}.dim_policy WHERE is_current=1")

    # ── NEW: load validation_refund set (ticket_number → has refund) ─────────
    vr_set: set = load_vr_set(scur)
    log.info("fact_parking_session: %s ticket_numbers have validation refunds", len(vr_set))

    rows_read = rows_inserted = 0
    last_pk = start_after

    # ── SOURCE 1 : tickets  →  extension_overstay_flag = 0 ──────────────────
    log.info("fact_parking_session – SOURCE 1: tickets (flag=0)")
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.tickets", pk_col="id",
            cols=("id,facility_id,vehicle_id,user_id,partner_id,rate_id,"
                  "checkin_time,check_in_datetime,checkout_time,checkout_datetime,"
                  "estimated_checkout,reservation_id,user_pass_id,permit_request_id,"
                  "affiliate_business_id,deleted_at,cancelled_at,is_checkout,"
                  "is_checkin,length,ticket_number,license_plate,promocode,"
                  "is_extended,is_overstay,session_id,event_user_id,policy_id"),
            where="deleted_at IS NULL",
            start_after=start_after):
        out = []
        for r in rows:
            (src_id, fac_id, veh_id, usr_id, ptnr_id, rate_id,
             ckin_t, ckin_dt, ckout_t, ckout_dt, est_ckout,
             rsv_id, upass_id, permit_id, aff_id,
             del_at, can_at, is_ckout, is_ckin, length,
             tkt_no, lic_pl, promo, is_ext, is_ovs, sess_id, evt_usr_id,
             policy_id) = r

            entry = safe_dt(ckin_t or ckin_dt)
            exit_ = safe_dt(ckout_t or ckout_dt or est_ckout)
            status = (
                "void"      if del_at or can_at else
                "closed"    if is_ckout == 1 or ckout_dt or ckout_t else
                "open"      if is_ckin  == 1 or ckin_t  or ckin_dt  else
                "estimated" if est_ckout else "unknown"
            )
            if rsv_id:
                rpf = 1
                reservation_key_val     = resv_key.get(int(rsv_id))
                event_key_val           = resv_evt.get(int(rsv_id))
                permit_subscription_key = None
                pass_subscription_key   = None
            elif permit_id:
                rpf = 2
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = perm_sub.get(int(permit_id))
                pass_subscription_key   = None
            elif upass_id:
                rpf = 3
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = None
                pass_subscription_key   = pass_sub.get(int(upass_id))
            else:
                rpf = 0
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = None
                pass_subscription_key   = None

            # NEW: set flag if this ticket_number has any validation refund
            vr_flag = 1 if tkt_no is not None and tkt_no in vr_set else 0

            out.append((
                int(src_id), 0,
                fac.get(fac_id), veh.get(veh_id), par.get(usr_id),
                acc.get(ptnr_id), rate.get(rate_id), pro.get(promo),
                dk(entry), dk(exit_), tk(entry), tk(exit_),
                reservation_key_val, event_key_val,
                permit_subscription_key, pass_subscription_key,
                float(length or 0) if length is not None else 0.0, rpf,
                1 if permit_id else 0,
                1 if aff_id    else 0,
                status, None, None, tkt_no, None, None, None, lic_pl, promo,
                evt_usr_id,
                pol.get(int(policy_id)) if policy_id is not None else None,
                vr_flag,           # validation_refund_flag
                datetime.now(),
            ))

        inserted = bulk_insert(tcur, tconn, _FPS_INSERT, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_parking_session",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_parking_session tickets pk<=%s read=%s inserted=%s",
                 last_pk, rows_read, rows_inserted)

    # ── SOURCE 2 : overstay_tickets  →  extension_overstay_flag = 1 ─────────
    log.info("fact_parking_session – SOURCE 2: overstay_tickets (flag=1)")
    last_pk_ov = 0
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.overstay_tickets", pk_col="id",
            cols=("id,user_id,facility_id,partner_id,rate_id,"
                  "check_in_datetime,checkout_datetime,estimated_checkout,"
                  "reservation_id,is_checkin,is_checkout,length,ticket_number"),
            where="1=1", start_after=0):
        out = []
        for r in rows:
            (ov_id, user_id, fac_id, partner_id, rate_id,
             ckin_dt, ckout_dt, est_ckout,
             rsv_id, is_ckin, is_ckout, length, tkt_no) = r

            entry  = safe_dt(ckin_dt)
            exit_  = safe_dt(ckout_dt or est_ckout)
            status = (
                "closed"    if str(is_ckout or "") == "1" else
                "open"      if str(is_ckin  or "") == "1" else
                "estimated" if est_ckout else "unknown"
            )
            if rsv_id:
                rpf = 1
                reservation_key_val = resv_key.get(int(rsv_id))
                event_key_val       = resv_evt.get(int(rsv_id))
            else:
                rpf = 0
                reservation_key_val = None
                event_key_val       = None

            vr_flag = 1 if tkt_no is not None and tkt_no in vr_set else 0

            out.append((
                int(ov_id), 1,
                fac.get(fac_id), None, par.get(user_id),
                acc.get(partner_id), rate.get(rate_id), None,
                dk(entry), dk(exit_), tk(entry), tk(exit_),
                reservation_key_val, event_key_val, None, None,
                float(length or 0) if length is not None else 0.0, rpf,
                0, 0,
                status, None, None, tkt_no, None, None, None, None, None,
                None, None,
                vr_flag,           # validation_refund_flag
                datetime.now(),
            ))

        inserted = bulk_insert(tcur, tconn, _FPS_INSERT, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk_ov = int(rows[-1][0])
        log.info("fact_parking_session overstay pk<=%s read=%s inserted=%s",
                 last_pk_ov, rows_read, rows_inserted)

    # ── SOURCE 3 : ticket_extends  →  extension_overstay_flag = 2 ───────────
    log.info("fact_parking_session – SOURCE 3: ticket_extends (flag=2)")
    last_pk_ext = 0
    while True:
        scur.execute(f"""
            SELECT te.id, te.facility_id, te.partner_id, t.user_id, t.vehicle_id, t.rate_id,
                   te.checkin_time, te.checkout_time,
                   t.reservation_id, t.user_pass_id, t.permit_request_id,
                   te.length, te.ticket_number
            FROM {SOURCE_DB}.ticket_extends te
            INNER JOIN {SOURCE_DB}.tickets t ON te.ticket_id = t.id AND t.deleted_at IS NULL
            WHERE te.deleted_at IS NULL AND te.id > %s
            ORDER BY te.id LIMIT %s
        """, (last_pk_ext, BATCH_SIZE))
        rows = scur.fetchall()
        if not rows:
            break

        out = []
        for r in rows:
            (ext_id, fac_id, partner_id, user_id, veh_id, rate_id,
             ckin_t, ckout_t, rsv_id, upass_id, permit_id, length, tkt_no) = r

            entry  = safe_dt(ckin_t)
            exit_  = safe_dt(ckout_t)
            status = ("closed" if ckout_t else "open" if ckin_t else "unknown")

            if rsv_id:
                rpf = 1
                reservation_key_val     = resv_key.get(int(rsv_id))
                event_key_val           = resv_evt.get(int(rsv_id))
                permit_subscription_key = None
                pass_subscription_key   = None
            elif permit_id:
                rpf = 2
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = perm_sub.get(int(permit_id))
                pass_subscription_key   = None
            elif upass_id:
                rpf = 3
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = None
                pass_subscription_key   = pass_sub.get(int(upass_id))
            else:
                rpf = 0
                reservation_key_val     = None
                event_key_val           = None
                permit_subscription_key = None
                pass_subscription_key   = None

            vr_flag = 1 if tkt_no is not None and tkt_no in vr_set else 0

            out.append((
                int(ext_id), 2,
                fac.get(fac_id), veh.get(veh_id), par.get(user_id),
                acc.get(partner_id), rate.get(rate_id), None,
                dk(entry), dk(exit_), tk(entry), tk(exit_),
                reservation_key_val, event_key_val,
                permit_subscription_key, pass_subscription_key,
                float(length or 0) if length is not None else 0.0, rpf,
                1 if permit_id else 0, 0,
                status, None, None, tkt_no, None, None, None, None, None,
                None, None,
                vr_flag,           # validation_refund_flag
                datetime.now(),
            ))

        inserted = bulk_insert(tcur, tconn, _FPS_INSERT, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk_ext = int(rows[-1][0])
        log.info("fact_parking_session extends pk<=%s read=%s inserted=%s",
                 last_pk_ext, rows_read, rows_inserted)

    set_watermark(tcur, tconn, "fact_parking_session",
                  last_pk=last_pk, status="RUNNING",
                  rows_read=rows_read, rows_inserted=rows_inserted)
    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# fact_reservation  (unchanged logic)
# ---------------------------------------------------------------------------
def ensure_fact_reservation(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_reservation (
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
            policy_key            BIGINT NULL,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (reservation_key),
            UNIQUE KEY uk_source_reservation_id    (source_reservation_id),
            UNIQUE KEY uk_source_transaction_id    (source_transaction_id),
            KEY idx_fact_reservation_facility_key  (facility_key),
            KEY idx_fact_reservation_event_key     (event_key),
            KEY idx_fact_reservation_partner_key   (partner_account_key),
            KEY idx_fact_reservation_parker_key    (parker_key),
            KEY idx_fact_reservation_vehicle_key   (vehicle_key),
            KEY idx_fact_reservation_rate_plan_key (rateplan_key),
            KEY idx_fact_reservation_promo_key     (promo_key),
            KEY idx_fact_reservation_booking_id    (booking_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    tconn.commit()

def load_fact_reservation(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_reservation(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_reservation")
    mc    = MapCache(tcur)
    fac   = mc.load("fac",   f"SELECT facility_key, facility_id       FROM {TARGET_DB}.dim_facility       WHERE is_current=1")
    evt   = mc.load("evt",   f"SELECT event_key,    event_id          FROM {TARGET_DB}.dim_event")
    acc   = mc.load("acc",   f"SELECT partner_account_key, account_id_source FROM {TARGET_DB}.dim_partner_account WHERE is_current=1")
    par   = mc.load("par",   f"SELECT parker_key,   customer_id       FROM {TARGET_DB}.dim_parker")
    veh   = mc.load("veh",   f"SELECT vehicle_key,  vehicle_id        FROM {TARGET_DB}.dim_vehicle")
    rate  = mc.load("rate",  f"SELECT rateplan_key, pricing_id        FROM {TARGET_DB}.dim_rateplan       WHERE is_current=1")
    promo = mc.load("promo", f"SELECT promo_key,    promo_code        FROM {TARGET_DB}.dim_promo_code     WHERE is_current=1")
    ti_rows = fetch_all(scur, f"SELECT id, name FROM {SOURCE_DB}.thirdparty_integrations")
    ti_map  = {int(r[0]): r[1] for r in ti_rows if r[0] is not None}
    rows_read = rows_inserted = 0
    last_pk = start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.reservations", pk_col="id",
            cols=("id,facility_id,event_id,partner_id,user_id,vehicle_id,"
                  "rate_id,created_at,start_timestamp,end_timestamp,"
                  "cancelled_at,checkin_status,deleted_at,is_charged,"
                  "promocode,booking_source,license_plate,ticketech_code,"
                  "thirdparty_integration_id,anet_transaction_id"),
            where="deleted_at IS NULL", start_after=start_after):
        out = []
        for r in rows:
            (src_id,fac_id,event_id,partner_id,user_id,vehicle_id,
             rate_id,created_at_raw,start_ts_raw,end_ts_raw,
             cancelled_at,checkin_status,deleted_at,is_charged,
             promocode,booking_source_raw,license_plate_raw,
             ticketech_code,thirdparty_id,anet_txn_id) = r
            checkin_lower = str(checkin_status or "").lower()
            if cancelled_at is not None:     status = "cancelled"
            elif checkin_lower in ("no_show","noshow"): status = "no_show"
            elif deleted_at is not None:     status = "expired"
            elif str(is_charged or "")=="1": status = "used"
            else:                            status = "booked"
            ti_name     = ti_map.get(int(thirdparty_id)) if thirdparty_id is not None else None
            booking_src = ti_name if ti_name is not None else booking_source_raw
            lp = str(license_plate_raw or "")[:10] if license_plate_raw else None
            out.append((
                int(src_id),
                int(anet_txn_id) if anet_txn_id is not None else None,
                fac.get(fac_id), evt.get(event_id), acc.get(partner_id),
                par.get(user_id), veh.get(vehicle_id), rate.get(rate_id),
                safe_dt(created_at_raw), safe_dt(start_ts_raw), safe_dt(end_ts_raw),
                status, promo.get(promocode), booking_src, lp,
                str(ticketech_code) if ticketech_code is not None else None,
            ))
        inserted = bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.fact_reservation (
                source_reservation_id, source_transaction_id,
                facility_key, event_key, partner_account_key,
                parker_key, vehicle_key, rateplan_key,
                created_ts, start_ts, end_ts, status,
                promo_key, booking_source, license_plate, booking_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                source_transaction_id=COALESCE(VALUES(source_transaction_id),source_transaction_id),
                facility_key=VALUES(facility_key), event_key=VALUES(event_key),
                partner_account_key=VALUES(partner_account_key), parker_key=VALUES(parker_key),
                vehicle_key=VALUES(vehicle_key), rateplan_key=VALUES(rateplan_key),
                created_ts=VALUES(created_ts), start_ts=VALUES(start_ts), end_ts=VALUES(end_ts),
                status=VALUES(status), promo_key=VALUES(promo_key),
                booking_source=VALUES(booking_source), license_plate=VALUES(license_plate),
                booking_id=VALUES(booking_id)
        """, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_reservation",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_reservation pk<=%s read=%s inserted=%s", last_pk, rows_read, rows_inserted)
    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# fact_validation_redemption  (unchanged)
# ---------------------------------------------------------------------------
def ensure_fact_validation_redemption(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_validation_redemption (
            redemption_key        BIGINT AUTO_INCREMENT PRIMARY KEY,
            redemption_ts_utc     TIMESTAMP NULL,
            date_key              INT NULL,
            facility_key          BIGINT NULL,
            promo_key             BIGINT NULL,
            canonical_session_key BIGINT NULL,
            reservation_key       BIGINT NULL,
            redemption_amount     DECIMAL(12,2) DEFAULT 0,
            approved_flag         TINYINT NULL,
            rule_version          VARCHAR(50) NULL,
            source_ticket_id      BIGINT NOT NULL,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_ticket  (source_ticket_id),
            INDEX idx_date        (date_key),
            INDEX idx_facility    (facility_key),
            INDEX idx_promo       (promo_key),
            INDEX idx_session     (canonical_session_key),
            INDEX idx_reservation (reservation_key)
        )
    """)
    tconn.commit()

def load_fact_validation_redemption(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_validation_redemption(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_validation_redemption")
    mc    = MapCache(tcur)
    fac   = mc.load("fac",   f"SELECT facility_key, facility_id FROM {TARGET_DB}.dim_facility WHERE is_current=1")
    promo = mc.load("promo", f"SELECT promo_key, promo_code FROM {TARGET_DB}.dim_promo_code WHERE is_current=1")
    sess  = mc.load("sess",  f"SELECT canonical_session_key, ticket_number FROM {TARGET_DB}.fact_parking_session")
    resv  = mc.load("resv",  f"SELECT reservation_key, source_reservation_id FROM {TARGET_DB}.fact_reservation")
    rows_read = rows_inserted = 0
    last_pk = start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.tickets", pk_col="id",
            cols="id,facility_id,promocode,paid_date,created_at,paid_amount,session_id,reservation_id",
            where="promocode IS NOT NULL AND deleted_at IS NULL", start_after=start_after):
        out = []
        for r in rows:
            (src_id,fac_id,promo_code,paid_date,cre_at,paid_amt,sess_id,resv_id) = r
            redemption_ts = safe_dt(paid_date or cre_at)
            out.append((
                redemption_ts, dk(redemption_ts), fac.get(fac_id), promo.get(promo_code),
                sess.get(sess_id), resv.get(resv_id),
                float(paid_amt or 0), None, None, src_id,
            ))
        inserted = bulk_insert(tcur, tconn, f"""
            INSERT IGNORE INTO {TARGET_DB}.fact_validation_redemption (
                redemption_ts_utc,date_key,facility_key,promo_key,
                canonical_session_key,reservation_key,
                redemption_amount,approved_flag,rule_version,source_ticket_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_validation_redemption",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_validation_redemption pk<=%s read=%s inserted=%s",
                 last_pk, rows_read, rows_inserted)
    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# fact_permit_subscription  (unchanged)
# ---------------------------------------------------------------------------
def ensure_fact_permit_subscription(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_permit_subscription (
            permit_subscription_key BIGINT AUTO_INCREMENT PRIMARY KEY,
            parker_key              BIGINT,
            facility_key            BIGINT,
            facility_group_key      BIGINT NULL,
            product_key             BIGINT,
            period_start_date_key   DATE,
            period_end_date_key     DATE,
            status                  VARCHAR(50),
            billed_amount           DECIMAL(12,2) DEFAULT 0,
            paid_amount             DECIMAL(12,2) DEFAULT 0,
            balance                 DECIMAL(12,2) DEFAULT 0,
            spaces_entitled         INT NULL,
            source_permit_id        BIGINT,
            policy_key              BIGINT NULL,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_permit_id (source_permit_id),
            INDEX idx_parker   (parker_key),
            INDEX idx_facility (facility_key),
            INDEX idx_product  (product_key)
        )
    """)
    tconn.commit()

def load_fact_permit_subscription(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_permit_subscription(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_permit_subscription")
    mc   = MapCache(tcur)
    par  = mc.load("par",  f"SELECT parker_key, customer_id FROM {TARGET_DB}.dim_parker")
    fac  = mc.load("fac",  f"SELECT facility_key, facility_id FROM {TARGET_DB}.dim_facility WHERE is_current=1")
    prod = mc.load("prod", f"SELECT product_key, product_id FROM {TARGET_DB}.dim_parking_product")
    rows_read = rows_inserted = 0
    last_pk = start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.permit_requests", pk_col="id",
            cols=("id,user_id,facility_id,partner_id,"
                  "desired_start_date,desired_end_date,"
                  "status,deleted_at,cancelled_at,"
                  "permit_rate,anet_transaction_id"),
            where="deleted_at IS NULL", start_after=start_after):
        anet_ids = [r[10] for r in rows if r[10] is not None]
        anet_totals: dict[int, float] = {}
        if anet_ids:
            fmt = ",".join(["%s"] * len(anet_ids))
            scur.execute(
                f"SELECT id, total FROM {SOURCE_DB}.anet_transactions WHERE id IN ({fmt})",
                tuple(anet_ids))
            anet_totals = {int(r[0]): float(r[1] or 0) for r in scur.fetchall()}
        out = []
        for r in rows:
            (src_id,usr_id,fac_id,ptnr_id,start_dt,end_dt,
             status_code,del_at,can_at,permit_rate,anet_id) = r
            status = (
                "deleted"   if del_at else
                "cancelled" if can_at else
                "active"    if str(status_code or "")=="1" else
                "suspended" if str(status_code or "")=="2" else "pending"
            )
            billed = float(permit_rate or 0)
            paid   = anet_totals.get(anet_id, 0.0) if anet_id else 0.0
            out.append((
                par.get(usr_id), fac.get(fac_id), None, prod.get(ptnr_id),
                safe_dt(start_dt), safe_dt(end_dt),
                status, billed, paid, billed - paid, None, src_id,
            ))
        inserted = bulk_insert(tcur, tconn, f"""
            INSERT IGNORE INTO {TARGET_DB}.fact_permit_subscription (
                parker_key,facility_key,facility_group_key,product_key,
                period_start_date_key,period_end_date_key,
                status,billed_amount,paid_amount,balance,spaces_entitled,source_permit_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_permit_subscription",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_permit_subscription pk<=%s read=%s inserted=%s",
                 last_pk, rows_read, rows_inserted)
    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# fact_passes  (unchanged)
# ---------------------------------------------------------------------------
def ensure_fact_passes(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_passes (
            pass_subscription_key  BIGINT NOT NULL AUTO_INCREMENT,
            parker_key             BIGINT NULL,
            pass_key               BIGINT NULL,
            period_start_date_key  INT NULL,
            period_end_date_key    INT NULL,
            status_date            TIMESTAMP NULL,
            source_pass_id         VARCHAR(45) NULL,
            source_user_pass_id    BIGINT NULL,
            policy_key             BIGINT NULL,
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (pass_subscription_key),
            UNIQUE KEY uk_source_user_pass_id         (source_user_pass_id),
            KEY idx_fact_passes_parker_key            (parker_key),
            KEY idx_fact_passes_pass_key              (pass_key),
            KEY idx_fact_passes_period_start_date_key (period_start_date_key),
            KEY idx_fact_passes_period_end_date_key   (period_end_date_key),
            KEY idx_fact_passes_source_pass_id        (source_pass_id),
            KEY idx_fact_passes_source_user_pass_id   (source_user_pass_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    tconn.commit()

def load_fact_passes(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_passes(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_passes")
    mc    = MapCache(tcur)
    par   = mc.load("par",  f"SELECT parker_key, customer_id FROM {TARGET_DB}.dim_parker")
    pass_ = mc.load("pass", f"SELECT pass_key, pass_id FROM {TARGET_DB}.dim_pass")
    rows_read = rows_inserted = 0
    last_pk = start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.user_passes", pk_col="id",
            cols="id,user_id,rate_id,start_time,end_time,cancelled_at,pass_code",
            where="deleted_at IS NULL", start_after=start_after):
        out = []
        for r in rows:
            (src_id,user_id,rate_id,start_time_raw,
             end_time_raw,cancelled_at_raw,pass_code) = r
            start_dt = safe_dt(start_time_raw); end_dt = safe_dt(end_time_raw)
            cancelled_str = str(cancelled_at_raw or "").strip()
            status_date   = None if not cancelled_str else safe_dt(cancelled_at_raw)
            out.append((
                par.get(user_id), pass_.get(rate_id),
                dk(start_dt) if start_dt else None,
                dk(end_dt)   if end_dt   else None,
                status_date,
                str(pass_code) if pass_code is not None else None,
                int(src_id),
            ))
        inserted = bulk_insert(tcur, tconn, f"""
            INSERT INTO {TARGET_DB}.fact_passes (
                parker_key, pass_key,
                period_start_date_key, period_end_date_key,
                status_date, source_pass_id, source_user_pass_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                parker_key=VALUES(parker_key), pass_key=VALUES(pass_key),
                period_start_date_key=VALUES(period_start_date_key),
                period_end_date_key=VALUES(period_end_date_key),
                status_date=VALUES(status_date), source_pass_id=VALUES(source_pass_id)
        """, out)
        rows_read += len(rows); rows_inserted += inserted
        last_pk = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_passes",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_passes pk<=%s read=%s inserted=%s", last_pk, rows_read, rows_inserted)
    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# fact_payment
# NEW columns (41 total value slots):
#   validate_refund_amount  DECIMAL(10,2) NULL  — validation_refunds.total
#   vr_anet_trans_id        INT NULL            — validation_refunds.anet_transaction_id
#   vr_refund_status        ENUM(...) NULL      — validation_refunds.transaction_status
# All three are populated only for SOURCE 1 (tickets); NULL for every other source.
# ---------------------------------------------------------------------------
def ensure_fact_payment(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_payment (
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
            processing_fees         DECIMAL(8,2)  NULL,
            oversize_fees           DECIMAL(8,2)  NULL,
            net_parking_amount      DECIMAL(12,2) NULL,
            refund_date             TIMESTAMP NULL,
            permit_prorate          DECIMAL(12,2) NULL,
            is_offline_payment      TINYINT(1) NOT NULL DEFAULT 0,
            tax_exempt_flag         ENUM('0','1','2','3','4','5','6','7','8','9') NULL
                COMMENT 'Mapped from tickets.paid_type; NULL for non-ticket sources',
            sales_tax_exemption     DECIMAL(12,2) NULL DEFAULT 0,
            sales_tax_collected     DECIMAL(12,2) NULL DEFAULT 0,
            validate_refund_amount  DECIMAL(10,2) NULL
                COMMENT 'validation_refunds.total for this ticket; NULL if no refund or non-ticket source',
            vr_anet_trans_id        INT NULL
                COMMENT 'validation_refunds.anet_transaction_id; NULL if no refund or non-ticket source',
            vr_refund_status        ENUM('PENDING','FAILED','REFUNDED') NULL
                COMMENT 'validation_refunds.transaction_status; NULL if no refund or non-ticket source',
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_transaction_id (source_transaction_id),
            INDEX idx_date_key        (date_key),
            INDEX idx_facility_key    (facility_key),
            INDEX idx_session_key     (canonical_session_key),
            INDEX idx_reservation_key (reservation_key),
            INDEX idx_pass_sub_key    (pass_subscription_key)
        )
    """)
    tconn.commit()

    # Backfill any columns that may be missing on pre-existing tables
    for col_name, col_def in [
        ("event_key",             "BIGINT NULL AFTER reservation_key"),
        ("pass_subscription_key", "BIGINT NULL AFTER permit_subscription_key"),
        ("card_type",             "VARCHAR(50) NULL AFTER approved_flag"),
        ("is_offline_payment",    "TINYINT(1) NOT NULL DEFAULT 0 AFTER oversize_fees"),
        ("net_parking_amount",    "DECIMAL(12,2) NULL AFTER is_offline_payment"),
        ("refund_date",           "TIMESTAMP NULL AFTER net_parking_amount"),
        ("permit_prorate",        "DECIMAL(12,2) NULL AFTER refund_date"),
        ("release_parking_amount","DECIMAL(10,2) NULL AFTER void_amount"),
        ("tax_exempt_flag",
         "ENUM('0','1','2','3','4','5','6','7','8','9') NULL "
         "COMMENT 'Mapped from tickets.paid_type; NULL for non-ticket sources' "
         "AFTER is_offline_payment"),
        ("sales_tax_exemption",
         "DECIMAL(12,2) NULL DEFAULT 0 AFTER tax_exempt_flag"),
        ("sales_tax_collected",
         "DECIMAL(12,2) NULL DEFAULT 0 AFTER sales_tax_exemption"),
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
        if not col_exists(tcur, TARGET_DB, "fact_payment", col_name):
            tcur.execute(f"ALTER TABLE {TARGET_DB}.fact_payment ADD COLUMN {col_name} {col_def}")
            tconn.commit()
            log.info("Added missing column fact_payment.%s", col_name)


def _normalize_payment_method_type(value, *, offline: bool = False) -> str:
    if offline:
        return "CASH"
    s = str(value or "").strip().lower()
    if not s:
        return "UNKNOWN"
    aliases = {
        "card": "CARD", "credit card": "CARD", "debit card": "CARD",
        "google pay": "Google Pay", "gpay": "Google Pay", "googlepay": "Google Pay",
        "apple pay": "Apple Pay", "applepay": "Apple Pay",
        "cash": "CASH", "offline": "CASH",
    }
    return aliases.get(s, str(value).strip()[:50] or "UNKNOWN")

def _approved_flag(status_str: str) -> int:
    return 1 if "approv" in str(status_str or "").lower() else 0

def _fetch_anet(scur, anet_ids: list) -> dict:
    if not anet_ids:
        return {}
    fmt = ",".join(["%s"] * len(anet_ids))
    scur.execute(f"""
        SELECT at.id, at.total, at.anet_trans_id, at.method, at.card_type,
               ast.status AS status_text, ast.category AS category_text,
               at.anet_type_id, at.created_at
        FROM {SOURCE_DB}.anet_transactions at
        LEFT JOIN {SOURCE_DB}.anet_statuses ast ON ast.id = at.anet_status_id
        WHERE at.id IN ({fmt})
    """, tuple(anet_ids))
    return {int(row[0]): row[1:] for row in scur.fetchall()}


def _build_payment_row(
    *,
    source_txn_id: Optional[int],
    anet,
    payment_ts_fallback,
    fac_key,
    canonical_session_key,
    reservation_key,
    event_key,
    permit_subscription_key,
    pass_subscription_key,
    amount_fallback: float,
    posted_gross_amount: float,
    base_parking_amount: float,
    net_parking_amount,
    discount_amount: float,
    validate_amount,
    sales_tax: float,
    processing_fees: float,
    city_surcharge,
    oversize_fees,
    cc_refund_amount: float,
    void_amount,
    release_parking_amount,
    permit_prorate,
    refund_date,
    is_offline_payment: int,
    tax_exempt_flag=None,
    # ── NEW: validation refund fields (None for all non-ticket sources) ───────
    validate_refund_amount=None,    # float | None
    vr_anet_trans_id=None,          # int | None
    vr_refund_status=None,          # str | None  — already validated against enum
    # ─────────────────────────────────────────────────────────────────────────
    pmm: dict = {},
    prc: dict = {},
) -> tuple:
    if anet is not None:
        (anet_total, proc_txn_id, method_raw, card_type,
         status_text, category_text, anet_type_id, anet_created_at) = anet
        payment_ts       = safe_dt(anet_created_at) or safe_dt(payment_ts_fallback)
        amount           = float(anet_total or 0)
        if str(category_text or "").strip().lower() == "refund":
            amount = -abs(amount)
        approved_flag    = _approved_flag(str(status_text or "") + str(category_text or ""))
        processor_txn_id = str(proc_txn_id) if proc_txn_id else None
        transaction_type = str(anet_type_id) if anet_type_id is not None else None
    else:
        payment_ts       = safe_dt(payment_ts_fallback)
        amount           = None
        approved_flag    = 0
        processor_txn_id = None
        transaction_type = None
        card_type        = None

    created_at = payment_ts or datetime.now()
    payment_method_key = pmm.get((prc.get("Authorize.Net"), _normalize_payment_method_type(
        None if anet is None else anet[2], offline=bool(is_offline_payment)
    ))) if prc.get("Authorize.Net") else None

    tef = str(tax_exempt_flag) if tax_exempt_flag is not None else None

    # Ensure validate_refund_amount is float or None
    vra = float(validate_refund_amount) if validate_refund_amount is not None else None

    return (
        source_txn_id,
        created_at,
        dk(created_at),
        fac_key,
        tk(created_at),
        payment_method_key,
        prc.get("Authorize.Net"),
        canonical_session_key,
        reservation_key,
        event_key,
        permit_subscription_key,
        pass_subscription_key,
        transaction_type,
        amount,
        approved_flag,
        card_type,
        processor_txn_id,
        None,               # reason_key
        sales_tax,
        created_at,         # transaction_date
        cc_refund_amount,
        city_surcharge,
        posted_gross_amount,
        discount_amount,
        base_parking_amount,
        validate_amount,
        void_amount,
        float(release_parking_amount) if release_parking_amount is not None else None,
        processing_fees,
        oversize_fees,
        float(net_parking_amount) if net_parking_amount is not None else None,
        safe_dt(refund_date),
        float(permit_prorate) if permit_prorate is not None else None,
        int(is_offline_payment),
        tef,                # tax_exempt_flag
        0.0,                # sales_tax_exemption
        0.0,                # sales_tax_collected
        vra,                # validate_refund_amount  [NEW]
        vr_anet_trans_id,   # vr_anet_trans_id        [NEW]
        vr_refund_status,   # vr_refund_status         [NEW]
        datetime.now(),     # created_at
    )


# 41 %s slots (38 previous + 3 new vr columns).
_FACT_PAYMENT_UPSERT_COLS = f"""
    INSERT INTO {TARGET_DB}.fact_payment (
        source_transaction_id,
        payment_ts_utc, date_key, facility_key, payment_time_key,
        payment_method_key, processor_key, canonical_session_key,
        reservation_key, event_key, permit_subscription_key, pass_subscription_key,
        transaction_type, amount, approved_flag,
        card_type, processor_txn_id, reason_key,
        sales_tax, transaction_date, cc_refund_amount, city_surcharge,
        posted_gross_amount, discount_amount, base_parking_amount,
        validate_amount, void_amount, release_parking_amount, processing_fees, oversize_fees,
        net_parking_amount, refund_date, permit_prorate,
        is_offline_payment,
        tax_exempt_flag, sales_tax_exemption, sales_tax_collected,
        validate_refund_amount, vr_anet_trans_id, vr_refund_status,
        created_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        payment_ts_utc          = IF(VALUES(amount)>0, VALUES(payment_ts_utc), payment_ts_utc),
        date_key                = IF(VALUES(amount)>0, VALUES(date_key), date_key),
        facility_key            = COALESCE(VALUES(facility_key), facility_key),
        payment_time_key        = IF(VALUES(amount)>0, VALUES(payment_time_key), payment_time_key),
        payment_method_key      = COALESCE(VALUES(payment_method_key), payment_method_key),
        processor_key           = COALESCE(VALUES(processor_key), processor_key),
        canonical_session_key   = COALESCE(VALUES(canonical_session_key), canonical_session_key),
        reservation_key         = COALESCE(VALUES(reservation_key), reservation_key),
        event_key               = COALESCE(VALUES(event_key), event_key),
        permit_subscription_key = COALESCE(VALUES(permit_subscription_key), permit_subscription_key),
        pass_subscription_key   = COALESCE(VALUES(pass_subscription_key), pass_subscription_key),
        transaction_type        = IF(VALUES(amount)>0, VALUES(transaction_type), transaction_type),
        amount                  = VALUES(amount),
        approved_flag           = VALUES(approved_flag),
        card_type               = COALESCE(VALUES(card_type), card_type),
        processor_txn_id        = COALESCE(VALUES(processor_txn_id), processor_txn_id),
        reason_key              = COALESCE(VALUES(reason_key), reason_key),
        sales_tax               = VALUES(sales_tax),
        transaction_date        = IF(VALUES(amount)>0, VALUES(transaction_date), transaction_date),
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
        net_parking_amount      = COALESCE(VALUES(net_parking_amount), net_parking_amount),
        refund_date             = COALESCE(VALUES(refund_date), refund_date),
        permit_prorate          = COALESCE(VALUES(permit_prorate), permit_prorate),
        is_offline_payment      = VALUES(is_offline_payment),
        tax_exempt_flag         = COALESCE(VALUES(tax_exempt_flag), tax_exempt_flag),
        sales_tax_exemption     = VALUES(sales_tax_exemption),
        sales_tax_collected     = VALUES(sales_tax_collected),
        validate_refund_amount  = COALESCE(VALUES(validate_refund_amount), validate_refund_amount),
        vr_anet_trans_id        = COALESCE(VALUES(vr_anet_trans_id), vr_anet_trans_id),
        vr_refund_status        = COALESCE(VALUES(vr_refund_status), vr_refund_status)
"""


def load_fact_payment(scur, tcur, tconn, *, start_after: int = 0) -> tuple[int, int]:
    ensure_fact_payment(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_payment")

    mc  = MapCache(tcur)
    fac = mc.load("fac", f"SELECT facility_key, facility_id FROM {TARGET_DB}.dim_facility WHERE is_current=1")

    tcur.execute(f"SELECT payment_method_key, payment_method_id, method_type FROM {TARGET_DB}.dim_payment_method")
    pmm = {
        (int(r[1]), str(r[2])): int(r[0])
        for r in tcur.fetchall() if r[1] is not None and r[2] is not None
    }

    tcur.execute(f"SELECT processor_key, processor_name, provider FROM {TARGET_DB}.dim_processor")
    prc: dict[str, int] = {}
    for processor_key, processor_name, provider in tcur.fetchall():
        if processor_name is not None and str(processor_name) not in prc:
            prc[str(processor_name)] = int(processor_key)
        if provider is not None and str(provider) not in prc:
            prc[str(provider)] = int(processor_key)

    fps_ticket   = mc.load("fps_0",
        f"SELECT canonical_session_key, source_id FROM {TARGET_DB}.fact_parking_session "
        f"WHERE extension_overstay_flag=0")
    fps_overstay = mc.load("fps_1",
        f"SELECT canonical_session_key, source_id FROM {TARGET_DB}.fact_parking_session "
        f"WHERE extension_overstay_flag=1")
    fps_extend   = mc.load("fps_2",
        f"SELECT canonical_session_key, source_id FROM {TARGET_DB}.fact_parking_session "
        f"WHERE extension_overstay_flag=2")

    resv     = mc.load("resv",     f"SELECT reservation_key, source_reservation_id FROM {TARGET_DB}.fact_reservation")
    perm     = mc.load("perm",     f"SELECT permit_subscription_key, source_permit_id FROM {TARGET_DB}.fact_permit_subscription")
    pass_sub = mc.load("pass_sub",
        f"SELECT pass_subscription_key, source_user_pass_id "
        f"FROM {TARGET_DB}.fact_passes WHERE source_user_pass_id IS NOT NULL")
    if not pass_sub:
        log.warning("fact_payment: pass_sub map is empty — fact_passes may not be loaded")

    # Reverse-lookup maps (all tickets, including soft-deleted)
    log.info("fact_payment: building reverse session maps …")
    scur.execute(f"SELECT reservation_id, id FROM {SOURCE_DB}.tickets WHERE reservation_id IS NOT NULL AND deleted_at IS NULL")
    sess_by_resv = {int(r[0]): fps_ticket.get(int(r[1])) for r in scur.fetchall() if r[1] is not None}

    scur.execute(f"SELECT permit_request_id, id FROM {SOURCE_DB}.tickets WHERE permit_request_id IS NOT NULL AND deleted_at IS NULL")
    sess_by_perm = {int(r[0]): fps_ticket.get(int(r[1])) for r in scur.fetchall() if r[1] is not None}

    scur.execute(f"SELECT user_pass_id, id FROM {SOURCE_DB}.tickets WHERE user_pass_id IS NOT NULL AND deleted_at IS NULL")
    sess_by_pass = {int(r[0]): fps_ticket.get(int(r[1])) for r in scur.fetchall() if r[1] is not None}

    # ── NEW: load validation_refunds detail map for SOURCE 1 (tickets) ───────
    vr_map: dict = load_vr_map(scur)

    upsert_sql    = _FACT_PAYMENT_UPSERT_COLS
    rows_read     = rows_inserted = 0

    # ── SOURCE 1: tickets ────────────────────────────────────────────────────
    log.info("fact_payment – SOURCE 1: tickets")
    last_pk_tickets = start_after
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.tickets", pk_col="id",
            cols=("id, facility_id, anet_transaction_id, is_offline_payment, created_at,"
                  " grand_total, parking_amount, net_parking_amount, discount_amount,"
                  " paid_amount, tax_fee, processing_fee, additional_fee,"
                  " surcharge_fee, oversize_fee, refund_amount, release_amount,"
                  " reservation_id, permit_request_id, user_pass_id, refund_date,"
                  " release_parking_amount, paid_type, ticket_number"),
            where="deleted_at IS NULL", start_after=last_pk_tickets):
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        out = []
        for r in rows:
            (tick_id, fac_id, anet_txn_id, is_offline, cre_at,
             grand_total, parking_amt, net_parking_amt, discount,
             paid_amount, tax_fee, proc_fee, add_fee,
             surcharge_fee, oversize_fee, refund_amount, release_amount,
             reservation_id, permit_request_id, user_pass_id, refund_date,
             release_parking_amt, paid_type, ticket_number) = r

            anet       = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id = int(anet_txn_id) if anet_txn_id else None

            # NEW: look up validation refund data by ticket_number
            vr_data = vr_map.get(ticket_number) if ticket_number is not None else None
            vra, vr_anet, vr_status = vr_data if vr_data is not None else (None, None, None)

            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = fps_ticket.get(int(tick_id)),
                reservation_key         = resv.get(int(reservation_id)) if reservation_id else None,
                event_key               = None,
                permit_subscription_key = perm.get(int(permit_request_id)) if permit_request_id else None,
                pass_subscription_key   = None,
                amount_fallback         = float(paid_amount or grand_total or 0),
                posted_gross_amount     = float(grand_total    or 0),
                base_parking_amount     = float(parking_amt    or 0),
                net_parking_amount      = float(net_parking_amt or 0),
                discount_amount         = float(discount       or 0),
                validate_amount         = float(paid_amount    or 0),
                sales_tax               = float(tax_fee        or 0),
                processing_fees         = float(proc_fee or 0) + float(add_fee or 0),
                city_surcharge          = float(surcharge_fee  or 0),
                oversize_fees           = float(oversize_fee   or 0),
                cc_refund_amount        = float(refund_amount  or 0),
                void_amount             = float(release_amount or 0),
                release_parking_amount  = float(release_parking_amt or 0),
                permit_prorate          = None,
                refund_date             = refund_date,
                is_offline_payment      = int(is_offline or 0),
                tax_exempt_flag         = str(paid_type) if paid_type is not None else None,
                validate_refund_amount  = vra,      # NEW
                vr_anet_trans_id        = vr_anet,  # NEW
                vr_refund_status        = vr_status, # NEW
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_tickets = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_payment", last_pk=last_pk_tickets,
                      status="RUNNING", rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("  tickets pk<=%s  read=%s inserted=%s", last_pk_tickets, rows_read, rows_inserted)

    # ── SOURCE 2: reservations ───────────────────────────────────────────────
    # validate_refund_amount / vr_anet_trans_id / vr_refund_status stay NULL
    log.info("fact_payment – SOURCE 2: reservations")
    evt = mc.load("evt", f"SELECT event_key, event_id FROM {TARGET_DB}.dim_event")
    last_pk_resv = 0
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.reservations", pk_col="id",
            cols=("id, facility_id, anet_transaction_id, created_at,"
                  " total, parking_amount, reservation_amount, discount,"
                  " tax_fee, processing_fee, refund_amount, event_id,"
                  " net_parking_amount, refund_date"),
            where="deleted_at IS NULL", start_after=last_pk_resv):
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        out = []
        for r in rows:
            (rsv_id, fac_id, anet_txn_id, cre_at,
             total, parking_amt, reservation_amt, discount,
             tax_fee, proc_fee, refund_amount, event_id,
             net_parking_amt, refund_date) = r
            anet       = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id = int(anet_txn_id) if anet_txn_id else None
            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = sess_by_resv.get(int(rsv_id)),
                reservation_key         = resv.get(int(rsv_id)),
                event_key               = evt.get(event_id) if event_id else None,
                permit_subscription_key = None,
                pass_subscription_key   = None,
                amount_fallback         = float(total or 0),
                posted_gross_amount     = float(total or 0),
                base_parking_amount     = float(parking_amt or reservation_amt or 0),
                net_parking_amount      = float(net_parking_amt or 0),
                discount_amount         = float(discount    or 0),
                validate_amount         = None,
                sales_tax               = float(tax_fee     or 0),
                processing_fees         = float(proc_fee    or 0),
                city_surcharge          = None, oversize_fees=None,
                cc_refund_amount        = float(refund_amount or 0),
                void_amount=None, release_parking_amount=None, permit_prorate=None,
                refund_date             = refund_date,
                is_offline_payment      = 0,
                tax_exempt_flag=None,
                validate_refund_amount=None, vr_anet_trans_id=None, vr_refund_status=None,
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_resv = int(rows[-1][0])
        log.info("  reservations pk<=%s  read=%s inserted=%s", last_pk_resv, rows_read, rows_inserted)

    # ── SOURCE 3: permit_requests ────────────────────────────────────────────
    log.info("fact_payment – SOURCE 3: permit_requests")
    last_pk_perm = 0
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.permit_requests", pk_col="id",
            cols=("id, facility_id, anet_transaction_id, created_at,"
                  " permit_final_amount, permit_rate, net_parking_amount, discount_amount,"
                  " tax_fee, processing_fee, additional_fee, surcharge_fee,"
                  " refund_amount, permit_prorate, refund_date"),
            where="deleted_at IS NULL", start_after=last_pk_perm):
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        out = []
        for r in rows:
            (pr_id, fac_id, anet_txn_id, cre_at,
             permit_final_amt, permit_rate, net_parking_amt, discount,
             tax_fee, proc_fee, add_fee, surcharge_fee,
             refund_amount, permit_prorate, refund_date) = r
            anet       = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id = int(anet_txn_id) if anet_txn_id else None
            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = sess_by_perm.get(int(pr_id)),
                reservation_key=None, event_key=None,
                permit_subscription_key = perm.get(int(pr_id)),
                pass_subscription_key   = None,
                amount_fallback         = float(permit_final_amt or 0),
                posted_gross_amount     = float(permit_final_amt or 0),
                base_parking_amount     = float(permit_rate      or 0),
                net_parking_amount      = float(net_parking_amt  or 0),
                discount_amount         = float(discount         or 0),
                validate_amount=None,
                sales_tax               = float(tax_fee          or 0),
                processing_fees         = float(proc_fee or 0) + float(add_fee or 0),
                city_surcharge          = float(surcharge_fee    or 0),
                oversize_fees=None,
                cc_refund_amount        = float(refund_amount    or 0),
                void_amount=None, release_parking_amount=None,
                permit_prorate          = float(permit_prorate   or 0),
                refund_date             = refund_date,
                is_offline_payment      = 0,
                tax_exempt_flag=None,
                validate_refund_amount=None, vr_anet_trans_id=None, vr_refund_status=None,
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_perm = int(rows[-1][0])
        log.info("  permit_requests pk<=%s  read=%s inserted=%s", last_pk_perm, rows_read, rows_inserted)

    # ── SOURCE 4: user_passes ────────────────────────────────────────────────
    log.info("fact_payment – SOURCE 4: user_passes")
    last_pk_pass = 0
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.user_passes", pk_col="id",
            cols=("id, facility_id, anet_transaction_id, created_at,"
                  " total, parking_amount, discount_amount,"
                  " tax_fee, processing_fee, refund_amount,"
                  " net_parking_amount, refund_date"),
            where="deleted_at IS NULL", start_after=last_pk_pass):
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        out = []
        for r in rows:
            (up_id, fac_id, anet_txn_id, cre_at,
             total, parking_amt, discount,
             tax_fee, proc_fee, refund_amount,
             net_parking_amt, refund_date) = r
            anet       = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id = int(anet_txn_id) if anet_txn_id else None
            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = sess_by_pass.get(int(up_id)),
                reservation_key=None, event_key=None, permit_subscription_key=None,
                pass_subscription_key   = pass_sub.get(int(up_id)),
                amount_fallback         = float(total or 0),
                posted_gross_amount     = float(total       or 0),
                base_parking_amount     = float(parking_amt or 0),
                net_parking_amount      = float(net_parking_amt or 0),
                discount_amount         = float(discount    or 0),
                validate_amount=None,
                sales_tax               = float(tax_fee     or 0),
                processing_fees         = float(proc_fee    or 0),
                city_surcharge=None, oversize_fees=None,
                cc_refund_amount        = float(refund_amount or 0),
                void_amount=None, release_parking_amount=None, permit_prorate=None,
                refund_date             = refund_date,
                is_offline_payment      = 0,
                tax_exempt_flag=None,
                validate_refund_amount=None, vr_anet_trans_id=None, vr_refund_status=None,
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_pass = int(rows[-1][0])
        log.info("  user_passes pk<=%s  read=%s inserted=%s", last_pk_pass, rows_read, rows_inserted)

    # ── SOURCE 5: overstay_tickets ───────────────────────────────────────────
    log.info("fact_payment – SOURCE 5: overstay_tickets")
    last_pk_ov = 0
    _ov_add_fee_expr   = "additional_fee" if col_exists(scur, SOURCE_DB, "overstay_tickets", "additional_fee") else "0"
    _ov_surcharge_expr = "surcharge_fee"  if col_exists(scur, SOURCE_DB, "overstay_tickets", "surcharge_fee")  else "0"
    while True:
        scur.execute(f"""
            SELECT id, facility_id, anet_transaction_id,
                   is_offline_payment, payment_date, created_at,
                   grand_total, parking_amount, discount_amount,
                   tax_fee, processing_fee,
                   {_ov_add_fee_expr}   AS additional_fee,
                   {_ov_surcharge_expr} AS surcharge_fee,
                   penalty_fee, reservation_id
            FROM {SOURCE_DB}.overstay_tickets
            WHERE id > %s ORDER BY id LIMIT %s
        """, (last_pk_ov, BATCH_SIZE))
        rows = scur.fetchall()
        if not rows:
            break
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        out = []
        for r in rows:
            (ov_id, fac_id, anet_txn_id, is_offline,
             payment_date, cre_at,
             grand_total, parking_amt, discount,
             tax_fee, proc_fee, add_fee, surcharge_fee, penalty_fee,
             reservation_id) = r
            anet       = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id = int(anet_txn_id) if anet_txn_id else None
            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = payment_date or cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = fps_overstay.get(int(ov_id)),
                reservation_key         = resv.get(int(reservation_id)) if reservation_id else None,
                event_key=None, permit_subscription_key=None, pass_subscription_key=None,
                amount_fallback         = float(grand_total or 0),
                posted_gross_amount     = float(grand_total  or 0),
                base_parking_amount     = float(parking_amt  or 0),
                net_parking_amount      = None,
                discount_amount         = float(discount     or 0),
                validate_amount=None,
                sales_tax               = float(tax_fee      or 0),
                processing_fees         = float(proc_fee or 0) + float(add_fee or 0),
                city_surcharge          = float(surcharge_fee or 0),
                oversize_fees           = float(penalty_fee   or 0),
                cc_refund_amount        = 0.0,
                void_amount=None, release_parking_amount=None, permit_prorate=None,
                refund_date=None,
                is_offline_payment      = int(is_offline or 0),
                tax_exempt_flag=None,
                validate_refund_amount=None, vr_anet_trans_id=None, vr_refund_status=None,
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_ov = int(rows[-1][0])
        log.info("  overstay_tickets pk<=%s  read=%s inserted=%s",
                 last_pk_ov, rows_read, rows_inserted)

    # ── SOURCE 6: ticket_extends ─────────────────────────────────────────────
    log.info("fact_payment – SOURCE 6: ticket_extends")
    last_pk_ext = 0
    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.ticket_extends", pk_col="id",
            cols=("id, facility_id, anet_transaction_id, created_at,"
                  " grand_total, parking_amounts, discount_amount,"
                  " tax_fee, processing_fee, additional_fee, surcharge_fee,"
                  " oversize_fee, refund_amount, net_parking_amount, ticket_id"),
            where="deleted_at IS NULL", start_after=last_pk_ext):
        anet_ids = [int(r[2]) for r in rows if r[2] is not None]
        anet_map = _fetch_anet(scur, anet_ids)
        ticket_ids = [int(r[14]) for r in rows if r[14] is not None]
        parent_rsv_map: dict[int, Optional[int]] = {}
        if ticket_ids:
            fmt2 = ",".join(["%s"] * len(ticket_ids))
            scur.execute(
                f"SELECT id, reservation_id FROM {SOURCE_DB}.tickets WHERE id IN ({fmt2})",
                tuple(ticket_ids))
            for pr in scur.fetchall():
                parent_rsv_map[int(pr[0])] = int(pr[1]) if pr[1] is not None else None
        out = []
        for r in rows:
            (ext_id, fac_id, anet_txn_id, cre_at,
             grand_total, parking_amts, discount,
             tax_fee, proc_fee, add_fee, surcharge_fee,
             oversize_fee, refund_amount, net_parking_amt, t_id) = r
            anet          = anet_map.get(int(anet_txn_id)) if anet_txn_id else None
            src_txn_id    = int(anet_txn_id) if anet_txn_id else None
            parent_rsv_id = parent_rsv_map.get(int(t_id)) if t_id else None
            out.append(_build_payment_row(
                source_txn_id           = src_txn_id,
                anet                    = anet,
                payment_ts_fallback     = cre_at,
                fac_key                 = fac.get(fac_id),
                canonical_session_key   = fps_extend.get(int(ext_id)),
                reservation_key         = resv.get(parent_rsv_id) if parent_rsv_id else None,
                event_key=None, permit_subscription_key=None, pass_subscription_key=None,
                amount_fallback         = float(grand_total or 0),
                posted_gross_amount     = float(grand_total    or 0),
                base_parking_amount     = float(parking_amts   or 0),
                net_parking_amount      = float(net_parking_amt or 0),
                discount_amount         = float(discount        or 0),
                validate_amount=None,
                sales_tax               = float(tax_fee         or 0),
                processing_fees         = float(proc_fee or 0) + float(add_fee or 0),
                city_surcharge          = float(surcharge_fee   or 0),
                oversize_fees           = float(oversize_fee    or 0),
                cc_refund_amount        = float(refund_amount   or 0),
                void_amount=None, release_parking_amount=None, permit_prorate=None,
                refund_date=None,
                is_offline_payment      = 0,
                tax_exempt_flag=None,
                validate_refund_amount=None, vr_anet_trans_id=None, vr_refund_status=None,
                pmm=pmm, prc=prc,
            ))
        if out:
            rows_inserted += bulk_insert(tcur, tconn, upsert_sql, out)
        rows_read += len(rows)
        last_pk_ext = int(rows[-1][0])
        log.info("  ticket_extends pk<=%s  read=%s inserted=%s",
                 last_pk_ext, rows_read, rows_inserted)

    set_watermark(tcur, tconn, "fact_payment",
                  last_pk=max(last_pk_tickets, last_pk_resv,
                              last_pk_perm, last_pk_pass,
                              last_pk_ov, last_pk_ext),
                  status="RUNNING", rows_read=rows_read, rows_inserted=rows_inserted)
    return rows_read, rows_inserted


"""
INITIAL LOAD ADDITION — fact_payment_sweep_transactions
Add these two functions (ensure_ + load_) to the main ETL script alongside
the other fact loaders, and call them from main() in dependency order
(after fact_payment, since we JOIN to it for payment_key / canonical_session_key).

Column mapping (source → target):
  pst.id                  → source_sweep_id   (natural key / dedup)
  pst.facility_id         → facility_key       (via dim_facility)
  pst.transaction_at      → start_date_key     (dim_date, date part)
  pst.funded_at           → end_date_key       (dim_date, date part)
  pst.transaction_at      → start_time_key     (dim_time, time part)
  pst.funded_at           → end_time_key       (dim_time, time part)
  pst.partner_id          → partner_account_key(via dim_partner_account)
  pst.transaction_id      → canonical_session_key
                            payment_method_key
                            payment_key
                            (all resolved via fact_payment.processor_txn_id
                             → fact_payment.canonical_session_key /
                               fact_payment.payment_method_key /
                               fact_payment.payment_key)
  pst.cc_base_share       → cc_base_fees
  pst.cc_variable_share   → cc_variable_fees
  pst.pe_base_share       → pe_base_transaction_fees
  pst.pe_variable_share   → pe_variable_transaction_fees
  pst.partner_base_share  → partner_base_fees
  pst.partner_variable_share → partner_variable_fees
  pst.account_number      → account_number
  pst.sweep_batch         → sweep_batch
  pst.service_type        → service_type
"""


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def ensure_fact_payment_sweep_transactions(tcur, tconn) -> None:
    tcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DB}.fact_payment_sweep_transactions (
            payment_sweep_key          BIGINT        AUTO_INCREMENT PRIMARY KEY,
            source_sweep_id            BIGINT        NOT NULL
                COMMENT 'payment_sweep_transactions.id from source',
            facility_key               BIGINT        NULL,
            start_date_key             INT           NULL
                COMMENT 'dim_date key for transaction_at date',
            end_date_key               INT           NULL
                COMMENT 'dim_date key for funded_at date',
            start_time_key             INT           NULL
                COMMENT 'dim_time key for transaction_at time',
            end_time_key               INT           NULL
                COMMENT 'dim_time key for funded_at time',
            partner_account_key        BIGINT        NULL,
            canonical_session_key      BIGINT        NULL,
            payment_method_key         BIGINT        NULL,
            payment_key                BIGINT        NULL,
            cc_base_fees               DECIMAL(12,2) NULL,
            cc_variable_fees           DECIMAL(12,2) NULL,
            pe_base_transaction_fees   DECIMAL(12,2) NULL,
            pe_variable_transaction_fees DECIMAL(12,2) NULL,
            partner_base_fees          DECIMAL(12,2) NULL,
            partner_variable_fees      DECIMAL(12,2) NULL,
            account_number             VARCHAR(50)   NULL,
            sweep_batch                VARCHAR(50)   NULL,
            service_type               VARCHAR(255)  NULL,
            created_at                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
            updated_at                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_source_sweep_id (source_sweep_id),
            INDEX idx_fpst_facility        (facility_key),
            INDEX idx_fpst_partner         (partner_account_key),
            INDEX idx_fpst_session         (canonical_session_key),
            INDEX idx_fpst_payment         (payment_key),
            INDEX idx_fpst_start_date      (start_date_key),
            INDEX idx_fpst_sweep_batch     (sweep_batch)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Fact table for payment sweep transactions (CC settlement data)'
    """)
    tconn.commit()


# ---------------------------------------------------------------------------
# LOADER
# ---------------------------------------------------------------------------

def load_fact_payment_sweep_transactions(
        scur, tcur, tconn, *, start_after: int = 0
) -> tuple[int, int]:
    """
    Loads payment_sweep_transactions from the source DB into
    fact_payment_sweep_transactions in the target DW.

    Resolution chain:
      pst.transaction_id
        → fact_payment.processor_txn_id   (target lookup)
        → fact_payment.payment_key         (stored directly)
        → fact_payment.canonical_session_key  (stored directly)
        → fact_payment.payment_method_key  (stored directly)

    All dim lookups use MapCache for efficiency.
    """
    ensure_fact_payment_sweep_transactions(tcur, tconn)
    if FULL_RELOAD:
        truncate(tcur, tconn, "fact_payment_sweep_transactions")

    # ── Gold-side dimension/fact maps ────────────────────────────────────────
    mc  = MapCache(tcur)
    fac = mc.load("fac", f"SELECT facility_key, facility_id "
                          f"FROM {TARGET_DB}.dim_facility WHERE is_current=1")
    acc = mc.load("acc", f"SELECT partner_account_key, account_id_source "
                          f"FROM {TARGET_DB}.dim_partner_account WHERE is_current=1")

    # Build lookup: processor_txn_id → (payment_key, canonical_session_key, payment_method_key)
    # processor_txn_id in fact_payment stores the anet_trans_id / external txn id,
    # which equals payment_sweep_transactions.transaction_id.
    log.info("fact_payment_sweep_transactions: building processor_txn_id lookup map …")
    tcur.execute(f"""
        SELECT processor_txn_id, payment_key, canonical_session_key, payment_method_key
        FROM {TARGET_DB}.fact_payment
        WHERE processor_txn_id IS NOT NULL
    """)
    payment_map: dict[str, tuple] = {}
    for row in tcur.fetchall():
        txn_id = row[0]
        if txn_id and txn_id not in payment_map:
            payment_map[txn_id] = (row[1], row[2], row[3])   # (payment_key, canon_key, pm_key)
    log.info("fact_payment_sweep_transactions: payment_map has %s entries", len(payment_map))

    rows_read = rows_inserted = 0
    last_pk   = start_after

    _UPSERT = f"""
        INSERT INTO {TARGET_DB}.fact_payment_sweep_transactions (
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
            account_number, sweep_batch, service_type
        ) VALUES (
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            facility_key               = COALESCE(VALUES(facility_key),          facility_key),
            start_date_key             = COALESCE(VALUES(start_date_key),         start_date_key),
            end_date_key               = COALESCE(VALUES(end_date_key),           end_date_key),
            start_time_key             = COALESCE(VALUES(start_time_key),         start_time_key),
            end_time_key               = COALESCE(VALUES(end_time_key),           end_time_key),
            partner_account_key        = COALESCE(VALUES(partner_account_key),    partner_account_key),
            canonical_session_key      = COALESCE(VALUES(canonical_session_key),  canonical_session_key),
            payment_method_key         = COALESCE(VALUES(payment_method_key),     payment_method_key),
            payment_key                = COALESCE(VALUES(payment_key),            payment_key),
            cc_base_fees               = VALUES(cc_base_fees),
            cc_variable_fees           = VALUES(cc_variable_fees),
            pe_base_transaction_fees   = VALUES(pe_base_transaction_fees),
            pe_variable_transaction_fees = VALUES(pe_variable_transaction_fees),
            partner_base_fees          = VALUES(partner_base_fees),
            partner_variable_fees      = VALUES(partner_variable_fees),
            account_number             = VALUES(account_number),
            sweep_batch                = VALUES(sweep_batch),
            service_type               = VALUES(service_type)
    """

    for rows in keyset_batches(scur,
            table=f"{SOURCE_DB}.payment_sweep_transactions",
            pk_col="id",
            cols=("id, facility_id, partner_id, transaction_id,"
                  " transaction_at, funded_at,"
                  " cc_base_share, cc_variable_share,"
                  " pe_base_share, pe_variable_share,"
                  " partner_base_share, partner_variable_share,"
                  " account_number, sweep_batch, service_type"),
            where="1=1",
            start_after=last_pk):

        out = []
        for r in rows:
            (src_id, fac_id, partner_id, transaction_id,
             transaction_at, funded_at,
             cc_base, cc_var,
             pe_base, pe_var,
             pb_base, pb_var,
             account_number, sweep_batch, service_type) = r

            # Resolve timestamps
            txn_dt   = safe_dt(transaction_at)
            fund_dt  = safe_dt(funded_at)

            # Resolve dim keys
            facility_key        = fac.get(fac_id)
            partner_account_key = acc.get(partner_id)
            start_date_key      = dk(txn_dt)
            end_date_key        = dk(fund_dt)
            start_time_key      = tk(txn_dt)
            end_time_key        = tk(fund_dt)

            # Resolve payment-side keys via transaction_id → fact_payment.processor_txn_id
            pm_data            = payment_map.get(str(transaction_id)) if transaction_id else None
            payment_key        = pm_data[0] if pm_data else None
            canonical_session_key = pm_data[1] if pm_data else None
            payment_method_key = pm_data[2] if pm_data else None

            out.append((
                int(src_id),
                facility_key,
                start_date_key, end_date_key,
                start_time_key, end_time_key,
                partner_account_key,
                canonical_session_key,
                payment_method_key,
                payment_key,
                safe_decimal(cc_base),  safe_decimal(cc_var),
                safe_decimal(pe_base),  safe_decimal(pe_var),
                safe_decimal(pb_base),  safe_decimal(pb_var),
                str(account_number)[:50]  if account_number  else None,
                str(sweep_batch)[:50]     if sweep_batch      else None,
                str(service_type)[:255]   if service_type     else None,
            ))

        inserted = bulk_insert(tcur, tconn, _UPSERT, out)
        rows_read    += len(rows)
        rows_inserted += inserted
        last_pk       = int(rows[-1][0])
        set_watermark(tcur, tconn, "fact_payment_sweep_transactions",
                      last_pk=last_pk, status="RUNNING",
                      rows_read=rows_read, rows_inserted=rows_inserted)
        log.info("fact_payment_sweep_transactions pk<=%s read=%s inserted=%s",
                 last_pk, rows_read, rows_inserted)

    return rows_read, rows_inserted


# ---------------------------------------------------------------------------
# ADD TO main() — paste inside the "Loading facts" block, after fact_payment:
# ---------------------------------------------------------------------------
# run_load("fact_payment_sweep_transactions",
#          load_fact_payment_sweep_transactions, tcur, target, scur,
#          start_after=get_watermark(tcur, "fact_payment_sweep_transactions")
#                     if not FULL_RELOAD else 0)

# ===========================================================================
# ORCHESTRATION
# ===========================================================================
def run_load(name: str, loader_fn, tcur, tconn,
             scur=None, *, start_after: int = 0) -> None:
    if table_exists(tcur, TARGET_DB, name):
        tcur.execute(f"SELECT COUNT(*) FROM {TARGET_DB}.{name}")
        count = int(tcur.fetchone()[0])
        if count > 0:
            log.info("⏭️  %-35s already exists (%s rows) – skipping", name, count)
            return
        log.info("⚠️  %-35s exists but is empty – reloading", name)
    ensure_etl_row(tcur, tconn, name)
    run_id = start_run(tcur, tconn, name)
    rows_read = rows_inserted = 0
    last_pk = start_after
    try:
        if scur is not None:
            result = loader_fn(scur, tcur, tconn, start_after=start_after)
        else:
            result = loader_fn(tcur, tconn)
        if isinstance(result, tuple):
            rows_read, rows_inserted = result
        elif isinstance(result, int):
            last_pk = result
        finish_run(tcur, tconn, run_id, status="SUCCESS",
                   rows_read=rows_read, rows_inserted=rows_inserted,
                   last_pk=get_watermark(tcur, name), message="OK")
        set_watermark(tcur, tconn, name, last_pk=get_watermark(tcur, name),
                      status="SUCCESS", rows_read=rows_read,
                      rows_inserted=rows_inserted, message="OK")
        log.info("✅ %s DONE  read=%s inserted=%s", name, rows_read, rows_inserted)
    except Exception as exc:
        wm = 0
        try:
            wm = get_watermark(tcur, name)
        except Exception:
            pass
        finish_run(tcur, tconn, run_id, status="FAILED",
                   rows_read=rows_read, rows_inserted=rows_inserted,
                   last_pk=wm, message=str(exc))
        set_watermark(tcur, tconn, name, last_pk=wm, status="FAILED",
                      rows_read=rows_read, rows_inserted=rows_inserted,
                      message=str(exc)[:500])
        log.exception("❌ %s FAILED: %s", name, exc)
        raise

def main() -> None:
    t0 = time.time()
    log.info("=" * 60)
    log.info("Enterprise ETL start | FULL_RELOAD=%s | BATCH_SIZE=%s", FULL_RELOAD, BATCH_SIZE)
    log.info("SOURCE=%s  TARGET=%s", SOURCE_DB, TARGET_DB)
    log.info("=" * 60)
    source = connect(SOURCE_CONFIG)
    target = connect(TARGET_CONFIG)
    scur   = source.cursor()
    tcur   = target.cursor()
    try:
        ensure_control_tables(tcur, target)
        acquire_lock(tcur, target)
        assert_read_only(source)
        lag = get_replica_lag(source)
        if lag is not None and lag > REPLICA_MAX_LAG_SECONDS:
            raise RuntimeError(f"Replica lag {lag}s > limit {REPLICA_MAX_LAG_SECONDS}s")
        scur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        tcur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        tcur.execute("SET SESSION sql_safe_updates=0")
        tcur.execute("SET SESSION foreign_key_checks=0")
        tcur.execute("SET SESSION unique_checks=0")
        log.info("── Loading dimensions ──────────────────────────────")
        run_load("dim_date",           load_dim_date,           tcur, target)
        run_load("dim_time",           load_dim_time,           tcur, target)
        run_load("dim_partner_account",load_dim_partner_account,tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_partner_account") if not FULL_RELOAD else 0)
        run_load("dim_parker",         load_dim_parker,         tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_parker") if not FULL_RELOAD else 0)
        run_load("dim_facility",       load_dim_facility,       tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_facility") if not FULL_RELOAD else 0)
        run_load("dim_vehicle",        load_dim_vehicle,        tcur, target, scur)
        run_load("dim_device",         load_dim_device,         tcur, target, scur)
        run_load("dim_rateplan",       load_dim_rateplan,       tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_rateplan") if not FULL_RELOAD else 0)
        run_load("dim_parking_product",load_dim_parking_product,tcur, target, scur)
        run_load("dim_permit_plan",    load_dim_permit_plan,    tcur, target, scur)
        run_load("dim_pass",           load_dim_pass,           tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_pass") if not FULL_RELOAD else 0)
        run_load("dim_promo_code",     load_dim_promo_code,     tcur, target, scur,
                 start_after=get_watermark(tcur,"dim_promo_code") if not FULL_RELOAD else 0)
        run_load("dim_processor",      load_dim_processor,      tcur, target, scur)
        run_load("dim_payment_method", load_dim_payment_method, tcur, target, scur)
        run_load("dim_event",          load_dim_event,          tcur, target, scur)
        run_load("dim_reason",         load_dim_reason,         tcur, target, scur)
        run_load("dim_source_system",  load_dim_source_system,  tcur, target, scur)
        run_load("dim_policy",         load_dim_policy,         tcur, target, scur)
        log.info("── Loading facts ───────────────────────────────────")
        run_load("fact_reservation",   load_fact_reservation,   tcur, target, scur,
                 start_after=get_watermark(tcur,"fact_reservation") if not FULL_RELOAD else 0)
        run_load("fact_permit_subscription",load_fact_permit_subscription,tcur,target,scur,
                 start_after=get_watermark(tcur,"fact_permit_subscription") if not FULL_RELOAD else 0)
        run_load("fact_passes",        load_fact_passes,        tcur, target, scur,
                 start_after=get_watermark(tcur,"fact_passes") if not FULL_RELOAD else 0)
        run_load("fact_parking_session",load_fact_parking_session,tcur,target,scur,
                 start_after=get_watermark(tcur,"fact_parking_session") if not FULL_RELOAD else 0)
        run_load("fact_validation_redemption",load_fact_validation_redemption,tcur,target,scur,
                 start_after=get_watermark(tcur,"fact_validation_redemption") if not FULL_RELOAD else 0)
        run_load("fact_payment",       load_fact_payment,       tcur, target, scur,
                 start_after=get_watermark(tcur,"fact_payment") if not FULL_RELOAD else 0)
        run_load("fact_payment_sweep_transactions",
         load_fact_payment_sweep_transactions, tcur, target, scur,
         start_after=get_watermark(tcur, "fact_payment_sweep_transactions")
                    if not FULL_RELOAD else 0)
        log.info("=" * 60)
        log.info("✅ ETL complete in %s", fmt_dur(time.time() - t0))
        log.info("=" * 60)
    except Exception as exc:
        target.rollback()
        log.exception("ETL aborted: %s", exc)
        sys.exit(1)
    finally:
        release_lock(tcur, target)
        for c in (scur, tcur):
            try:
                c.close()
            except Exception:
                pass
        for cn in (source, target):
            try:
                cn.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()