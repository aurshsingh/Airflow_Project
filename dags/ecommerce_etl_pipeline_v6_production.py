"""
══════════════════════════════════════════════════════════════════════════════
 E-COMMERCE ETL PIPELINE  -  PRODUCTION VERSION (v6)          Apache Airflow 3.x
══════════════════════════════════════════════════════════════════════════════

 DAG ID : ecommerce_etl_pipeline_v6
 Source : Olist CSVs (orders + customers)
 Target : MySQL  ->  fact_orders (+ stg_orders, etl_run_audit, etl_rejected_orders)

 FLOW
 ----
   wait_for_orders_file ─┐
   wait_for_customers_file ─┼─> extract -> transform -> validate ─┬─> stage_delta ─> choose_load_strategy
   ensure_tables ─────────┘                                       │        ├─> no_changes
                                                                   │        ├─> upsert_standard
                                                                   │        └─> upsert_batched
                                                                   └─> load_rejected          │
                                                                              └──> finalize_load <┘
                                                                                      │
                                                                                 write_audit
                                                                                  ├─> cleanup_staging
                                                                                  └─> pipeline_summary

 WHAT CHANGED FROM v5 (and why)
 ------------------------------
 1. IDEMPOTENT LOAD. v5 used if_exists="append" with no primary key, so a retry
    after a partial write inserted duplicates. v6 loads into a staging table and
    UPSERTs into fact_orders (PRIMARY KEY order_id). Re-running any task, or the
    whole run, is safe.
 2. REAL INCREMENTAL / CDC LOAD. Each row carries an MD5 `row_hash`. Rows whose
    hash already exists in fact_orders are dropped from staging, so only NEW or
    CHANGED rows are written. This also picks up late-arriving orders and status
    changes (shipped -> delivered), which v5's "timestamp > max" filter missed.
 3. EXPLICIT SCHEMA. Tables are created with proper types (DATETIME, DATE,
    VARCHAR(32) keys, indexes) instead of letting pandas guess TEXT columns.
 4. QUARANTINE, NOT SILENT DROPS. Bad rows go to etl_rejected_orders with a
    reject_reason, so data loss is visible and auditable.
 5. RUN-SCOPED STAGING FILES. Every run works in its own directory
    (<STAGING>/<run_id>/), so backfills / reruns cannot overwrite each other.
    Extract snapshots the source files so all tasks see the same bytes.
 6. SCHEMA CONTRACT + QUALITY GATE. Source columns are checked at extract time.
    Validation collects ALL failures, and raises AirflowFailException (no pointless
    retries for bad data).
 7. THE BRANCH NOW DOES SOMETHING: no changes -> skip, small delta -> one-shot
    upsert, large delta -> batched upsert with a commit per batch.
 8. AUDIT FOR SUCCESS *AND* FAILURE. A failure callback writes a FAILED row
    (failed task + error) to etl_run_audit; success writes full row counts.
 9. POST-LOAD RECONCILIATION. After upserting, we verify every delta row now
    exists in fact_orders with the matching hash.
10. OPERATIONAL SETTINGS. max_active_runs=1, dagrun_timeout, per-task
    execution_timeout, env-driven config, Asset outlet for downstream DAGs.

 CONFIGURATION (environment variables, all optional)
 ---------------------------------------------------
   ECOM_DATA_DIR         source folder           default /opt/airflow/dags/data
   ECOM_STAGING_DIR      run-scoped temp files   default /tmp/ecommerce_etl
   ECOM_MYSQL_CONN_ID    Airflow connection id   default mysql_default
   ECOM_MIN_RETENTION    min clean/raw ratio     default 0.95
   ECOM_MAX_ORPHAN_RATE  orders w/o customer     default 0.01
   ECOM_MAX_VOLUME_DROP  drop vs last success    default 0.50
   ECOM_LARGE_DELTA      rows => batched upsert  default 50000
   ECOM_BATCH_SIZE       rows per batch          default 20000
   ECOM_KEEP_STAGING     "1" keeps run folders   default 0

 REQUIREMENTS
 ------------
   apache-airflow-providers-mysql, apache-airflow-providers-standard,
   pandas>=2.0, SQLAlchemy>=2.0, a MySQL driver (mysqlclient or pymysql).

 KNOWN LIMITS (be honest in interviews)
 --------------------------------------
   * Staging files live on local disk. With several workers (Celery/K8s) use a
     shared volume or object storage (S3/GCS) for ECOM_STAGING_DIR.
   * stg_orders is shared, hence max_active_runs=1.
   * Rows deleted at the source are not deleted from fact_orders (soft policy).
   * VALUES() in ON DUPLICATE KEY UPDATE is deprecated since MySQL 8.0.20 but
     still supported; switch to a row alias on newer MySQL if it is removed.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
from datetime import datetime, timedelta, timezone

import pandas as pd

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Asset, dag, get_current_context, task

try:  # Airflow 3: FileSensor lives in the standard provider
    from airflow.providers.standard.sensors.filesystem import FileSensor
except ImportError:  # pragma: no cover - older layouts
    from airflow.sensors.filesystem import FileSensor

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover
    from airflow.exceptions import AirflowFailException


logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = os.environ.get("ECOM_DATA_DIR", "/opt/airflow/dags/data")
STAGING_ROOT = os.environ.get("ECOM_STAGING_DIR", "/tmp/ecommerce_etl")
MYSQL_CONN_ID = os.environ.get("ECOM_MYSQL_CONN_ID", "mysql_default")

MIN_RETENTION = float(os.environ.get("ECOM_MIN_RETENTION", "0.95"))
MAX_ORPHAN_RATE = float(os.environ.get("ECOM_MAX_ORPHAN_RATE", "0.01"))
MAX_VOLUME_DROP = float(os.environ.get("ECOM_MAX_VOLUME_DROP", "0.50"))
LARGE_DELTA_ROWS = int(os.environ.get("ECOM_LARGE_DELTA", "50000"))
BATCH_SIZE = int(os.environ.get("ECOM_BATCH_SIZE", "20000"))
KEEP_STAGING = os.environ.get("ECOM_KEEP_STAGING", "0") == "1"

ORDERS_SOURCE = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
CUSTOMERS_SOURCE = os.path.join(DATA_DIR, "olist_customers_dataset.csv")

# Table names (distinct from earlier versions so old tables are not reused)
FACT_TABLE = "fact_orders"
STAGING_TABLE = "stg_orders"
AUDIT_TABLE = "etl_run_audit"
REJECT_TABLE = "etl_rejected_orders"

ORDERS_ASSET = Asset(f"mysql://ecommerce_db/{FACT_TABLE}")

# ── Source "contract": columns we require to exist in the raw files ───────────
ORDERS_SOURCE_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]
CUSTOMERS_SOURCE_COLUMNS = [
    "customer_id", "customer_unique_id", "customer_city", "customer_state",
]

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"  # strict: anything else becomes NaT and is caught

ALLOWED_STATUSES = {
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
}

# ── Target schema: single source of truth for DDL, staging, upsert, validation ─
ORDER_COLUMNS = [
    ("order_id", "VARCHAR(32) NOT NULL"),
    ("customer_id", "VARCHAR(32) NOT NULL"),
    ("customer_unique_id", "VARCHAR(32) NULL"),
    ("customer_city", "VARCHAR(100) NULL"),
    ("customer_state", "VARCHAR(10) NULL"),
    ("order_status", "VARCHAR(20) NOT NULL"),
    ("order_purchase_timestamp", "DATETIME NOT NULL"),
    ("order_approved_at", "DATETIME NULL"),
    ("order_delivered_carrier_date", "DATETIME NULL"),
    ("order_delivered_customer_date", "DATETIME NULL"),
    ("order_estimated_delivery_date", "DATETIME NULL"),
    ("purchase_date", "DATE NOT NULL"),
    ("year", "SMALLINT NOT NULL"),
    ("month", "TINYINT NOT NULL"),
    ("quarter", "TINYINT NOT NULL"),
    ("delivery_days", "DECIMAL(8,2) NULL"),
    ("approval_hours", "DECIMAL(10,2) NULL"),
    ("is_late", "TINYINT(1) NULL"),
    ("row_hash", "CHAR(32) NOT NULL"),
]
COLUMN_NAMES = [name for name, _ in ORDER_COLUMNS]
HASH_COLUMNS = [c for c in COLUMN_NAMES if c != "row_hash"]
NOT_NULL_COLUMNS = [name for name, ddl in ORDER_COLUMNS if "NOT NULL" in ddl]


# ══════════════════════════════════════════════════════════════════════════════
# SMALL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _q(identifier: str) -> str:
    """Backtick-quote a SQL identifier (year/month/quarter are keywords)."""
    return f"`{identifier}`"


def _utcnow() -> datetime:
    """Naive UTC timestamp (MySQL DATETIME has no timezone)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _naive(value):
    if value is None:
        return None
    return value.replace(tzinfo=None) if getattr(value, "tzinfo", None) else value


def _hook() -> MySqlHook:
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID)


def _run_dir(run_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    return os.path.join(STAGING_ROOT, safe)


def _md5_file(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _count_csv_rows(path: str) -> int:
    """Row count that is safe for quoted newlines (unlike wc -l)."""
    return sum(len(chunk) for chunk in pd.read_csv(path, usecols=[0], chunksize=100_000))


def _to_db_frame(df: pd.DataFrame) -> pd.DataFrame:
    """NaN / NaT / pd.NA  ->  None so the driver writes SQL NULL."""
    out = df.astype(object)
    return out.where(pd.notna(out), None)


# ══════════════════════════════════════════════════════════════════════════════
# DDL
# ══════════════════════════════════════════════════════════════════════════════

def _fact_ddl() -> str:
    cols = [f"{_q(n)} {t}" for n, t in ORDER_COLUMNS]
    cols += [
        "`etl_run_id` VARCHAR(250) NOT NULL",
        "`etl_loaded_at` DATETIME NOT NULL",
        "PRIMARY KEY (`order_id`)",
        "KEY `idx_purchase_date` (`purchase_date`)",
        "KEY `idx_status` (`order_status`)",
        "KEY `idx_customer` (`customer_id`)",
        "KEY `idx_run` (`etl_run_id`)",
    ]
    body = ",\n  ".join(cols)
    return (f"CREATE TABLE IF NOT EXISTS {_q(FACT_TABLE)} (\n  {body}\n) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")


def _staging_ddl() -> str:
    cols = [f"{_q(n)} {t}" for n, t in ORDER_COLUMNS]
    cols += [
        "`batch_id` INT NOT NULL",
        "PRIMARY KEY (`order_id`)",
        "KEY `idx_batch` (`batch_id`)",
    ]
    body = ",\n  ".join(cols)
    return (f"CREATE TABLE IF NOT EXISTS {_q(STAGING_TABLE)} (\n  {body}\n) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")


AUDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS {_q(AUDIT_TABLE)} (
  `run_id` VARCHAR(250) NOT NULL,
  `dag_id` VARCHAR(100) NOT NULL,
  `logical_date` DATETIME NULL,
  `status` VARCHAR(20) NOT NULL,
  `failed_task` VARCHAR(100) NULL,
  `message` TEXT NULL,
  `source_orders_md5` CHAR(32) NULL,
  `rows_extracted` INT NULL,
  `rows_clean` INT NULL,
  `rows_rejected` INT NULL,
  `rows_inserted` INT NULL,
  `rows_updated` INT NULL,
  `rows_unchanged` INT NULL,
  `target_total_rows` INT NULL,
  `load_strategy` VARCHAR(30) NULL,
  `started_at` DATETIME NULL,
  `finished_at` DATETIME NULL,
  PRIMARY KEY (`run_id`),
  KEY `idx_status_finished` (`status`, `finished_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

REJECT_DDL = f"""
CREATE TABLE IF NOT EXISTS {_q(REJECT_TABLE)} (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `etl_run_id` VARCHAR(250) NOT NULL,
  `order_id` VARCHAR(64) NULL,
  `customer_id` VARCHAR(64) NULL,
  `order_status` VARCHAR(32) NULL,
  `order_purchase_timestamp` VARCHAR(32) NULL,
  `reject_reason` VARCHAR(50) NOT NULL,
  `rejected_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_run` (`etl_run_id`),
  KEY `idx_reason` (`reject_reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _ensure_tables(hook: MySqlHook) -> None:
    hook.run([_fact_ddl(), _staging_ddl(), AUDIT_DDL, REJECT_DDL], autocommit=True)


# ══════════════════════════════════════════════════════════════════════════════
# AUDIT + FAILURE CALLBACK
# ══════════════════════════════════════════════════════════════════════════════

def _upsert_audit(hook: MySqlHook, row: dict) -> None:
    """Insert-or-update one audit row keyed by run_id (only the given columns)."""
    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_sql = ", ".join(_q(c) for c in columns)
    updates = ", ".join(f"{_q(c)} = VALUES({_q(c)})" for c in columns if c != "run_id")
    sql = (f"INSERT INTO {_q(AUDIT_TABLE)} ({col_sql}) VALUES ({placeholders}) "
           f"ON DUPLICATE KEY UPDATE {updates}")
    hook.run(sql, parameters=tuple(row[c] for c in columns), autocommit=True)


def _on_task_failure(context) -> None:
    """
    Runs when a task fails for good (after its retries are exhausted).
    Records a FAILED row in etl_run_audit. Must never raise.
    This is also the place to plug in Slack / PagerDuty / e-mail alerting.
    """
    try:
        ti = context.get("task_instance") or context.get("ti")
        dag_run = context.get("dag_run")
        error = context.get("exception")
        run_id = context.get("run_id") or getattr(ti, "run_id", None)
        task_id = getattr(ti, "task_id", "unknown")

        logger.error("Task %s failed in run %s: %s", task_id, run_id, error)

        hook = _hook()
        _ensure_tables(hook)
        _upsert_audit(hook, {
            "run_id": run_id,
            "dag_id": getattr(ti, "dag_id", "ecommerce_etl_pipeline_v6"),
            "logical_date": _naive(context.get("logical_date")),
            "status": "FAILED",
            "failed_task": task_id,
            "message": str(error)[:4000] if error else None,
            "started_at": _naive(getattr(dag_run, "start_date", None)),
            "finished_at": _utcnow(),
        })
    except Exception:  # noqa: BLE001 - a callback must not mask the real failure
        logger.exception("Could not write FAILED audit row")


# ══════════════════════════════════════════════════════════════════════════════
# PURE DATA LOGIC  (no Airflow, no database  ->  easy to unit-test)
# ══════════════════════════════════════════════════════════════════════════════

def build_clean_orders(orders_raw: pd.DataFrame, customers_raw: pd.DataFrame):
    """
    Clean + enrich orders.

    Returns (clean_df, rejected_df, metrics).
    Every input row ends up in exactly one of clean_df / rejected_df.
    A row gets the FIRST matching reject reason (rules run in this order).
    """
    raw = orders_raw.copy()
    orders = raw.copy()

    # ── standardise text ──────────────────────────────────────────────────────
    for col in ("order_id", "customer_id", "order_status"):
        orders[col] = orders[col].str.strip()
    orders["order_status"] = orders["order_status"].str.lower()

    # ── parse timestamps strictly; count anything that fails to parse ─────────
    unparseable = {}
    for col in TIMESTAMP_COLUMNS:
        parsed = pd.to_datetime(orders[col], format=TS_FORMAT, errors="coerce")
        unparseable[col] = int((orders[col].notna() & parsed.isna()).sum())
        orders[col] = parsed

    # ── reject rules (first match wins) ───────────────────────────────────────
    reason = pd.Series(pd.NA, index=orders.index, dtype="object")

    def flag(mask: pd.Series, label: str) -> None:
        reason.loc[mask & reason.isna()] = label

    flag(orders["order_id"].isna() | (orders["order_id"] == ""), "missing_order_id")
    flag(orders["customer_id"].isna() | (orders["customer_id"] == ""), "missing_customer_id")
    flag(orders["order_purchase_timestamp"].isna(), "invalid_purchase_timestamp")
    flag(~orders["order_status"].isin(ALLOWED_STATUSES), "invalid_status")
    flag(orders["order_approved_at"].isna(), "missing_approval")
    flag(orders["order_approved_at"] < orders["order_purchase_timestamp"],
         "approval_before_purchase")
    flag(orders["order_delivered_customer_date"] < orders["order_purchase_timestamp"],
         "delivery_before_purchase")

    # duplicates: judged only among rows that survived so far; keep the first
    survivors = orders[reason.isna()]
    dup_index = survivors.index[survivors.duplicated(subset=["order_id"], keep="first")]
    flag(pd.Series(orders.index.isin(dup_index), index=orders.index), "duplicate_order_id")

    is_rejected = reason.notna()
    clean = orders[~is_rejected].copy()
    rejected = raw[is_rejected].copy()
    rejected["reject_reason"] = reason[is_rejected]

    # ── join customers (left join; customer_id made unique first) ─────────────
    customers = customers_raw[CUSTOMERS_SOURCE_COLUMNS].copy()
    customers["customer_id"] = customers["customer_id"].str.strip()
    duplicate_customers = int(customers.duplicated(subset=["customer_id"]).sum())
    customers = customers.drop_duplicates(subset=["customer_id"])

    clean = clean.merge(customers, on="customer_id", how="left", validate="many_to_one")
    orphan_orders = int(clean["customer_unique_id"].isna().sum())

    # ── derived business columns ──────────────────────────────────────────────
    purchase = clean["order_purchase_timestamp"]
    delivered = clean["order_delivered_customer_date"]
    estimated = clean["order_estimated_delivery_date"]

    clean["purchase_date"] = purchase.dt.date
    clean["year"] = purchase.dt.year
    clean["month"] = purchase.dt.month
    clean["quarter"] = purchase.dt.quarter
    clean["delivery_days"] = ((delivered - purchase).dt.total_seconds() / 86400).round(2)
    clean["approval_hours"] = (
        (clean["order_approved_at"] - purchase).dt.total_seconds() / 3600
    ).round(2)
    clean["is_late"] = (delivered > estimated).astype("Int8").where(
        delivered.notna() & estimated.notna()
    )

    # ── row hash: lets the load step detect NEW / CHANGED rows cheaply ────────
    clean = clean[HASH_COLUMNS].reset_index(drop=True)
    # NULLs are replaced by an explicit marker BEFORE stringifying, so the hash
    # is identical on every pandas version (astype(str) treats NaN differently).
    as_text = clean.astype(object).where(clean.notna(), "\x00NULL").astype(str)
    payload = as_text.agg("|".join, axis=1)
    clean["row_hash"] = payload.map(lambda s: hashlib.md5(s.encode("utf-8")).hexdigest())
    clean = clean[COLUMN_NAMES]

    metrics = {
        "rows_raw": int(len(raw)),
        "rows_clean": int(len(clean)),
        "rows_rejected": int(len(rejected)),
        "reject_reasons": {k: int(v) for k, v in reason.dropna().value_counts().items()},
        "duplicate_customers": duplicate_customers,
        "orphan_orders": orphan_orders,
        "unparseable_timestamps": {k: v for k, v in unparseable.items() if v},
    }
    return clean, rejected, metrics


def run_quality_checks(df: pd.DataFrame, meta: dict, previous_clean_rows):
    """
    Data-quality gate. Returns (failures, passed_count).
    Collects EVERY failure so one run shows the full picture.
    """
    failures: list[str] = []
    passed = 0

    def check(ok: bool, message: str) -> None:
        nonlocal passed
        if ok:
            passed += 1
        else:
            failures.append(message)

    check(len(df) > 0, "dataset is empty")
    if len(df) == 0:
        return failures, passed

    missing = sorted(set(COLUMN_NAMES) - set(df.columns))
    check(not missing, f"missing columns: {missing}")
    if missing:
        return failures, passed

    check(len(df) == meta["rows_clean"],
          f"file has {len(df)} rows but transform reported {meta['rows_clean']}")

    check(df["order_id"].notna().all(), "null order_id values found")
    dups = int(df["order_id"].duplicated().sum())
    check(dups == 0, f"{dups} duplicate order_id values")

    for col in NOT_NULL_COLUMNS:
        nulls = int(df[col].isna().sum())
        check(nulls == 0, f"{nulls} nulls in NOT NULL column '{col}'")

    latest = pd.to_datetime(df["order_purchase_timestamp"]).max()
    check(latest <= pd.Timestamp.now() + pd.Timedelta(days=1),
          f"future purchase timestamp detected: {latest}")

    bad_status = sorted(set(df["order_status"]) - ALLOWED_STATUSES)
    check(not bad_status, f"unexpected order_status values: {bad_status}")

    days = df["delivery_days"].dropna()
    check((days >= 0).all(), "negative delivery_days found")

    retention = meta["rows_clean"] / meta["rows_raw"] if meta["rows_raw"] else 0
    check(retention >= MIN_RETENTION,
          f"retention {retention:.2%} is below minimum {MIN_RETENTION:.0%}")

    orphan_rate = meta["orphan_orders"] / max(meta["rows_clean"], 1)
    check(orphan_rate <= MAX_ORPHAN_RATE,
          f"{orphan_rate:.2%} of orders have no matching customer (max {MAX_ORPHAN_RATE:.0%})")

    if previous_clean_rows:
        floor = previous_clean_rows * (1 - MAX_VOLUME_DROP)
        check(meta["rows_clean"] >= floor,
              f"volume dropped: {meta['rows_clean']} rows vs {previous_clean_rows} last success")

    return failures, passed


def upsert_sql(batch: bool) -> str:
    """INSERT ... SELECT ... ON DUPLICATE KEY UPDATE  from staging into fact."""
    cols = ", ".join(_q(c) for c in COLUMN_NAMES)
    updates = ", ".join(
        f"{_q(c)} = VALUES({_q(c)})" for c in COLUMN_NAMES if c != "order_id"
    )
    updates += ", `etl_run_id` = VALUES(`etl_run_id`), `etl_loaded_at` = VALUES(`etl_loaded_at`)"
    where = " WHERE `batch_id` = %s" if batch else ""
    return (
        f"INSERT INTO {_q(FACT_TABLE)} ({cols}, `etl_run_id`, `etl_loaded_at`) "
        f"SELECT {cols}, %s, %s FROM {_q(STAGING_TABLE)}{where} "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# DAG
# ══════════════════════════════════════════════════════════════════════════════

@dag(
    dag_id="ecommerce_etl_pipeline_v6",
    description="Production-style incremental e-commerce ETL into MySQL",
    doc_md=__doc__,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,                      # stg_orders is shared between runs
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "Aursh",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": _on_task_failure,
    },
    tags=["ecommerce", "etl", "production", "mysql", "incremental"],
)
def ecommerce_etl_pipeline_v6():

    # ──────────────────────────────────────────────────────────────────────────
    # SENSORS  - wait for BOTH source files (v5 only waited for orders)
    # mode="reschedule" frees the worker slot between checks.
    # ──────────────────────────────────────────────────────────────────────────
    wait_for_orders_file = FileSensor(
        task_id="wait_for_orders_file",
        filepath=ORDERS_SOURCE,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )
    wait_for_customers_file = FileSensor(
        task_id="wait_for_customers_file",
        filepath=CUSTOMERS_SOURCE,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # ──────────────────────────────────────────────────────────────────────────
    # ENSURE TABLES  - fail fast if MySQL is unreachable, before doing any work
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def ensure_tables():
        _ensure_tables(_hook())
        logger.info("Target tables are ready")

    # ──────────────────────────────────────────────────────────────────────────
    # EXTRACT  - snapshot sources into a run-scoped folder + schema contract
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def extract():
        context = get_current_context()
        run_dir = _run_dir(context["run_id"])
        os.makedirs(run_dir, exist_ok=True)

        result = {"run_dir": run_dir}
        sources = (
            ("orders", ORDERS_SOURCE, ORDERS_SOURCE_COLUMNS),
            ("customers", CUSTOMERS_SOURCE, CUSTOMERS_SOURCE_COLUMNS),
        )
        for name, source, required in sources:
            if not os.path.exists(source) or os.path.getsize(source) == 0:
                raise AirflowFailException(f"Source file missing or empty: {source}")

            snapshot = os.path.join(run_dir, f"raw_{name}.csv")
            shutil.copy2(source, snapshot)  # all later tasks read THIS copy

            header = set(pd.read_csv(snapshot, nrows=0).columns)
            missing = [c for c in required if c not in header]
            if missing:
                raise AirflowFailException(
                    f"Schema drift in {name} file - missing columns: {missing}"
                )

            result[f"{name}_path"] = snapshot
            result[f"{name}_rows"] = _count_csv_rows(snapshot)
            result[f"{name}_md5"] = _md5_file(snapshot)

        logger.info("Extract complete: %s", result)
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # TRANSFORM  - clean, enrich, quarantine bad rows
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def transform(extract_meta):
        orders_raw = pd.read_csv(extract_meta["orders_path"], dtype=str)
        customers_raw = pd.read_csv(extract_meta["customers_path"], dtype=str)

        clean, rejected, metrics = build_clean_orders(orders_raw, customers_raw)

        run_dir = extract_meta["run_dir"]
        clean_path = os.path.join(run_dir, "orders_clean.csv")
        rejected_path = os.path.join(run_dir, "orders_rejected.csv")
        clean.to_csv(clean_path, index=False)
        rejected.to_csv(rejected_path, index=False)

        if metrics["unparseable_timestamps"]:
            logger.warning("Unparseable timestamps: %s", metrics["unparseable_timestamps"])
        logger.info("Rejected by reason: %s", metrics["reject_reasons"])

        result = {
            **extract_meta,
            **metrics,
            "clean_path": clean_path,
            "rejected_path": rejected_path,
        }
        logger.info("Transform complete: %s", {k: v for k, v in metrics.items()})
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # VALIDATE  - quality gate; bad data = FAIL (no retries, it won't fix itself)
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def validate(meta):
        df = pd.read_csv(
            meta["clean_path"],
            dtype={"order_id": str, "customer_id": str},
        )

        row = _hook().get_first(
            f"SELECT `rows_clean` FROM {_q(AUDIT_TABLE)} "
            "WHERE `status` = 'SUCCESS' ORDER BY `finished_at` DESC LIMIT 1"
        )
        previous = row[0] if row and row[0] is not None else None

        failures, passed = run_quality_checks(df, meta, previous)
        if failures:
            for item in failures:
                logger.error("VALIDATION FAILED: %s", item)
            raise AirflowFailException(
                f"{len(failures)} data-quality check(s) failed: " + "; ".join(failures)
            )

        logger.info("All %d quality checks passed", passed)
        return {**meta, "checks_passed": passed}

    # ──────────────────────────────────────────────────────────────────────────
    # LOAD REJECTED  - quarantine table (delete-then-insert per run = idempotent)
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def load_rejected(meta):
        run_id = get_current_context()["run_id"]
        hook = _hook()
        hook.run(
            f"DELETE FROM {_q(REJECT_TABLE)} WHERE `etl_run_id` = %s",
            parameters=(run_id,), autocommit=True,
        )
        if meta["rows_rejected"] == 0:
            logger.info("No rejected rows")
            return {"rejected_loaded": 0}

        rejected = pd.read_csv(meta["rejected_path"], dtype=str)
        frame = pd.DataFrame({
            "etl_run_id": run_id,
            "order_id": rejected["order_id"].str.slice(0, 64),
            "customer_id": rejected["customer_id"].str.slice(0, 64),
            "order_status": rejected["order_status"].str.slice(0, 32),
            "order_purchase_timestamp": rejected["order_purchase_timestamp"].str.slice(0, 32),
            "reject_reason": rejected["reject_reason"],
            "rejected_at": _utcnow(),
        })
        _to_db_frame(frame).to_sql(
            REJECT_TABLE, con=hook.get_sqlalchemy_engine(),
            if_exists="append", index=False, chunksize=2000, method="multi",
        )
        logger.info("Quarantined %d rejected rows", len(frame))
        return {"rejected_loaded": int(len(frame))}

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE DELTA  - load everything into stg_orders, then DROP rows whose hash
    # already exists in fact_orders. What remains is exactly the NEW + CHANGED
    # rows. Safe to re-run: staging is truncated first.
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def stage_delta(meta):
        hook = _hook()
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_csv(
            meta["clean_path"],
            dtype={"order_id": str, "customer_id": str, "customer_unique_id": str,
                   "customer_city": str, "customer_state": str, "row_hash": str},
            parse_dates=TIMESTAMP_COLUMNS,
        )
        df["purchase_date"] = pd.to_datetime(df["purchase_date"]).dt.date
        df["is_late"] = df["is_late"].astype("Int8")
        df["batch_id"] = df.index // BATCH_SIZE

        hook.run(f"TRUNCATE TABLE {_q(STAGING_TABLE)}", autocommit=True)
        _to_db_frame(df).to_sql(
            STAGING_TABLE, con=engine, if_exists="append",
            index=False, chunksize=2000, method="multi",
        )
        staged = len(df)

        # remove rows that are already loaded and unchanged
        hook.run(
            f"DELETE s FROM {_q(STAGING_TABLE)} s "
            f"JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "AND f.`row_hash` = s.`row_hash`",
            autocommit=True,
        )

        delta = hook.get_first(f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)}")[0]
        new_rows = hook.get_first(
            f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)} s "
            f"LEFT JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "WHERE f.`order_id` IS NULL"
        )[0]

        result = {
            **meta,
            "staged_rows": int(staged),
            "delta_rows": int(delta),
            "new_rows": int(new_rows),
            "changed_rows": int(delta - new_rows),
            "unchanged_rows": int(staged - delta),
        }
        logger.info(
            "Staged %d rows -> delta %d (new %d, changed %d, unchanged %d)",
            staged, delta, new_rows, delta - new_rows, staged - delta,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # BRANCH  - pick the load strategy from the size of the delta
    # ──────────────────────────────────────────────────────────────────────────
    @task.branch
    def choose_load_strategy(stage):
        delta = stage["delta_rows"]
        if delta == 0:
            logger.info("Nothing to load")
            return "no_changes"
        if delta > LARGE_DELTA_ROWS:
            logger.info("Large delta (%d rows) -> batched upsert", delta)
            return "upsert_batched"
        logger.info("Small delta (%d rows) -> single upsert", delta)
        return "upsert_standard"

    @task
    def no_changes():
        logger.info("Source and warehouse are in sync - skipping load")

    @task
    def upsert_standard(stage):
        run_id = get_current_context()["run_id"]
        _hook().run(
            upsert_sql(batch=False),
            parameters=(run_id, _utcnow()),
            autocommit=True,
        )
        logger.info("Single-statement upsert done (%d rows)", stage["delta_rows"])

    @task
    def upsert_batched(stage):
        run_id = get_current_context()["run_id"]
        hook = _hook()
        batch_ids = [
            r[0] for r in hook.get_records(
                f"SELECT DISTINCT `batch_id` FROM {_q(STAGING_TABLE)} ORDER BY `batch_id`"
            )
        ]
        for position, batch_id in enumerate(batch_ids, start=1):
            hook.run(
                upsert_sql(batch=True),
                parameters=(run_id, _utcnow(), batch_id),
                autocommit=True,   # one commit per batch keeps transactions small
            )
            logger.info("Upserted batch %d/%d", position, len(batch_ids))

    # ──────────────────────────────────────────────────────────────────────────
    # FINALIZE  - reconcile: every delta row must now be in fact_orders with the
    # same hash. trigger_rule is required because the branch SKIPS two of three
    # upstream tasks.
    # ──────────────────────────────────────────────────────────────────────────
    @task(trigger_rule="none_failed_min_one_success", outlets=[ORDERS_ASSET])
    def finalize_load(stage, rejected):
        hook = _hook()

        verified = hook.get_first(
            f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)} s "
            f"JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "AND f.`row_hash` = s.`row_hash`"
        )[0]
        if verified != stage["delta_rows"]:
            raise AirflowFailException(
                f"Reconciliation failed: {verified} of {stage['delta_rows']} "
                "delta rows verified in target"
            )

        total = hook.get_first(f"SELECT COUNT(*) FROM {_q(FACT_TABLE)}")[0]
        strategy = (
            "none" if stage["delta_rows"] == 0
            else "batched" if stage["delta_rows"] > LARGE_DELTA_ROWS
            else "standard"
        )
        stats = {**stage, "target_total_rows": int(total), "load_strategy": strategy,
                 "rejected_loaded": rejected["rejected_loaded"]}
        logger.info("Reconciliation OK. %s now holds %d rows", FACT_TABLE, total)
        return stats

    # ──────────────────────────────────────────────────────────────────────────
    # AUDIT (success path; the failure path is the on_failure_callback)
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def write_audit(stats):
        context = get_current_context()
        dag_run = context.get("dag_run")
        _upsert_audit(_hook(), {
            "run_id": context["run_id"],
            "dag_id": "ecommerce_etl_pipeline_v6",
            "logical_date": _naive(context.get("logical_date")),
            "status": "SUCCESS",
            "failed_task": None,
            "message": None,
            "source_orders_md5": stats["orders_md5"],
            "rows_extracted": stats["orders_rows"],
            "rows_clean": stats["rows_clean"],
            "rows_rejected": stats["rows_rejected"],
            "rows_inserted": stats["new_rows"],
            "rows_updated": stats["changed_rows"],
            "rows_unchanged": stats["unchanged_rows"],
            "target_total_rows": stats["target_total_rows"],
            "load_strategy": stats["load_strategy"],
            "started_at": _naive(getattr(dag_run, "start_date", None)),
            "finished_at": _utcnow(),
        })
        logger.info("Audit row written")
        return stats

    @task
    def cleanup_staging(stats):
        if KEEP_STAGING:
            logger.info("ECOM_KEEP_STAGING=1 -> keeping %s", stats["run_dir"])
            return
        shutil.rmtree(stats["run_dir"], ignore_errors=True)
        logger.info("Removed staging folder %s", stats["run_dir"])

    @task
    def pipeline_summary(stats):
        logger.info("=" * 60)
        logger.info("E-COMMERCE ETL PIPELINE COMPLETED")
        logger.info("=" * 60)
        logger.info("Source rows        : %d", stats["orders_rows"])
        logger.info("Clean rows         : %d", stats["rows_clean"])
        logger.info("Rejected rows      : %d  %s", stats["rows_rejected"], stats["reject_reasons"])
        logger.info("Inserted           : %d", stats["new_rows"])
        logger.info("Updated            : %d", stats["changed_rows"])
        logger.info("Unchanged (skipped): %d", stats["unchanged_rows"])
        logger.info("Load strategy      : %s", stats["load_strategy"])
        logger.info("Rows in %s : %d", FACT_TABLE, stats["target_total_rows"])
        logger.info("=" * 60)

    # ──────────────────────────────────────────────────────────────────────────
    # WIRING
    # ──────────────────────────────────────────────────────────────────────────
    tables_ready = ensure_tables()
    extracted = extract()
    transformed = transform(extracted)
    validated = validate(transformed)

    staged = stage_delta(validated)
    rejected = load_rejected(validated)

    branch = choose_load_strategy(staged)
    skip = no_changes()
    standard = upsert_standard(staged)
    batched = upsert_batched(staged)

    finalized = finalize_load(staged, rejected)
    audited = write_audit(finalized)
    cleanup = cleanup_staging(audited)
    summary = pipeline_summary(audited)

    [wait_for_orders_file, wait_for_customers_file, tables_ready] >> extracted
    branch >> [skip, standard, batched] >> finalized
    audited >> [cleanup, summary]


ecommerce_etl_pipeline_v6()
