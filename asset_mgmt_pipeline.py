# Databricks notebook source
# MAGIC %md
# MAGIC # Trusted Asset-Management Data Pipeline (PySpark + Delta, Databricks)
# MAGIC
# MAGIC **Layers:** Landing (Volume) -> Bronze (raw, append-only) -> Silver (validated, typed, deduped, MERGE-idempotent) -> Gold (trusted positions + compliance exceptions)
# MAGIC
# MAGIC ```
# MAGIC  SOURCES            LANDING (UC Volume)        BRONZE                SILVER                 GOLD                         CONSUMERS
# MAGIC  ---------          ------------------         ----------------      -------------------    -------------------------    -------------------
# MAGIC  Security Master -->                            bronze_<ds>           silver_<ds>            gold_positions               BI / Risk / Ops
# MAGIC  Transactions    --> CSV files per ds/date --> (all strings,     --> (typed, DQ-checked, --> gold_position_exceptions --> Compliance
# MAGIC  Positions       --> + SHA-256 file manifest   row-hash, lineage)    deduped, MERGE by key)  gold_compliance_exceptions   (secure masked view)
# MAGIC  Market Prices   -->                                  |                    |                    ^ staging -> gate -> replaceWhere
# MAGIC  Restricted Sec. -->                                  |                    +--> quarantine      |
# MAGIC                                                       v                                         |
# MAGIC  CONTROL PLANE:  file_manifest | file_events | run_log | stage_log | recon | dq_results | metrics | alerts
# MAGIC  SECURITY:       Unity Catalog grants, secret scope for webhooks, masked view, Delta history + audit logs
# MAGIC  OBSERVABILITY:  freshness, volume deviation, reject-rate, recon balance, gate result, alerts table
# MAGIC ```
# MAGIC
# MAGIC **Run order in this notebook:** 0 Config -> 1 Tables -> 2 Generator -> 3 Pipeline code -> 4 Security -> 5 Scenarios (all 5 required + extras) -> 6 Scale test -> 7 Decision record.
# MAGIC
# MAGIC Assumes Unity Catalog (catalog + schema + Volume). Tested design for DBR 14+/serverless. Run cells top to bottom.

# COMMAND ----------

# DBTITLE 1,0. Config
import os, csv, json, uuid, time, shutil, hashlib, random
from datetime import datetime, date, timedelta
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DateType, TimestampType
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "asset_mgmt")
dbutils.widgets.text("volume", "landing")
CATALOG, SCHEMA, VOLUME = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

def T(name):  # fully-qualified table name
    return f"{CATALOG}.{SCHEMA}.{name}"

# Tunable business / operational thresholds
CFG = dict(
    stale_price_days=3,            # price older than this (vs business date) = treated as missing
    max_unpriced_pct=10.0,         # publication gate: max % of positions that may be held out for pricing issues
    max_reject_rate_pct=5.0,       # alert threshold for DQ rejections
    volume_dev_pct=50.0,           # alert threshold for volume change vs previous run
    compliance_lookback_days=7,    # re-evaluate restricted trades for this window (late data)
    required_daily=["transactions", "market_prices"],  # a missing file for these is an alert
)

FAIL_AT = None  # failure-injection point, set by run_pipeline(fail_at=...)

class InjectedFailure(Exception): pass
class PublicationBlocked(Exception): pass

def maybe_fail(point):
    if FAIL_AT == point:
        raise InjectedFailure(f"Injected failure at '{point}'")

# COMMAND ----------

# DBTITLE 1,1. Dataset specs and table DDL (schemas are explicit and versioned here)
SPECS = {
    "security_master": dict(
        cols=["security_id", "isin", "asset_class", "currency", "effective_from", "effective_to"],
        required=["security_id", "isin", "asset_class", "currency", "effective_from"],
        key=["security_id", "effective_from"]),
    "transactions": dict(  # extension: side (BUY/SELL)
        cols=["transaction_id", "portfolio_id", "security_id", "trade_date", "side", "quantity", "price", "currency"],
        required=["transaction_id", "portfolio_id", "security_id", "trade_date", "side", "quantity", "price", "currency"],
        key=["transaction_id"]),
    "positions": dict(
        cols=["portfolio_id", "security_id", "position_date", "quantity", "book_value", "currency"],
        required=["portfolio_id", "security_id", "position_date", "quantity", "book_value", "currency"],
        key=["portfolio_id", "security_id", "position_date"]),
    "market_prices": dict(
        cols=["security_id", "price_date", "price", "price_currency", "price_source"],
        required=["security_id", "price_date", "price", "price_currency"],
        key=["security_id", "price_date"]),
    "restricted_securities": dict(  # extension: restriction_id (business key)
        cols=["restriction_id", "security_id", "restriction_type", "effective_from", "effective_to", "status"],
        required=["restriction_id", "security_id", "restriction_type", "effective_from", "status"],
        key=["restriction_id"]),
}
TYPES = {
    "security_master": {"effective_from": "date", "effective_to": "date"},
    "transactions": {"trade_date": "date", "quantity": "decimal(28,8)", "price": "decimal(28,8)"},
    "positions": {"position_date": "date", "quantity": "decimal(28,8)", "book_value": "decimal(28,2)"},
    "market_prices": {"price_date": "date", "price": "decimal(28,8)"},
    "restricted_securities": {"effective_from": "date", "effective_to": "date"},
}
DATE_COL = {"transactions": "trade_date", "positions": "position_date", "market_prices": "price_date",
            "restricted_securities": "effective_from"}
SILVER_DDL = {
    "security_master": "security_id STRING, isin STRING, asset_class STRING, currency STRING, effective_from DATE, effective_to DATE",
    "transactions": "transaction_id STRING, portfolio_id STRING, security_id STRING, trade_date DATE, side STRING, quantity DECIMAL(28,8), price DECIMAL(28,8), currency STRING",
    "positions": "portfolio_id STRING, security_id STRING, position_date DATE, quantity DECIMAL(28,8), book_value DECIMAL(28,2), currency STRING",
    "market_prices": "security_id STRING, price_date DATE, price DECIMAL(28,8), price_currency STRING, price_source STRING",
    "restricted_securities": "restriction_id STRING, security_id STRING, restriction_type STRING, effective_from DATE, effective_to DATE, status STRING",
}
SILVER_LINEAGE = "_source_file STRING, _file_hash STRING, _row_hash STRING, _run_id STRING, _delivery_ts TIMESTAMP, _version INT, _updated_ts TIMESTAMP"
BRONZE_META = "_extra_json STRING, _source_file STRING, _file_hash STRING, _row_hash STRING, _run_id STRING, _delivery_ts TIMESTAMP"
SILVER_ORDER = ["security_master", "restricted_securities", "transactions", "positions", "market_prices"]  # master first

GOLD_POS = """business_date DATE, portfolio_id STRING, security_id STRING, isin STRING, asset_class STRING, currency STRING,
  quantity DECIMAL(28,8), opening_quantity DECIMAL(28,8), net_txn_quantity DECIMAL(28,8), txn_count INT,
  price DECIMAL(28,8), price_currency STRING, price_date DATE, price_status STRING, price_source STRING,
  market_value DECIMAL(38,8), opening_book_value DECIMAL(28,2), contributing_transaction_ids ARRAY<STRING>,
  _opening_position_date DATE, _opening_source_files ARRAY<STRING>, _price_source_file STRING,
  _sm_source_file STRING, _sm_effective_from DATE, _run_id STRING, _published_ts TIMESTAMP"""
GOLD_EXC = """business_date DATE, portfolio_id STRING, security_id STRING, reason STRING, quantity DECIMAL(28,8),
  price_status STRING, detail STRING, _run_id STRING, _created_ts TIMESTAMP"""
GOLD_COMP = """run_business_date DATE, transaction_id STRING, portfolio_id STRING, security_id STRING, trade_date DATE, side STRING,
  quantity DECIMAL(28,8), price DECIMAL(28,8), restriction_id STRING, restriction_type STRING,
  restriction_effective_from DATE, restriction_effective_to DATE, severity STRING,
  _txn_source_file STRING, _txn_row_hash STRING, _restriction_source_file STRING, _detected_run_id STRING, _detected_ts TIMESTAMP"""

CONTROL_DDL = {
    "file_manifest": "file_hash STRING, dataset STRING, file_path STRING, business_date DATE, status STRING, row_count BIGINT, load_run_id STRING, loaded_ts TIMESTAMP, silver_done BOOLEAN, silver_run_id STRING",
    "file_events": "event_ts TIMESTAMP, run_id STRING, dataset STRING, file_path STRING, file_hash STRING, event STRING, detail STRING",
    "run_log": "run_id STRING, business_date DATE, status STRING, ts TIMESTAMP, detail STRING",
    "stage_log": "run_id STRING, stage STRING, status STRING, ts TIMESTAMP, detail STRING",
    "quarantine": "dataset STRING, run_id STRING, reasons ARRAY<STRING>, raw_json STRING, _source_file STRING, _file_hash STRING, _row_hash STRING, quarantined_ts TIMESTAMP",
    "recon": """run_id STRING, business_date DATE, dataset STRING, files_loaded INT, duplicate_files INT, schema_rejected_files INT,
        records_received BIGINT, records_rejected BIGINT, records_duplicate_in_batch BIGINT, records_new BIGINT,
        records_corrected BIGINT, records_redelivered_unchanged BIGINT, records_ignored_out_of_order BIGINT,
        balanced BOOLEAN, recon_ts TIMESTAMP""",
    "dq_results": "run_id STRING, business_date DATE, dataset STRING, rule STRING, failed_rows BIGINT, total_rows BIGINT, severity STRING, result STRING",
    "metrics": "run_id STRING, business_date DATE, dataset STRING, metric STRING, value DOUBLE, metric_ts TIMESTAMP",
    "alerts": "alert_ts TIMESTAMP, run_id STRING, severity STRING, code STRING, dataset STRING, message STRING",
    "gold_positions": GOLD_POS, "stg_gold_positions": GOLD_POS,
    "gold_position_exceptions": GOLD_EXC, "stg_gold_position_exceptions": GOLD_EXC,
    "gold_compliance_exceptions": GOLD_COMP,
}

def all_tables():
    return ([f"bronze_{d}" for d in SPECS] + [f"silver_{d}" for d in SPECS] + list(CONTROL_DDL))

def create_tables():
    for ds, spec in SPECS.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {T('bronze_'+ds)} ({', '.join(c+' STRING' for c in spec['cols'])}, {BRONZE_META}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {T('silver_'+ds)} ({SILVER_DDL[ds]}, {SILVER_LINEAGE}) USING DELTA")
    for name, ddl in CONTROL_DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {T(name)} ({ddl}) USING DELTA")

def reset_all():
    """Demo helper: drops everything and clears landing files."""
    for t in all_tables():
        spark.sql(f"DROP TABLE IF EXISTS {T(t)}")
    for ds in SPECS:
        shutil.rmtree(f"{LANDING}/{ds}", ignore_errors=True)
    create_tables()

create_tables()

# COMMAND ----------

# DBTITLE 1,2. Synthetic data generator (with injected defects)
SEC_IDS = [f"SEC{i:04d}" for i in range(1, 51)]
CCY = {s: ["USD", "EUR", "GBP"][i % 3] for i, s in enumerate(SEC_IDS)}
ACLASS = {s: ["EQUITY", "BOND", "FUND", "ETF"][i % 4] for i, s in enumerate(SEC_IDS)}
def isin(i, v=0): return "US" + f"{i:09d}" + str(v)

def _d(s): return date.fromisoformat(s) if isinstance(s, str) else s
def _ds(d): return d.isoformat()

def _write(ds, D, seq, df):
    os.makedirs(f"{LANDING}/{ds}", exist_ok=True)
    p = f"{LANDING}/{ds}/{ds}_{D}_{seq}.csv"
    df.to_csv(p, index=False)
    time.sleep(0.05)  # distinct mtimes => deterministic delivery ordering
    return p

def gen_security_master():
    rows = []
    for i, s in enumerate(SEC_IDS, start=1):
        if s == "SEC0002":   # ISIN changed: two effective-dated versions
            rows.append((s, isin(i, 0), ACLASS[s], CCY[s], "2020-01-01", "2024-06-30"))
            rows.append((s, isin(i, 1), ACLASS[s], CCY[s], "2024-07-01", ""))
        elif s == "SEC0049":  # expired security
            rows.append((s, isin(i), ACLASS[s], CCY[s], "2020-01-01", "2024-12-31"))
        else:
            rows.append((s, isin(i), ACLASS[s], CCY[s], "2020-01-01", ""))
    return pd.DataFrame(rows, columns=SPECS["security_master"]["cols"])

def gen_restricted(D):
    d = _d(D)
    return pd.DataFrame([
        ("R001", "SEC0003", "NO_BUY",   _ds(d - timedelta(5)),  "",                 "ACTIVE"),    # active -> exceptions
        ("R002", "SEC0004", "NO_TRADE", _ds(d - timedelta(30)), _ds(d - timedelta(1)), "ACTIVE"),  # ended yesterday -> no exception
        ("R003", "SEC0005", "NO_TRADE", _ds(d - timedelta(10)), "",                 "INACTIVE"),  # inactive -> no exception
    ], columns=SPECS["restricted_securities"]["cols"])

def gen_prices(D):
    d = _d(D); rows = []
    for s in SEC_IDS[:48]:
        if s == "SEC0007": continue                                  # NO price at all -> missing price scenario
        if s != "SEC0008":                                           # SEC0008 only has yesterday's price -> STALE
            p = -5 if s == "SEC0010" else round(random.uniform(20, 200), 4)   # SEC0010: invalid price -> quarantined
            rows.append((s, _ds(d), p, CCY[s], "VENDOR_A"))
        rows.append((s, _ds(d - timedelta(1)), round(random.uniform(20, 200), 4), CCY[s], "VENDOR_A"))
    rows.append(rows[0])                                             # exact duplicate row in file
    return pd.DataFrame(rows, columns=SPECS["market_prices"]["cols"])

def gen_positions(D):
    d = _d(D) - timedelta(1); rows = []
    for p in range(1, 6):
        for s in SEC_IDS[:20]:
            q = random.randint(100, 1000)
            rows.append((f"P{p:03d}", s, _ds(d), q, round(q * random.uniform(20, 200), 2), CCY[s]))
    rows.append(rows[0])                                             # duplicate
    rows.append(("", "SEC0001", _ds(d), 10, 100.0, "USD"))           # missing key
    rows.append(("P001", "SEC9999", _ds(d), 10, 100.0, "USD"))       # unknown security
    rows.append(("P001", "SEC0001", "2025-02-30", 10, 100.0, "USD")) # invalid date
    return pd.DataFrame(rows, columns=SPECS["positions"]["cols"])

def gen_transactions(D, n=200):
    d = _d(D); ok_secs = [s for s in SEC_IDS[:20] if s not in ("SEC0003", "SEC0004")]; rows = []
    for i in range(n):
        s = random.choice(ok_secs)
        rows.append((f"TXN-{D}-{i:06d}", f"P{random.randint(1,5):03d}", s, D, random.choice(["BUY", "SELL"]),
                     random.randint(1, 100), round(random.uniform(20, 200), 2), CCY[s]))
    # business-scenario rows
    rows.append((f"TXN-{D}-RESTRICTED-BUY",  "P001", "SEC0003", D, "BUY",  500, 100.0, CCY["SEC0003"]))  # EXCEPTION expected
    rows.append((f"TXN-{D}-RESTRICTED-SELL", "P001", "SEC0003", D, "SELL", 50,  100.0, CCY["SEC0003"]))  # NO_BUY only -> no exception
    rows.append((f"TXN-{D}-AFTER-LIFT",      "P002", "SEC0004", D, "BUY",  100, 100.0, CCY["SEC0004"]))  # restriction ended -> none
    # defects
    rows.append(rows[0])                                                                                  # duplicate
    rows.append((f"TXN-{D}-BADQTY", "P001", "SEC0001", D, "BUY", -10, 50.0, "USD"))                       # invalid qty
    rows.append((f"TXN-{D}-UNKNOWN", "P001", "SEC9999", D, "BUY", 10, 50.0, "USD"))                       # unknown sec
    rows.append((f"TXN-{D}-EXPIRED", "P001", "SEC0049", D, "BUY", 10, 50.0, CCY["SEC0049"]))              # master expired
    rows.append(("", "P001", "SEC0001", D, "BUY", 10, 50.0, "USD"))                                       # missing key
    rows.append((f"TXN-{D}-BADPX", "P001", "SEC0001", D, "BUY", 10, "abc", "USD"))                        # invalid price
    return pd.DataFrame(rows, columns=SPECS["transactions"]["cols"])

def generate_day(D, include_positions=True, include_master=True, n_txn=200, seed=1):
    random.seed(seed)
    out = {}
    if include_master:
        out["security_master"] = _write("security_master", D, "001", gen_security_master())
        out["restricted_securities"] = _write("restricted_securities", D, "001", gen_restricted(D))
    out["market_prices"] = _write("market_prices", D, "001", gen_prices(D))
    if include_positions:
        out["positions"] = _write("positions", D, "001", gen_positions(D))
    out["transactions"] = _write("transactions", D, "001", gen_transactions(D, n_txn))
    return out

# --- delivery-scenario helpers ---
def make_exact_duplicate(D):      # same bytes, new filename -> caught by file hash
    src = f"{LANDING}/transactions/transactions_{D}_001.csv"; dst = f"{LANDING}/transactions/transactions_{D}_dup.csv"
    shutil.copyfile(src, dst); return dst

def make_redelivery(D, n=20):     # overlapping records, different file content -> caught by key + row hash in silver
    df = pd.read_csv(f"{LANDING}/transactions/transactions_{D}_001.csv", dtype=str, keep_default_na=False).head(n)
    return _write("transactions", D, "002", df)

def make_correction(D, idx=5):    # late correction: same transaction_id, different quantity
    df = pd.read_csv(f"{LANDING}/transactions/transactions_{D}_001.csv", dtype=str, keep_default_na=False).iloc[[idx]].copy()
    df["quantity"] = str(int(df["quantity"].iloc[0]) + 1000)
    return _write("transactions", D, "003", df), df["transaction_id"].iloc[0]

def make_schema_drift(D, kind):
    df = pd.read_csv(f"{LANDING}/transactions/transactions_{D}_001.csv", dtype=str, keep_default_na=False).head(5).copy()
    df["transaction_id"] = df["transaction_id"] + f"-{kind}"
    if kind == "extra_col": df["broker"] = "ACME"                    # additive change: tolerated, logged
    if kind == "missing_col": df = df.drop(columns=["price"])        # breaking change: file rejected
    return _write("transactions", D, f"drift_{kind}", df)

# COMMAND ----------

# DBTITLE 1,3a. Control-plane helpers (logging, alerts, recon, metrics)
def _coerce(v, dtype):
    if v is None: return None
    if isinstance(dtype, DateType) and isinstance(v, str): return date.fromisoformat(v)
    return v

def log_row(table, **kw):
    sch = spark.table(T(table)).schema
    row = tuple(_coerce(kw.get(f.name), f.dataType) for f in sch.fields)
    spark.createDataFrame([row], sch).write.mode("append").insertInto(T(table))

def append_df(df, table):
    cols = spark.table(T(table)).columns
    df.select(*cols).write.mode("append").insertInto(T(table))

def now(): return datetime.utcnow()

def alert(run_id, severity, code, message, dataset=None):
    log_row("alerts", alert_ts=now(), run_id=run_id, severity=severity, code=code, dataset=dataset, message=message)
    print(f"[ALERT:{severity}] {code} {dataset or ''} - {message}")
    # Production: route CRITICAL/HIGH to PagerDuty/Teams/Slack. Webhook URL lives in a secret scope, never in code:
    # url = dbutils.secrets.get(scope="asset-mgmt", key="alert-webhook"); requests.post(url, json={...})

def replace_where(df, table, predicate):
    """Atomic single-transaction overwrite of a slice. Re-running with the same input yields the same table state."""
    df.select(*spark.table(T(table)).columns).write.format("delta").mode("overwrite") \
      .option("replaceWhere", predicate).saveAsTable(T(table))

def write_metrics(run_id, D, rows):
    for ds, metric, value in rows:
        log_row("metrics", run_id=run_id, business_date=D, dataset=ds, metric=metric, value=float(value), metric_ts=now())

# COMMAND ----------

# DBTITLE 1,3b. Bronze ingestion (file manifest, duplicate-file detection, schema drift handling)
def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def list_files(ds, D):
    d = f"{LANDING}/{ds}"
    if not os.path.isdir(d): return []
    fs = [f"{d}/{f}" for f in os.listdir(d) if f.startswith(f"{ds}_{D}_") and f.endswith(".csv")]
    return sorted(fs, key=os.path.getmtime)

def ingest_dataset(ds, D, run_id):
    spec = SPECS[ds]; expected, required = spec["cols"], spec["required"]
    files = list_files(ds, D); stats = dict(loaded=0, duplicate=0, rejected=0)
    if not files:
        log_row("file_events", event_ts=now(), run_id=run_id, dataset=ds, event="NO_FILE", detail=f"no file for {D}")
        if ds in CFG["required_daily"]:
            alert(run_id, "HIGH", "MISSING_FILE", f"no {ds} file received for {D}", ds)
        return stats
    for path in files:
        h = sha256_file(path)
        if spark.table(T("file_manifest")).filter(F.col("file_hash") == h).count() > 0:
            log_row("file_events", event_ts=now(), run_id=run_id, dataset=ds, file_path=path, file_hash=h,
                    event="DUPLICATE_FILE", detail="identical content already in manifest; skipped")
            stats["duplicate"] += 1; continue
        with open(path, newline="") as fh:
            header = next(csv.reader(fh), [])
        missing = [c for c in required if c not in header]
        mf_row = dict(file_hash=h, dataset=ds, file_path=path, business_date=D, load_run_id=run_id, loaded_ts=now(),
                      silver_done=False)
        if missing:  # breaking schema change: reject whole file, load nothing, alert
            mf = spark.createDataFrame([tuple(_coerce(mf_row.get(f.name), f.dataType) if f.name != "status" and f.name != "row_count"
                                              else {"status": "SCHEMA_REJECTED", "row_count": 0}[f.name]
                                              for f in spark.table(T("file_manifest")).schema.fields)],
                                       spark.table(T("file_manifest")).schema)
            mf.write.mode("append").insertInto(T("file_manifest"))
            log_row("file_events", event_ts=now(), run_id=run_id, dataset=ds, file_path=path, file_hash=h,
                    event="SCHEMA_REJECTED", detail=f"missing required columns: {missing}")
            alert(run_id, "CRITICAL", "SCHEMA_BREAK", f"{os.path.basename(path)} missing {missing}; file rejected", ds)
            stats["rejected"] += 1; continue
        extras = [c for c in header if c not in expected]
        if extras:   # additive change: keep expected columns, preserve extras as JSON, warn
            alert(run_id, "WARN", "SCHEMA_DRIFT", f"{os.path.basename(path)} has unexpected columns {extras}; kept in _extra_json", ds)
        df = spark.read.option("header", True).option("inferSchema", False).csv(path)
        for c in expected:
            if c not in df.columns: df = df.withColumn(c, F.lit(None).cast("string"))
        row_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in expected]), 256)
        df = (df.select(*expected, *([F.to_json(F.struct(*extras)).alias("_extra_json")] if extras else [F.lit(None).cast("string").alias("_extra_json")]))
                .withColumn("_source_file", F.lit(path)).withColumn("_file_hash", F.lit(h))
                .withColumn("_row_hash", row_hash).withColumn("_run_id", F.lit(run_id))
                .withColumn("_delivery_ts", F.lit(datetime.fromtimestamp(os.path.getmtime(path)))))
        # delete-then-append keyed on file hash => a crash between bronze write and manifest write is safely replayable
        spark.sql(f"DELETE FROM {T('bronze_'+ds)} WHERE _file_hash = '{h}'")
        append_df(df, f"bronze_{ds}")
        n = spark.table(T(f"bronze_{ds}")).filter(F.col("_file_hash") == h).count()
        sch = spark.table(T("file_manifest")).schema
        vals = dict(mf_row, status="LOADED", row_count=n)
        spark.createDataFrame([tuple(_coerce(vals.get(f.name), f.dataType) for f in sch.fields)], sch) \
             .write.mode("append").insertInto(T("file_manifest"))
        log_row("file_events", event_ts=now(), run_id=run_id, dataset=ds, file_path=path, file_hash=h, event="LOADED", detail=f"{n} rows")
        stats["loaded"] += 1
    return stats

def bronze_stage(D, run_id):
    return {ds: ingest_dataset(ds, D, run_id) for ds in SPECS}

# COMMAND ----------

# DBTITLE 1,3c. Data quality rules + effective-dated reference check (pure functions; reused by the scale test)
MAXD = F.lit("9999-12-31").cast("date")
CUR = r"^[A-Z]{3}$"

def _bad_pos(c):  return F.coalesce(F.col(f"{c}_t") <= 0, F.lit(True))
def _bad_date(c): return F.col(f"{c}_t").isNull()
def _not_in(c, vals): return ~F.coalesce(F.col(c).isin(*vals), F.lit(False))
def _bad_rx(c, pat):  return ~F.coalesce(F.col(c).rlike(pat), F.lit(False))
def _null(*cs): return reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cs])
def _range_bad(): return F.col("effective_to_t") < F.col("effective_from_t")
def _unknown(dc): return F.col("security_id").isNotNull() & F.col(dc).isNotNull() & ~F.col("_ref_ok")

def rules_for(ds, D):
    """Returns [(rule_name, condition_true_when_BAD)]. All rejects here are hard rejects -> quarantine."""
    if ds == "security_master":
        return [("MISSING_KEY", _null("security_id", "effective_from")),
                ("INVALID_EFFECTIVE_FROM", _bad_date("effective_from")),
                ("INVALID_EFFECTIVE_TO", F.col("effective_to").isNotNull() & _bad_date("effective_to")),
                ("EFFECTIVE_RANGE", _range_bad()),
                ("INVALID_ISIN", _bad_rx("isin", r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")),
                ("INVALID_ASSET_CLASS", _not_in("asset_class", ["EQUITY", "BOND", "FUND", "ETF", "CASH"])),
                ("INVALID_CURRENCY", _bad_rx("currency", CUR))]
    if ds == "transactions":
        return [("MISSING_KEY", _null("transaction_id", "portfolio_id", "security_id")),
                ("INVALID_TRADE_DATE", _bad_date("trade_date") | (F.col("trade_date_t") > D)),
                ("INVALID_SIDE", _not_in("side", ["BUY", "SELL"])),
                ("INVALID_QUANTITY", _bad_pos("quantity")),
                ("INVALID_PRICE", _bad_pos("price")),
                ("INVALID_CURRENCY", _bad_rx("currency", CUR)),
                ("UNKNOWN_SECURITY", _unknown("trade_date_t"))]
    if ds == "positions":
        return [("MISSING_KEY", _null("portfolio_id", "security_id")),
                ("INVALID_POSITION_DATE", _bad_date("position_date") | (F.col("position_date_t") > D)),
                ("INVALID_QUANTITY", F.col("quantity_t").isNull()),
                ("INVALID_BOOK_VALUE", F.col("book_value_t").isNull()),
                ("INVALID_CURRENCY", _bad_rx("currency", CUR)),
                ("UNKNOWN_SECURITY", _unknown("position_date_t"))]
    if ds == "market_prices":
        return [("MISSING_KEY", _null("security_id")),
                ("INVALID_PRICE_DATE", _bad_date("price_date") | (F.col("price_date_t") > D)),
                ("INVALID_PRICE", _bad_pos("price")),
                ("INVALID_CURRENCY", _bad_rx("price_currency", CUR)),
                ("UNKNOWN_SECURITY", _unknown("price_date_t"))]
    if ds == "restricted_securities":
        return [("MISSING_KEY", _null("restriction_id", "security_id")),
                ("INVALID_TYPE", _not_in("restriction_type", ["NO_BUY", "NO_SELL", "NO_TRADE"])),
                ("INVALID_EFFECTIVE_FROM", _bad_date("effective_from")),
                ("INVALID_EFFECTIVE_TO", F.col("effective_to").isNotNull() & _bad_date("effective_to")),
                ("EFFECTIVE_RANGE", _range_bad()),
                ("INVALID_STATUS", _not_in("status", ["ACTIVE", "INACTIVE", "CANCELLED"])),
                ("UNKNOWN_SECURITY", _unknown("effective_from_t"))]

def add_ref_flag(df, sm, date_col):
    """Effective-dated security-master check WITHOUT a row-multiplying range join:
    collapse master versions to an array per security, then test 'any version covers the date'."""
    rng = sm.groupBy("security_id").agg(F.collect_list(F.struct("effective_from", "effective_to")).alias("_ranges"))
    dt = F.col(date_col)
    return (df.join(F.broadcast(rng), "security_id", "left")
              .withColumn("_ref_ok", F.coalesce(F.exists("_ranges",
                          lambda r: (dt >= r["effective_from"]) & (dt <= F.coalesce(r["effective_to"], MAXD))), F.lit(False)))
              .drop("_ranges"))

def apply_dq(ds, bronze, sm, D):
    """bronze-like string DF -> normalised + typed DF with `_reasons` array (empty array = valid)."""
    cols = SPECS[ds]["cols"]; meta = [c for c in bronze.columns if c.startswith("_")]
    df = bronze.select(*[F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))).alias(c) for c in cols], *meta)
    for c, t in TYPES.get(ds, {}).items():
        df = df.withColumn(f"{c}_t", F.expr(f"try_cast(`{c}` AS {t})"))   # invalid -> NULL (never throws, even under ANSI)
    if ds != "security_master":
        df = add_ref_flag(df, sm, f"{DATE_COL[ds]}_t")
    rules = rules_for(ds, F.lit(D).cast("date"))
    arr = F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules])
    df = df.withColumn("_reasons", F.filter(arr, lambda x: x.isNotNull()))
    return df, [n for n, _ in rules]

def to_silver_shape(df, ds, run_id):
    cols, types = SPECS[ds]["cols"], TYPES.get(ds, {})
    return df.select(*[(F.col(f"{c}_t") if c in types else F.col(c)).alias(c) for c in cols],
                     "_source_file", "_file_hash", "_row_hash", F.lit(run_id).alias("_run_id"), "_delivery_ts")

def dedupe_latest(df, keys):
    """Within a batch: latest delivery wins; deterministic tie-breakers."""
    w = Window.partitionBy(*keys).orderBy(F.col("_delivery_ts").desc(), F.col("_source_file").desc(), F.col("_row_hash").desc())
    return df.withColumn("_rn", F.row_number().over(w))

# COMMAND ----------

# DBTITLE 1,3d. Silver: validate -> quarantine -> dedupe -> idempotent MERGE -> reconcile
def write_recon(run_id, D, ds, **v):
    spark.sql(f"DELETE FROM {T('recon')} WHERE run_id='{run_id}' AND dataset='{ds}'")
    v.setdefault("files_loaded", 0)
    for k in ("records_received", "records_rejected", "records_duplicate_in_batch", "records_new", "records_corrected",
              "records_redelivered_unchanged", "records_ignored_out_of_order"):
        v.setdefault(k, 0)
    v["balanced"] = v["records_received"] == (v["records_rejected"] + v["records_duplicate_in_batch"] + v["records_new"]
                                              + v["records_corrected"] + v["records_redelivered_unchanged"] + v["records_ignored_out_of_order"])
    log_row("recon", run_id=run_id, business_date=D, dataset=ds, recon_ts=now(), **v)

def silver_process(ds, D, run_id):
    spec = SPECS[ds]; keys, cols = spec["key"], spec["cols"]
    ev = spark.table(T("file_events")).filter((F.col("run_id") == run_id) & (F.col("dataset") == ds))
    dup_files = ev.filter(F.col("event") == "DUPLICATE_FILE").count()
    rej_files = ev.filter(F.col("event") == "SCHEMA_REJECTED").count()
    pending = (spark.table(T("file_manifest")).filter((F.col("dataset") == ds) & (F.col("status") == "LOADED") & (~F.col("silver_done")))
               .select("file_hash").collect())
    hashes = [r.file_hash for r in pending]
    if not hashes:
        write_recon(run_id, D, ds, duplicate_files=dup_files, schema_rejected_files=rej_files)
        return {"pending_files": 0}
    hs = ",".join(f"'{h}'" for h in hashes)

    bronze = spark.table(T(f"bronze_{ds}")).filter(F.col("_file_hash").isin(hashes))
    sm = spark.table(T("silver_security_master"))
    tagged, rule_names = apply_dq(ds, bronze, sm, D)

    st = tagged.agg(F.count("*").alias("n"), F.sum(F.when(F.size("_reasons") > 0, 1).otherwise(0)).alias("bad")).first()
    received, rejected = st["n"], int(st["bad"] or 0)

    # --- quarantine (nothing is silently dropped) ---
    bad = tagged.filter(F.size("_reasons") > 0)
    spark.sql(f"DELETE FROM {T('quarantine')} WHERE dataset='{ds}' AND _file_hash IN ({hs})")
    append_df(bad.select(F.lit(ds).alias("dataset"), F.lit(run_id).alias("run_id"), F.col("_reasons").alias("reasons"),
                         F.to_json(F.struct(*cols)).alias("raw_json"), "_source_file", "_file_hash", "_row_hash",
                         F.current_timestamp().alias("quarantined_ts")), "quarantine")
    counts = {r["rule"]: r["c"] for r in bad.select(F.explode("_reasons").alias("rule")).groupBy("rule").agg(F.count("*").alias("c")).collect()}
    for r in rule_names:
        log_row("dq_results", run_id=run_id, business_date=D, dataset=ds, rule=r, failed_rows=counts.get(r, 0),
                total_rows=received, severity="ERROR", result="FAIL" if counts.get(r, 0) else "PASS")

    # --- dedupe valid rows within the batch ---
    good = to_silver_shape(tagged.filter(F.size("_reasons") == 0), ds, run_id)
    ranked = dedupe_latest(good, keys)
    dup_in_batch = ranked.filter(F.col("_rn") > 1).count()
    src = ranked.filter(F.col("_rn") == 1).drop("_rn")

    # --- classify vs current silver (replay-aware: same run + same file + same hash = our own earlier write) ---
    tgt = spark.table(T(f"silver_{ds}")).select(*keys, F.col("_row_hash").alias("t_hash"), F.col("_delivery_ts").alias("t_ts"),
                                               F.col("_run_id").alias("t_run"), F.col("_file_hash").alias("t_fh"))
    cls = (F.when(F.col("t_hash").isNull(), "NEW")
            .when((F.col("t_hash") == F.col("_row_hash")) & (F.col("t_run") == run_id) & (F.col("t_fh") == F.col("_file_hash")), "NEW")
            .when(F.col("t_hash") == F.col("_row_hash"), "UNCHANGED")
            .when(F.col("_delivery_ts") >= F.col("t_ts"), "CORRECTED").otherwise("IGNORED_OLD"))
    cc = {r["cls"]: r["c"] for r in src.join(tgt, keys, "left").groupBy(cls.alias("cls")).agg(F.count("*").alias("c")).collect()}

    # --- idempotent upsert: re-applying the same input yields identical table state ---
    non_keys = [c for c in cols if c not in keys] + ["_source_file", "_file_hash", "_row_hash", "_run_id", "_delivery_ts"]
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    upd = {c: f"s.{c}" for c in non_keys}; upd.update({"_version": "t._version + 1", "_updated_ts": "current_timestamp()"})
    ins = {c: f"s.{c}" for c in keys + non_keys}; ins.update({"_version": "1", "_updated_ts": "current_timestamp()"})
    (DeltaTable.forName(spark, T(f"silver_{ds}")).alias("t").merge(src.alias("s"), on)
        .whenMatchedUpdate(condition="t._row_hash <> s._row_hash AND s._delivery_ts >= t._delivery_ts", set=upd)
        .whenNotMatchedInsert(values=ins).execute())

    maybe_fail(f"silver_{ds}_after_merge")   # failure point: silver advanced, manifest not yet marked -> replay must be safe

    spark.sql(f"UPDATE {T('file_manifest')} SET silver_done = true, silver_run_id = '{run_id}' WHERE file_hash IN ({hs})")
    write_recon(run_id, D, ds, files_loaded=len(hashes), duplicate_files=dup_files, schema_rejected_files=rej_files,
                records_received=received, records_rejected=rejected, records_duplicate_in_batch=dup_in_batch,
                records_new=cc.get("NEW", 0), records_corrected=cc.get("CORRECTED", 0),
                records_redelivered_unchanged=cc.get("UNCHANGED", 0), records_ignored_out_of_order=cc.get("IGNORED_OLD", 0))
    return {"received": received, "rejected": rejected, "dup_in_batch": dup_in_batch, **cc}

# COMMAND ----------

# DBTITLE 1,3e. Gold: effective-dated positions with market value (pure builder + stage/validate/publish)
def build_gold(opening, txns, sm, prices, D, run_id, stale_days, opening_date):
    """Inputs are silver-shaped DataFrames. Returns (publishable_positions, held_out_exceptions)."""
    Dc = F.lit(D).cast("date")
    op = opening.select("portfolio_id", "security_id", F.col("quantity").alias("opening_quantity"),
                        F.col("book_value").alias("opening_book_value"), F.col("_source_file").alias("_op_file"))
    signed = F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))
    net = txns.groupBy("portfolio_id", "security_id").agg(
        F.sum(signed).alias("net_txn_quantity"), F.count("*").cast("int").alias("txn_count"),
        F.sort_array(F.collect_list("transaction_id")).alias("contributing_transaction_ids"))
    pos = (op.join(net, ["portfolio_id", "security_id"], "full_outer")
             .withColumn("quantity", (F.coalesce("opening_quantity", F.lit(0)) + F.coalesce("net_txn_quantity", F.lit(0))).cast("decimal(28,8)"))
             .filter(F.col("quantity") != 0))

    # effective-dated master: the version valid on the business date
    w1 = Window.partitionBy("security_id").orderBy(F.col("effective_from").desc())
    sm_asof = (sm.filter((F.col("effective_from") <= Dc) & (F.coalesce("effective_to", MAXD) >= Dc))
                 .withColumn("_rn", F.row_number().over(w1)).filter("_rn = 1")
                 .select("security_id", "isin", "asset_class", "currency",
                         F.col("_source_file").alias("_sm_source_file"), F.col("effective_from").alias("_sm_effective_from")))
    # latest price on/before business date within the staleness window
    w2 = Window.partitionBy("security_id").orderBy(F.col("price_date").desc(), F.col("_delivery_ts").desc())
    px = (prices.filter((F.col("price_date") <= Dc) & (F.col("price_date") >= F.date_sub(Dc, stale_days)))
                .withColumn("_rn", F.row_number().over(w2)).filter("_rn = 1")
                .select("security_id", "price", "price_currency", "price_date", "price_source",
                        F.col("_source_file").alias("_price_source_file")))
    j = pos.join(F.broadcast(sm_asof), "security_id", "left").join(px, "security_id", "left")
    status = (F.when(F.col("price").isNull(), "MISSING")
               .when(F.col("price_currency") != F.col("currency"), "CURRENCY_MISMATCH")
               .when(F.col("price_date") == Dc, "FRESH").otherwise("STALE"))
    hold = (F.when(F.col("isin").isNull(), "NO_SECURITY_MASTER")
             .when(F.col("price").isNull(), "PRICE_MISSING")
             .when(F.col("price_currency") != F.col("currency"), "PRICE_CURRENCY_MISMATCH"))
    j = (j.withColumn("price_status", status).withColumn("_hold", hold).withColumn("business_date", Dc)
          .withColumn("market_value", (F.col("quantity") * F.col("price")).cast("decimal(38,8)")))

    pub = j.filter(F.col("_hold").isNull()).select(
        "business_date", "portfolio_id", "security_id", "isin", "asset_class", "currency", "quantity", "opening_quantity",
        "net_txn_quantity", "txn_count", "price", "price_currency", "price_date", "price_status", "price_source", "market_value",
        "opening_book_value", "contributing_transaction_ids",
        F.lit(opening_date).cast("date").alias("_opening_position_date"),
        F.when(F.col("_op_file").isNotNull(), F.array("_op_file")).otherwise(F.array().cast("array<string>")).alias("_opening_source_files"),
        "_price_source_file", "_sm_source_file", "_sm_effective_from",
        F.lit(run_id).alias("_run_id"), F.current_timestamp().alias("_published_ts"))
    exc = j.filter(F.col("_hold").isNotNull()).select(
        "business_date", "portfolio_id", "security_id", F.col("_hold").alias("reason"), "quantity", "price_status",
        F.concat_ws(" ", F.lit("price_date="), F.col("price_date").cast("string"), F.lit("price_ccy="), F.col("price_currency"),
                    F.lit("sec_ccy="), F.col("currency")).alias("detail"),
        F.lit(run_id).alias("_run_id"), F.current_timestamp().alias("_created_ts"))
    return pub, exc

def gold_build_stage(D, run_id):
    """Phase 1 of 2: build into STAGING. Gold is not touched."""
    sp = spark.table(T("silver_positions"))
    od = sp.filter(F.col("position_date") < F.lit(D).cast("date")).agg(F.max("position_date")).first()[0]
    od = od or (_d(D) - timedelta(1))
    opening = sp.filter(F.col("position_date") == F.lit(od))
    txns = spark.table(T("silver_transactions")).filter((F.col("trade_date") > F.lit(od)) & (F.col("trade_date") <= F.lit(D).cast("date")))
    pub, exc = build_gold(opening, txns, spark.table(T("silver_security_master")), spark.table(T("silver_market_prices")),
                          D, run_id, CFG["stale_price_days"], od)
    replace_where(pub, "stg_gold_positions", f"business_date = '{D}'")
    replace_where(exc, "stg_gold_position_exceptions", f"business_date = '{D}'")
    return {"opening_date": str(od)}

def gold_publish_stage(D, run_id):
    """Phase 2 of 2: publication gate, then one atomic replaceWhere per gold table."""
    stg = spark.table(T("stg_gold_positions")).filter((F.col("business_date") == D) & (F.col("_run_id") == run_id))
    sx = spark.table(T("stg_gold_position_exceptions")).filter((F.col("business_date") == D) & (F.col("_run_id") == run_id))
    n_pub, n_exc = stg.count(), sx.count()
    total = n_pub + n_exc; pct = 100.0 * n_exc / total if total else 0.0
    unbalanced = spark.table(T("recon")).filter((F.col("run_id") == run_id) & (~F.col("balanced"))).count()
    sch_bad = (spark.table(T("file_events")).filter((F.col("run_id") == run_id) & (F.col("event") == "SCHEMA_REJECTED")
               & F.col("dataset").isin(CFG["required_daily"])).count())
    checks = {"unpriced_pct<=max": pct <= CFG["max_unpriced_pct"], "recon_balanced": unbalanced == 0, "no_schema_break_on_required": sch_bad == 0}
    write_metrics(run_id, D, [("gold", "positions_published_candidates", n_pub), ("gold", "positions_held_out", n_exc),
                              ("gold", "held_out_pct", pct), ("gold", "gate_passed", all(checks.values()))])
    if not all(checks.values()):
        failed = [k for k, v in checks.items() if not v]
        alert(run_id, "CRITICAL", "PUBLICATION_BLOCKED", f"gate failed {failed}; held_out={pct:.1f}%; gold left unchanged", "gold")
        raise PublicationBlocked(str(failed))
    maybe_fail("gold_before_publish")        # failure point: staging built, gold untouched -> safe
    replace_where(stg, "gold_positions", f"business_date = '{D}'")
    replace_where(sx, "gold_position_exceptions", f"business_date = '{D}'")
    n_live = spark.table(T("gold_positions")).filter(F.col("business_date") == D).count()
    assert n_live == n_pub, f"post-publish check failed: {n_live} != {n_pub}"
    if n_exc: alert(run_id, "WARN", "POSITIONS_HELD_OUT", f"{n_exc} positions held out ({pct:.1f}%) - see gold_position_exceptions", "gold")
    return {"published": n_pub, "held_out": n_exc}

# COMMAND ----------

# DBTITLE 1,3f. Compliance exceptions (effective-dated restricted list join), observability, orchestrator
def compliance_stage(D, run_id):
    lb = CFG["compliance_lookback_days"]; lo = _ds(_d(D) - timedelta(lb))
    t = spark.table(T("silver_transactions")).filter((F.col("trade_date") >= F.lit(lo).cast("date")) & (F.col("trade_date") <= F.lit(D).cast("date")))
    r = spark.table(T("silver_restricted_securities")).filter(F.col("status") == "ACTIVE")
    cond = ((F.col("t.security_id") == F.col("r.security_id")) & (F.col("t.trade_date") >= F.col("r.effective_from"))
            & (F.col("t.trade_date") <= F.coalesce(F.col("r.effective_to"), MAXD))
            & ((F.col("r.restriction_type") == "NO_TRADE") | ((F.col("r.restriction_type") == "NO_BUY") & (F.col("t.side") == "BUY"))
               | ((F.col("r.restriction_type") == "NO_SELL") & (F.col("t.side") == "SELL"))))
    out = t.alias("t").join(r.alias("r"), cond).select(
        F.lit(D).cast("date").alias("run_business_date"), "t.transaction_id", "t.portfolio_id", F.col("t.security_id").alias("security_id"),
        "t.trade_date", "t.side", F.col("t.quantity").alias("quantity"), F.col("t.price").alias("price"), "r.restriction_id", "r.restriction_type",
        F.col("r.effective_from").alias("restriction_effective_from"), F.col("r.effective_to").alias("restriction_effective_to"),
        F.lit("HIGH").alias("severity"), F.col("t._source_file").alias("_txn_source_file"), F.col("t._row_hash").alias("_txn_row_hash"),
        F.col("r._source_file").alias("_restriction_source_file"), F.lit(run_id).alias("_detected_run_id"), F.current_timestamp().alias("_detected_ts"))
    # slice-replace => self-correcting (cancelled restrictions / corrected trades disappear) and idempotent
    replace_where(out, "gold_compliance_exceptions", f"trade_date >= '{lo}' AND trade_date <= '{D}'")
    n = spark.table(T("gold_compliance_exceptions")).filter((F.col("trade_date") >= lo) & (F.col("trade_date") <= D)).count()
    if n: alert(run_id, "HIGH", "RESTRICTED_TRADES", f"{n} transaction(s) executed under an active restriction (window {lo}..{D})", "compliance")
    return {"exceptions": n}

def observability_stage(D, run_id):
    rows = spark.table(T("recon")).filter(F.col("run_id") == run_id).collect(); m = []
    for r in rows:
        ds = r["dataset"]; rec, rej = r["records_received"], r["records_rejected"]
        rate = 100.0 * rej / rec if rec else 0.0
        m += [(ds, "records_received", rec), (ds, "records_rejected", rej), (ds, "reject_rate_pct", rate),
              (ds, "records_duplicate_in_batch", r["records_duplicate_in_batch"]), (ds, "duplicate_files", r["duplicate_files"]),
              (ds, "schema_rejected_files", r["schema_rejected_files"]), (ds, "recon_balanced", r["balanced"])]
        if not r["balanced"]: alert(run_id, "CRITICAL", "RECON_IMBALANCE", "received != accounted-for records", ds)
        if rate > CFG["max_reject_rate_pct"]: alert(run_id, "WARN", "HIGH_REJECT_RATE", f"{rate:.1f}% rejected", ds)
        if ds in CFG["required_daily"] and rec > 0:   # volume anomaly vs previous business date
            prev = (spark.table(T("metrics")).filter((F.col("metric") == "records_received") & (F.col("dataset") == ds) & (F.col("value") > 0)
                    & (F.col("business_date") < F.lit(D).cast("date"))).orderBy(F.col("business_date").desc(), F.col("metric_ts").desc()).limit(1).collect())
            if prev:
                dev = 100.0 * (rec - prev[0]["value"]) / prev[0]["value"]
                m.append((ds, "volume_deviation_pct", dev))
                if abs(dev) > CFG["volume_dev_pct"]: alert(run_id, "WARN", "VOLUME_ANOMALY", f"{dev:+.0f}% vs previous load", ds)
    mx = spark.table(T("silver_market_prices")).agg(F.max("price_date")).first()[0]
    lag = (_d(D) - mx).days if mx else None
    if lag is not None:
        m.append(("market_prices", "freshness_lag_days", lag))
        if lag > 0: alert(run_id, "WARN", "STALE_PRICES", f"latest price_date is {lag} day(s) behind business date", "market_prices")
    m.append(("gold", "gold_positions_rows", spark.table(T("gold_positions")).filter(F.col("business_date") == D).count()))
    m.append(("quarantine", "quarantined_rows_run", spark.table(T("quarantine")).filter(F.col("run_id") == run_id).count()))
    write_metrics(run_id, D, m)
    return {"metrics": len(m)}

def run_stage(run_id, D, name, fn):
    done = spark.table(T("stage_log")).filter((F.col("run_id") == run_id) & (F.col("stage") == name) & (F.col("status") == "SUCCEEDED")).count() > 0
    if done:
        log_row("stage_log", run_id=run_id, stage=name, status="SKIPPED_ALREADY_SUCCEEDED", ts=now()); return
    log_row("stage_log", run_id=run_id, stage=name, status="STARTED", ts=now()); t0 = time.time()
    try:
        maybe_fail(name); out = fn()
        log_row("stage_log", run_id=run_id, stage=name, status="SUCCEEDED", ts=now(), detail=json.dumps({"sec": round(time.time()-t0, 2), **(out or {})}, default=str)[:4000])
    except PublicationBlocked as e:
        log_row("stage_log", run_id=run_id, stage=name, status="BLOCKED", ts=now(), detail=str(e)); raise
    except Exception as e:
        log_row("stage_log", run_id=run_id, stage=name, status="FAILED", ts=now(), detail=str(e)[:4000]); raise

def run_pipeline(business_date, run_id=None, fail_at=None, cfg=None):
    """run_id=None -> new run. Pass an old run_id to RESUME (succeeded stages are skipped; all stages are idempotent anyway)."""
    global FAIL_AT
    FAIL_AT = fail_at; old = CFG.copy(); CFG.update(cfg or {})
    resumed = run_id is not None; run_id = run_id or f"{business_date}-{uuid.uuid4().hex[:8]}"; t0 = time.time()
    log_row("run_log", run_id=run_id, business_date=business_date, status="RESUMED" if resumed else "STARTED", ts=now())
    D = business_date
    stages = ([("bronze_ingest", lambda: bronze_stage(D, run_id))]
              + [(f"silver_{ds}", (lambda ds=ds: silver_process(ds, D, run_id))) for ds in SILVER_ORDER]
              + [("gold_build", lambda: gold_build_stage(D, run_id)), ("gold_publish", lambda: gold_publish_stage(D, run_id)),
                 ("compliance", lambda: compliance_stage(D, run_id)), ("observability", lambda: observability_stage(D, run_id))])
    status = "SUCCEEDED"
    try:
        for name, fn in stages:
            try:
                run_stage(run_id, D, name, fn)
            except PublicationBlocked:
                status = "BLOCKED"     # gold untouched; keep going so compliance + observability still run
        write_metrics(run_id, D, [("pipeline", "run_duration_sec", time.time() - t0)])
    except Exception as e:
        status = "FAILED"
        alert(run_id, "CRITICAL", "PIPELINE_FAILED", f"{type(e).__name__}: {str(e)[:300]}")
        log_row("run_log", run_id=run_id, business_date=D, status=status, ts=now(), detail=str(e)[:2000])
        raise
    finally:
        CFG.clear(); CFG.update(old); FAIL_AT = None
    log_row("run_log", run_id=run_id, business_date=D, status=status, ts=now())
    print(f"run {run_id}: {status}")
    return {"run_id": run_id, "status": status}

# --- inspection helpers ---
def show_run(run_id):
    for t, order in [("run_log", "ts"), ("stage_log", "ts"), ("recon", "dataset"), ("alerts", "alert_ts")]:
        print(f"== {t} ==")
        display(spark.table(T(t)).filter(F.col("run_id") == run_id).orderBy(order))

def gold_checksum(D):
    r = spark.table(T("gold_positions")).filter(F.col("business_date") == D).agg(
        F.count("*"), F.sum("quantity"), F.sum("market_value")).first()
    return tuple(r)

# COMMAND ----------

# DBTITLE 1,3g. Lineage: trace a gold position (or compliance exception) back to bronze files
def trace_position(portfolio_id, security_id, D):
    g = spark.table(T("gold_positions")).filter((F.col("business_date") == D) & (F.col("portfolio_id") == portfolio_id) & (F.col("security_id") == security_id))
    print("GOLD row"); display(g)
    row = g.first()
    if not row: return
    print("SILVER transactions that contributed")
    tx = spark.table(T("silver_transactions")).filter(F.col("transaction_id").isin(row["contributing_transaction_ids"]))
    display(tx)
    print("SILVER opening position"); display(spark.table(T("silver_positions")).filter((F.col("portfolio_id") == portfolio_id) & (F.col("security_id") == security_id) & (F.col("position_date") == row["_opening_position_date"])))
    print("SILVER price used"); display(spark.table(T("silver_market_prices")).filter((F.col("security_id") == security_id) & (F.col("price_date") == row["price_date"])))
    print("SILVER master version used"); display(spark.table(T("silver_security_master")).filter((F.col("security_id") == security_id) & (F.col("effective_from") == row["_sm_effective_from"])))
    print("BRONZE rows + file manifest for contributing transactions")
    b = spark.table(T("bronze_transactions")).filter(F.col("transaction_id").isin(row["contributing_transaction_ids"]))
    display(b.join(spark.table(T("file_manifest")).select("file_hash", "load_run_id", "loaded_ts", "status"), "_file_hash" if False else b["_file_hash"] == spark.table(T("file_manifest"))["file_hash"]).limit(50))

def trace_compliance_exception(transaction_id):
    e = spark.table(T("gold_compliance_exceptions")).filter(F.col("transaction_id") == transaction_id); display(e)
    display(spark.table(T("bronze_transactions")).filter(F.col("transaction_id") == transaction_id))
    display(spark.table(T("silver_restricted_securities")).join(e.select("restriction_id"), "restriction_id"))

# COMMAND ----------

# DBTITLE 1,4. Security controls (Unity Catalog; each statement wrapped because groups/principals differ per workspace)
# MAGIC %md
# MAGIC **Implemented here:** least-privilege grants by layer, masked ISIN view for non-compliance users, secrets via secret scope (see `alert()`), auditability via append-only control tables + `DESCRIBE HISTORY` (+ `system.access.audit` in production).
# MAGIC
# MAGIC **Production:** service principal owns the job (users cannot write Silver/Gold), Bronze readable only by data-engineering, row filters per portfolio entitlement, column masks via UC (`ALTER TABLE ... SET MASK`), private networking, CMK encryption, Volume ACLs.

# COMMAND ----------

def try_sql(stmt):
    try:
        spark.sql(stmt); print("OK  :", stmt[:110])
    except Exception as e:
        print("SKIP:", stmt[:80], "->", str(e)[:100])

# Replace group names with real account groups.
for stmt in [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `data-consumers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `data-consumers`",
    f"GRANT SELECT ON TABLE {T('gold_position_exceptions')} TO `data-consumers`",
    f"GRANT SELECT ON TABLE {T('gold_compliance_exceptions')} TO `compliance`",
    f"GRANT SELECT, MODIFY ON SCHEMA {CATALOG}.{SCHEMA} TO `pipeline-service-principal`",
    f"REVOKE ALL PRIVILEGES ON TABLE {T('bronze_transactions')} FROM `data-consumers`",
]: try_sql(stmt)

try_sql(f"""CREATE OR REPLACE VIEW {T('gold_positions_secure')} AS
  SELECT business_date, portfolio_id, security_id,
         CASE WHEN is_account_group_member('compliance') THEN isin ELSE concat(substr(isin,1,4),'********') END AS isin,
         asset_class, currency, quantity, price, price_date, price_status, market_value, _run_id
  FROM {T('gold_positions')}""")
try_sql(f"GRANT SELECT ON VIEW {T('gold_positions_secure')} TO `data-consumers`")

# Secrets: never hard-code. Example (requires a secret scope):
# webhook = dbutils.secrets.get(scope="asset-mgmt", key="alert-webhook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation scenarios
# MAGIC Day 1 = `2025-01-15`, Day 2 = `2025-01-16`.

# COMMAND ----------

# DBTITLE 1,5.0 Clean start + SUCCESSFUL RUN (day 1)
D1, D2 = "2025-01-15", "2025-01-16"
reset_all()
generate_day(D1, seed=1)
r1 = run_pipeline(D1)
show_run(r1["run_id"])
display(spark.table(T("gold_positions")).filter(F.col("business_date") == D1).orderBy("portfolio_id", "security_id").limit(20))
display(spark.table(T("quarantine")).groupBy("dataset", F.concat_ws(",", "reasons").alias("reasons")).count().orderBy("dataset"))

# COMMAND ----------

# DBTITLE 1,5.1 DUPLICATE DELIVERY: same transactions twice => positions not double counted
before = gold_checksum(D1)
dup_path = make_exact_duplicate(D1)      # byte-identical file, new name  -> stopped at file-hash manifest
redo_path = make_redelivery(D1)          # overlapping records, new file  -> stopped at business key + row hash in silver
r = run_pipeline(D1)
after = gold_checksum(D1)
print("gold before:", before); print("gold after :", after)
assert before == after, "positions changed after duplicate delivery!"
assert spark.table(T("silver_transactions")).groupBy("transaction_id").count().filter("count > 1").count() == 0
display(spark.table(T("file_events")).filter(F.col("run_id") == r["run_id"]).filter("event != 'NO_FILE'"))
display(spark.table(T("recon")).filter((F.col("run_id") == r["run_id"]) & (F.col("dataset") == "transactions")))
print("PASS: duplicate file skipped; redelivered rows counted as 'redelivered_unchanged'; positions identical.")

# COMMAND ----------

# DBTITLE 1,5.1b CORRECTED RECORD (late correction changes exactly one transaction)
corr_path, corr_txn = make_correction(D1)
before = gold_checksum(D1)
r = run_pipeline(D1)
after = gold_checksum(D1)
print(before, "->", after)
assert before != after and spark.table(T("silver_transactions")).filter(F.col("transaction_id") == corr_txn).first()["_version"] == 2
display(spark.table(T("recon")).filter((F.col("run_id") == r["run_id"]) & (F.col("dataset") == "transactions")))

# COMMAND ----------

# DBTITLE 1,5.2 MISSING PRICE: held out of Gold, surfaced as exception; gate blocks if too many
exc = spark.table(T("gold_position_exceptions")).filter(F.col("business_date") == D1)
display(exc.groupBy("reason", "security_id").count())
assert exc.filter("security_id = 'SEC0007' AND reason = 'PRICE_MISSING'").count() > 0
assert spark.table(T("gold_positions")).filter((F.col("business_date") == D1) & (F.col("security_id") == "SEC0007")).count() == 0
print("Stale-but-usable prices are published flagged STALE:")
display(spark.table(T("gold_positions")).filter((F.col("business_date") == D1) & (F.col("price_status") == "STALE")).select("portfolio_id", "security_id", "price_date", "price_status", "market_value").limit(10))

# Now tighten the gate -> publication is BLOCKED, gold untouched
before = gold_checksum(D1)
rb = run_pipeline(D1, cfg={"max_unpriced_pct": 1.0})
assert rb["status"] == "BLOCKED" and gold_checksum(D1) == before
display(spark.table(T("alerts")).filter(F.col("run_id") == rb["run_id"]))
print("PASS: with a 1% threshold the run is BLOCKED; previously published gold unchanged.")

# COMMAND ----------

# DBTITLE 1,5.3 RESTRICTED SECURITY: traceable compliance exception (and correct negatives)
ce = spark.table(T("gold_compliance_exceptions"))
display(ce)
assert ce.count() == 1 and ce.first()["transaction_id"] == f"TXN-{D1}-RESTRICTED-BUY"
print("PASS: only the BUY on SEC0003 (NO_BUY, active) is flagged. SELL (wrong side), SEC0004 (restriction ended D-1), SEC0005 (INACTIVE) are not.")
trace_compliance_exception(f"TXN-{D1}-RESTRICTED-BUY")

# COMMAND ----------

# DBTITLE 1,5.4 MID-RUN FAILURE: interrupt, prove no partial publication, replay safely
generate_day(D2, include_positions=False, seed=2)    # day 2 rolls forward from day-1 opening snapshot + transactions
try:
    run_pipeline(D2, fail_at="silver_transactions_after_merge")   # silver merged, manifest NOT yet marked done
except InjectedFailure as e:
    print("Controlled failure:", e)
failed_run = spark.table(T("run_log")).filter(F.col("business_date") == D2).orderBy(F.col("ts").desc()).first()["run_id"]
show_run(failed_run)
assert spark.table(T("gold_positions")).filter(F.col("business_date") == D2).count() == 0, "partial publication!"
print("silver_transactions rows for D2 already merged:", spark.table(T("silver_transactions")).filter(F.col("trade_date") == D2).count())

# Replay with SAME run_id: succeeded stages skipped, failed stage re-run, result identical to an uninterrupted run.
rr = run_pipeline(D2, run_id=failed_run)
show_run(failed_run)
assert spark.table(T("gold_positions")).filter(F.col("business_date") == D2).count() > 0
assert spark.table(T("silver_transactions")).groupBy("transaction_id").count().filter("count > 1").count() == 0
assert spark.table(T("recon")).filter((F.col("run_id") == failed_run) & (~F.col("balanced"))).count() == 0
print("PASS: failure -> no gold rows; resume -> published once; no duplicates; recon balanced.")

# COMMAND ----------

# DBTITLE 1,5.5 SCHEMA DRIFT: additive change tolerated, breaking change blocks publication
make_schema_drift(D2, "extra_col")
make_schema_drift(D2, "missing_col")
before = gold_checksum(D2)
rs = run_pipeline(D2)
display(spark.table(T("file_events")).filter((F.col("run_id") == rs["run_id"]) & F.col("event").isin("SCHEMA_REJECTED", "LOADED")))
display(spark.table(T("alerts")).filter(F.col("run_id") == rs["run_id"]))
print(rs["status"], "(BLOCKED expected: required dataset had a breaking schema change); gold unchanged:", gold_checksum(D2) == before)

# COMMAND ----------

# DBTITLE 1,5.6 Lineage demo: trace one gold position back to source files
row = spark.table(T("gold_positions")).filter((F.col("business_date") == D2) & (F.col("txn_count") > 0)).first()
trace_position(row["portfolio_id"], row["security_id"], D2)

# COMMAND ----------

# DBTITLE 1,5.7 Observability snapshot
display(spark.table(T("metrics")).filter(F.col("business_date") == D1).orderBy("dataset", "metric"))
display(spark.table(T("alerts")).orderBy(F.col("alert_ts").desc()).limit(30))
display(spark.sql(f"DESCRIBE HISTORY {T('gold_positions')}").select("version", "timestamp", "operation", "operationParameters", "operationMetrics").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Volume test
# MAGIC Runs the real `apply_dq` + `dedupe_latest` + `build_gold` functions on progressively larger synthetic data (no I/O to tables, results sent to the `noop` sink so the full plan executes). Adjust `SIZES` to your cluster.

# COMMAND ----------

SIZES = [100_000, 1_000_000, 5_000_000]    # transactions; positions = n/2
S, P = 5000, 2000                          # securities, portfolios
BD = "2025-01-15"

def bench_inputs(n):
    ids = F.col("id")
    sm = (spark.range(S).select(F.concat(F.lit("SEC"), F.lpad((ids + 1).cast("string"), 4, "0")).alias("security_id"),
            F.concat(F.lit("US"), F.lpad((ids + 1).cast("string"), 9, "0"), F.lit("0")).alias("isin"), F.lit("EQUITY").alias("asset_class"),
            F.lit("USD").alias("currency"), F.lit("2020-01-01").cast("date").alias("effective_from"), F.lit(None).cast("date").alias("effective_to"),
            F.lit("bench_sm").alias("_source_file")))
    uniq = n - n // 100                    # last 1% of ids repeat => ~1% duplicates
    tb = (spark.range(n).select(F.concat(F.lit("TXN"), (ids % uniq).cast("string")).alias("transaction_id"),
            F.concat(F.lit("P"), (ids % P).cast("string")).alias("portfolio_id"),
            F.concat(F.lit("SEC"), F.lpad(((ids * 7) % S + 1).cast("string"), 4, "0")).alias("security_id"),
            F.lit(BD).alias("trade_date"), F.when(ids % 2 == 0, "BUY").otherwise("SELL").alias("side"),
            ((ids % 100) + 1).cast("string").alias("quantity"), (F.lit(50) + ids % 100).cast("string").alias("price"), F.lit("USD").alias("currency"),
            F.lit(None).cast("string").alias("_extra_json"), F.lit("bench").alias("_source_file"), F.lit("h").alias("_file_hash"),
            F.sha2(F.concat(F.lit("TXN"), (ids % uniq).cast("string")), 256).alias("_row_hash"), F.current_timestamp().alias("_delivery_ts")))
    op = (spark.range(n // 2).select(F.concat(F.lit("P"), (ids % P).cast("string")).alias("portfolio_id"),
            F.concat(F.lit("SEC"), F.lpad((((ids / P).cast("long")) % S + 1).cast("string"), 4, "0")).alias("security_id"),
            F.lit(BD).cast("date").alias("position_date"), F.lit(100).cast("decimal(28,8)").alias("quantity"),
            F.lit(1000).cast("decimal(28,2)").alias("book_value"), F.lit("bench").alias("_source_file")))
    px = (spark.range(S).select(F.concat(F.lit("SEC"), F.lpad((ids + 1).cast("string"), 4, "0")).alias("security_id"),
            F.lit(BD).cast("date").alias("price_date"), F.lit(100).cast("decimal(28,8)").alias("price"), F.lit("USD").alias("price_currency"),
            F.lit("V").alias("price_source"), F.lit("bench").alias("_source_file"), F.current_timestamp().alias("_delivery_ts")))
    return sm, tb, op, px

results = []
for n in SIZES:
    sm, tb, op, px = bench_inputs(n)
    t0 = time.time()
    tagged, _ = apply_dq("transactions", tb, sm, BD)
    clean = dedupe_latest(to_silver_shape(tagged.filter(F.size("_reasons") == 0), "transactions", "bench"), ["transaction_id"]).filter("_rn = 1").drop("_rn")
    clean.write.format("noop").mode("overwrite").save(); t_dq = time.time() - t0
    t0 = time.time()
    txn_typed = clean.filter("trade_date = '%s'" % BD)
    pub, exc = build_gold(op, txn_typed, sm, px, BD, "bench", 3, BD)
    pub.write.format("noop").mode("overwrite").save(); t_gold = time.time() - t0
    results.append((n, round(t_dq, 1), round(t_gold, 1), round(n / (t_dq + t_gold))))
    print(results[-1])
display(spark.createDataFrame(results, "txn_rows INT, dq_dedupe_sec DOUBLE, gold_build_sec DOUBLE, rows_per_sec LONG"))

# COMMAND ----------

# MAGIC %md
# MAGIC **How to read the result (fill in with your numbers):**
# MAGIC * Expect near-linear scaling at small sizes; fixed overhead (job startup, small partitions) dominates below ~1M rows.
# MAGIC * **Likely bottlenecks in order:** (1) shuffle in the `row_number()` dedupe window and in the `(portfolio, security)` full-outer join / group-by; (2) key skew (a few heavy portfolios/securities); (3) small-file proliferation from per-file Bronze appends and frequent MERGEs; (4) MERGE write amplification on Silver; (5) driver-side file hashing (`sha256_file`) and `os.listdir` at high file counts.
# MAGIC * **Scaling plan:** Auto Loader (`cloudFiles`) with checkpoints + `trigger(availableNow=True)` for Bronze (replaces driver-side listing/hashing; keep content hash for dup detection); broadcast the small dimensions (already done for master/prices); tune `spark.sql.shuffle.partitions` / AQE skew join; liquid clustering (or Z-ORDER) on merge keys, partition Gold by `business_date`; `OPTIMIZE` + auto-compaction + optimized writes; deletion vectors for cheaper MERGE; incremental Gold (only affected portfolio/security keys) instead of full-day recompute; photon + autoscaling job cluster; split per-dataset Silver stages into parallel Workflow tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Engineering decision record (short)
# MAGIC
# MAGIC | Topic | Decision | Why / trade-off |
# MAGIC |---|---|---|
# MAGIC | **Keys** | SM: `(security_id, effective_from)`; Txn: `transaction_id`; Pos: `(portfolio_id, security_id, position_date)`; Price: `(security_id, price_date)`; Restriction: `restriction_id` (added) | Natural business keys drive MERGE. Single price source assumed; production adds `price_source` priority rules. |
# MAGIC | **Idempotency** | 3 layers: (1) SHA-256 file hash manifest stops identical files; (2) in-batch dedupe + MERGE on business key with `_row_hash` stops overlapping redeliveries and applies corrections (newer delivery wins, older ignored, `_version` incremented); (3) Gold/staging/compliance written by `replaceWhere` slice overwrite | Any stage can be re-run without double counting. Bronze delete-by-hash-then-append makes crash-replay safe. |
# MAGIC | **Late / corrected data** | Corrections update Silver and Gold is recomputed for the business date; compliance re-evaluates a 7-day lookback window; positions roll forward from latest opening snapshot + transactions | Late data older than the lookback needs a backfill run (`run_pipeline(date)` is safe to execute for any past date). |
# MAGIC | **Quarantine vs warn** | Structural/technical and referential failures are hard-rejected to `quarantine` with reason codes. Missing/invalid price for a position: position is *held out* of Gold (exception table), stale price within N days is published flagged `STALE`, and a gate blocks publication if held-out % exceeds threshold | Never publish a market value that isn't trustworthy; never silently drop either. |
# MAGIC | **Temporal logic** | Security master and restrictions are effective-dated; as-of lookups at trade/position/business date (`add_ref_flag`, `sm_asof`, restriction join) | Array-of-ranges check avoids range-join row explosion. Overlapping master versions are not validated (production would). |
# MAGIC | **Schema evolution** | Explicit schemas; Bronze all-strings. Extra columns -> kept in `_extra_json` + WARN; missing required column -> file rejected + CRITICAL + gate blocks publication | Predictable, no silent coercion. Production: schema registry / data contracts. |
# MAGIC | **Recovery** | Stage log + same-`run_id` resume skips succeeded stages; staging -> gate -> atomic publish; failure injection points at stage boundaries and mid-stage | No partial publication: Gold is only written in one Delta transaction after validation. |
# MAGIC | **Security** | UC grants by layer, masked view, secret scope for webhooks, append-only audit tables, Delta history | See section 4 for production extras (service principals, row filters, CMK, private link). |
# MAGIC | **Monitoring** | `metrics`, `alerts`, `recon`, `dq_results`: freshness, volume deviation, reject rate, recon balance, gate result, run duration, missing file | Production: Lakehouse Monitoring / SQL alerts, webhook to on-call. |
# MAGIC | **Reconciliation identity** | `received = rejected + duplicate_in_batch + new + corrected + redelivered_unchanged + ignored_out_of_order` per dataset per run | Imbalance = CRITICAL alert + gate failure. |
# MAGIC
# MAGIC **Implemented in this prototype:** everything above, on synthetic data.
# MAGIC **Simplifications / would change for production:** driver-side hashing & listing -> Auto Loader; FX conversion (market value is in local currency; currency mismatch is held out); single price vendor; no Luhn check on ISIN; no overlapping-effective-date validation on master; single concurrent run per business date assumed (use job concurrency = 1 or a lock row); Gold recomputed fully per day rather than incrementally; Lakeflow/Workflows for scheduling, retries and alert routing; CI tests (pytest on the pure functions `apply_dq`, `build_gold`).
