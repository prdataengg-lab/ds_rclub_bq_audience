"""
GA4 Measurement Protocol event pusher (multi-brand).

Brand configs live in ./configs/<brand>.json; each config contains its own
SQL query (as a JSON array of lines) plus all GA4/BigQuery settings.
The GA4 API secret is fetched from GCP Secret Manager at runtime
(config key: ga4.api_secret_name). All settings come from the config file
only — no environment variable overrides.

Usage:
    python ga4_event_pusher.py --brand rclub
    python ga4_event_pusher.py --brand rajnigandha
    python ga4_event_pusher.py --config path/to/custom.json   # explicit file
"""

import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import requests
from google.cloud import bigquery
from google.cloud import secretmanager

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = SCRIPT_DIR / "configs"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def available_brands() -> list[str]:
    if not CONFIGS_DIR.is_dir():
        return []
    return sorted(p.stem for p in CONFIGS_DIR.glob("*.json"))


def resolve_config_path(brand: str | None, config: str | None) -> Path:
    """--config wins if given; otherwise map --brand to configs/<brand>.json."""
    if config:
        return Path(config)

    path = CONFIGS_DIR / f"{brand.lower()}.json"
    if not path.is_file():
        brands = ", ".join(available_brands()) or "none found"
        raise FileNotFoundError(
            f"No config for brand '{brand}'. Available brands: {brands}"
        )
    return path


def load_config(config_path: Path) -> dict:
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")

    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Query lives directly in the JSON config.
    # It can be a plain string, or a list of lines (easier to read/edit in JSON).
    query = cfg["bigquery"].get("query")
    if isinstance(query, list):
        query = "\n".join(query)
    cfg["bigquery"]["query_sql"] = query

    validate_config(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    required = [
        ("ga4", "measurement_id"),
        ("ga4", "api_secret_name"),
        ("ga4", "endpoint"),
        ("bigquery", "project"),
        ("bigquery", "query_sql"),
        ("event", "name"),
    ]
    missing = [".".join(keys) for keys in required if not cfg.get(keys[0], {}).get(keys[1])]
    if missing:
        raise ValueError(f"Missing required config values: {', '.join(missing)}")

    placeholders = json.dumps(cfg)
    if "REPLACE_WITH" in placeholders or "G-XXXXXXXXXX" in placeholders:
        raise ValueError(
            "Config still contains placeholder values (REPLACE_WITH_... / G-XXXXXXXXXX). "
            "Fill them in before running."
        )


def access_gcp_secret(path):
    if not path:
        return None
    if "/versions/" not in path:
        path = f"{path}/versions/latest"
    try:
        client_options = {}
        if "locations/" in path:
            location = path.split("/")[3]
            client_options = {"api_endpoint": f"secretmanager.{location}.rep.googleapis.com"}

        client = secretmanager.SecretManagerServiceClient(client_options=client_options)
        response = client.access_secret_version(request={"name": path})
        payload = response.payload.data.decode("utf-8").strip()
        try:
            return json.loads(payload)
        except Exception:
            return payload
    except Exception as e:
        log.error(f"Secret Error: {e}")
        return None


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

def fetch_from_bigquery(cfg: dict) -> list[dict]:
    bq_cfg = cfg["bigquery"]
    params = bq_cfg.get("query_params", {})

    client = bigquery.Client(project=bq_cfg["project"])

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("stream_id", "STRING", params.get("stream_id")),
            bigquery.ScalarQueryParameter("event_date_from", "STRING", params.get("event_date_from")),
            bigquery.ScalarQueryParameter("operating_system", "STRING", params.get("operating_system")),
            bigquery.ScalarQueryParameter("browser", "STRING", params.get("browser")),
            bigquery.ScalarQueryParameter("event_name", "STRING", cfg["event"]["name"]),
            bigquery.ScalarQueryParameter("row_limit", "INT64", params.get("row_limit", 100)),
        ]
    )

    log.info("Running BQ query from file...")
    rows = [dict(row) for row in client.query(bq_cfg["query_sql"], job_config=job_config).result()]
    log.info(f"Fetched {len(rows)} rows")
    return rows


# ---------------------------------------------------------------------------
# GA4 push
# ---------------------------------------------------------------------------

def send_event(session: requests.Session, row: dict, index: int, cfg: dict, api_secret: str) -> dict:
    ga4 = cfg["ga4"]
    event = cfg["event"]
    timeout = cfg["run"]["request_timeout_seconds"]

    user_pseudo_id = row["user_pseudo_id"]
    ds_group_user_id = str(row["ds_group_user_id"])
    timestamp_micros = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    payload = {
        "client_id": user_pseudo_id,
        "user_id": ds_group_user_id,
        "timestamp_micros": timestamp_micros,
        "events": [{
            "name": event["name"],
            "params": {
                "engagement_time_msec": event.get("engagement_time_msec", 100)
            }
        }]
    }

    url = f"{ga4['endpoint']}?measurement_id={ga4['measurement_id']}&api_secret={api_secret}"

    try:
        resp = session.post(url, json=payload, timeout=timeout)

        if resp.status_code == 204:
            log.info(f"[OK]   idx={index} | client_id={user_pseudo_id} | user_id={ds_group_user_id}")
            return {"status": "success", "client_id": user_pseudo_id, "user_id": ds_group_user_id}

        log.error(
            f"[FAIL] idx={index} | client_id={user_pseudo_id} | user_id={ds_group_user_id} | http={resp.status_code}")
        return {"status": "fail", "client_id": user_pseudo_id, "user_id": ds_group_user_id,
                "http_status": resp.status_code}

    except Exception as e:
        log.error(f"[ERROR] idx={index} | client_id={user_pseudo_id} | user_id={ds_group_user_id} | err={repr(e)}")
        return {"status": "error", "client_id": user_pseudo_id, "user_id": ds_group_user_id, "error": repr(e)}


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(results: list[dict], start_time: datetime, brand: str):
    total = len(results)
    if total == 0:
        log.warning("No rows fetched — nothing was sent.")
        return

    success = [r for r in results if r["status"] == "success"]
    fail = [r for r in results if r["status"] == "fail"]
    error = [r for r in results if r["status"] == "error"]

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

    http_status_counts = defaultdict(int)
    for r in fail:
        http_status_counts[r.get("http_status", "unknown")] += 1

    error_type_counts = defaultdict(int)
    for r in error:
        err_str = r.get("error", "unknown")
        error_type_counts[err_str.split("(")[0]] += 1

    log.info("=" * 60)
    log.info(f"          PUSH SUMMARY REPORT — {brand.upper()}")
    log.info("=" * 60)
    log.info(f"  Run Time         : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"  Elapsed          : {elapsed:.1f}s")
    log.info(f"  Total Users      : {total}")
    log.info(f"  Success (204)    : {len(success)}  ({100 * len(success) / total:.1f}%)")
    log.info(f"  Failed (non-204) : {len(fail)}  ({100 * len(fail) / total:.1f}%)")
    log.info(f"  Errors (exc)     : {len(error)}  ({100 * len(error) / total:.1f}%)")

    if http_status_counts:
        log.info("-" * 60)
        log.info("  HTTP Failure Breakdown:")
        for status, count in sorted(http_status_counts.items(), key=lambda x: str(x[0])):
            log.info(f"    HTTP {status} : {count} users")

    if error_type_counts:
        log.info("-" * 60)
        log.info("  Exception Breakdown:")
        for err_type, count in sorted(error_type_counts.items()):
            log.info(f"    {err_type} : {count} occurrences")

    if fail:
        log.info("-" * 60)
        log.info("  Failed client_ids (first 10):")
        for r in fail[:10]:
            log.info(f"    client_id={r['client_id']} | user_id={r['user_id']} | http={r.get('http_status')}")

    if error:
        log.info("-" * 60)
        log.info("  Errored client_ids (first 10):")
        for r in error[:10]:
            log.info(f"    client_id={r['client_id']} | user_id={r['user_id']} | err={r.get('error')}")

    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Push GA4 Measurement Protocol events from BigQuery")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--brand", choices=available_brands() or None,
                       help="Brand to run (loads configs/<brand>.json)")
    group.add_argument("--config", help="Explicit path to a config file (overrides --brand)")
    args = parser.parse_args()

    config_path = resolve_config_path(args.brand, args.config)
    cfg = load_config(config_path)
    setup_logging(cfg["run"].get("log_level", "INFO"))

    brand = args.brand or config_path.stem
    log.info(f"Brand={brand} | config={config_path} | event={cfg['event']['name']} | project={cfg['bigquery']['project']}")

    api_secret = access_gcp_secret(cfg["ga4"]["api_secret_name"])
    if not api_secret:
        log.error("Could not fetch GA4 API secret from Secret Manager — aborting.")
        sys.exit(1)
    if isinstance(api_secret, dict):
        # Secret stored as JSON — expect the value under key "api_secret"
        api_secret = api_secret.get("api_secret")
        if not api_secret:
            log.error('Secret payload is JSON but has no "api_secret" key — aborting.')
            sys.exit(1)

    start_time = datetime.now(timezone.utc)
    rows = fetch_from_bigquery(cfg)
    results = []

    throttle = cfg["run"].get("throttle_seconds", 0.05)

    with requests.Session() as session:
        for index, row in enumerate(rows):
            results.append(send_event(session, row, index, cfg, api_secret))
            time.sleep(throttle)

    print_summary(results, start_time, brand)

    fail_count = sum(1 for r in results if r["status"] in ("fail", "error"))
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()