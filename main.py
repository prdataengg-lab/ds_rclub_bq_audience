import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from collections import defaultdict

import requests
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout
)
log = logging.getLogger(__name__)

MEASUREMENT_ID = "G-V5JJBKWVGX"
API_SECRET = "l1lTY3TgR2aA2OUez_jBbw"
BQ_PROJECT = "dsgroup-havas-csa"
THROTTLE_SECONDS = float(0.05)

GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"


def fetch_from_bigquery() -> list[dict]:
    client = bigquery.Client(project=BQ_PROJECT)
    query = """
        with base as(
        select
        ds_group_user_id,
        user_pseudo_id,
        min(event_date) min_event_date,
        max(event_date) max_event_date,
        string_agg(distinct event_name ,"||") event_name
        FROM `dsgroup-havas-csa.ga4_silver.slv_events_flat` a
        where stream_id='13089212357'
        and event_name in ("page_view")
        and(lower(operating_system) = 'android' or lower(browser) = 'chrome' )
        and event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                                AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
         
        group by 1,2
        )
        select
        ds_group_user_id,
        user_pseudo_id,
        "PageView_30_Test" as event_name,
         
        from base
    """
    log.info("Running BQ query...")
    rows = [dict(row) for row in client.query(query).result()]
    log.info(f"Fetched {len(rows)} rows")
    return rows


def send_event(session: requests.Session, row: dict, index: int) -> dict:
    user_pseudo_id = row["user_pseudo_id"]
    ds_group_user_id = str(row["ds_group_user_id"])
    timestamp_micros = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    payload = {
        "client_id": user_pseudo_id,
        "user_id": ds_group_user_id,
        "timestamp_micros": timestamp_micros,
        "events": [{
            "name": "ATC_90_Days_Test",
            "params": {
                "engagement_time_msec": 100
            }
        }]
    }

    url = f"{GA4_ENDPOINT}?measurement_id={MEASUREMENT_ID}&api_secret={API_SECRET}"

    try:
        resp = session.post(url, json=payload, timeout=10)

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


def print_summary(results: list[dict], start_time: datetime):
    total = len(results)
    success = [r for r in results if r["status"] == "success"]
    fail = [r for r in results if r["status"] == "fail"]
    error = [r for r in results if r["status"] == "error"]

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

    # group errors by http status
    http_status_counts = defaultdict(int)
    for r in fail:
        http_status_counts[r.get("http_status", "unknown")] += 1

    # group exceptions by type
    error_type_counts = defaultdict(int)
    for r in error:
        err_str = r.get("error", "unknown")
        err_type = err_str.split("(")[0]  # e.g. "ConnectionError"
        error_type_counts[err_type] += 1

    log.info("=" * 60)
    log.info("               PUSH SUMMARY REPORT")
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
        for status, count in sorted(http_status_counts.items()):
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


def main():
    start_time = datetime.now(timezone.utc)
    rows = fetch_from_bigquery()
    results = []

    with requests.Session() as session:
        for index, row in enumerate(rows):
            result = send_event(session, row, index)
            results.append(result)
            time.sleep(THROTTLE_SECONDS)

    print_summary(results, start_time)

    fail_count = sum(1 for r in results if r["status"] in ("fail", "error"))
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()