import os
import sys
import json
import time
import logging
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout
)
log = logging.getLogger(__name__)

MEASUREMENT_ID      = "G-V5JJBKWVGX"
API_SECRET          = "l1lTY3TgR2aA2OUez_jBbw"
BQ_PROJECT          = "dsgroup-havas-csa"
USE_VALIDATION_MODE = "false"
THROTTLE_SECONDS    = float(0.05)

GA4_ENDPOINT       = "https://www.google-analytics.com/mp/collect"
GA4_VALIDATION_URL = "https://www.google-analytics.com/debug/mp/collect"


def fetch_from_bigquery() -> list[dict]:
    client = bigquery.Client(project=BQ_PROJECT)
    query  = """
        with base as(
            select
                ds_group_user_id,
                user_pseudo_id,
                min(event_date) min_event_date,
                max(event_date) max_event_date,
                string_agg(distinct event_name ,"||") event_name
            FROM `dsgroup-havas-csa.ga4_silver.slv_events_flat` a
            where stream_id='13089212357'
                and event_name in ("add_to_cart","purchase")
                and event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            
            group by 1,2
            )
            select
                ds_group_user_id,
                user_pseudo_id,
                "ATC_90_Days_Test" as event_name
            from base
            where event_name not like "%purchase%"
    """
    log.info("Running BQ query...")
    rows = [dict(row) for row in client.query(query).result()]
    log.info(f"Fetched {len(rows)} rows")
    return rows


def send_event(session: requests.Session, row: dict, index: int) -> bool:
    user_pseudo_id   = row["user_pseudo_id"]
    ds_group_user_id = row["ds_group_user_id"]
    timestamp_micros = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    payload = {
        "client_id":        user_pseudo_id,
        "user_id":          ds_group_user_id,
        "timestamp_micros": timestamp_micros,
        "events": [{"name": "ATC_90_Days_Test", "params": {"engagement_time_msec": "100"}}]
    }

    endpoint = GA4_VALIDATION_URL if USE_VALIDATION_MODE else GA4_ENDPOINT
    url      = f"{endpoint}?measurement_id={MEASUREMENT_ID}&api_secret={API_SECRET}"

    try:
        resp = session.post(url, json=payload, timeout=10)

        if USE_VALIDATION_MODE and resp.status_code == 200:
            messages = resp.json().get("validationMessages", [])
            if not messages:
                log.info(f"[VALID]  idx={index} user={user_pseudo_id}")
                return True
            log.warning(f"[INVALID] idx={index} user={user_pseudo_id} msgs={json.dumps(messages)}")
            return False

        if resp.status_code == 204:
            log.info(f"[OK] idx={index} user={user_pseudo_id}")
            return True

        log.error(f"[FAIL] idx={index} user={user_pseudo_id} status={resp.status_code}")
        return False

    except Exception as e:
        log.error(f"[ERROR] idx={index} user={user_pseudo_id} err={repr(e)}")
        return False


def main():
    rows    = fetch_from_bigquery()
    success = 0
    fail    = 0

    with requests.Session() as session:
        for index, row in enumerate(rows):
            if send_event(session, row, index):
                success += 1
            else:
                fail += 1
            time.sleep(THROTTLE_SECONDS)

    log.info(f"Done. Success={success} Fail={fail} Total={success+fail}")
    sys.exit(0 if fail == 0 else 1)  # non-zero exit marks job as failed in Cloud Run


if __name__ == "__main__":
    main()