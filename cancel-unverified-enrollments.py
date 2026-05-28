from __future__ import annotations
import os
import copy
import sys
import argparse
import calendar
from datetime import datetime, timedelta
from dhis2 import Api, RequestException

CONFIG_DIR = "./config"
ZEBRA_AUTH = os.path.join(CONFIG_DIR, "zebra_auth.json")
ZEBRA_PROG = "MQtbs8UkBxy"
VERIFICATION_STATUS = "HvgldgBK8Th"
VERIFIED_CODE = "RTSL_ZEB_AL_OS_VERIFICATION_VERIFIED"


def check_auth(api, name):
    try:
        _ = api.version
        return True
    except RequestException as e:
        if e.code == 401:
            print(f"ERROR: Credentials for {name} are incorrect (401 Unauthorized).")
        else:
            print(f"ERROR: Could not connect to {name} server (code {e.code}).")
        return False


def get_all_enrollments(api, params):
    all_instances = []
    page = 1
    page_size = 50
    while True:
        print(f"Fetching enrollments page {page}...")
        current_params = copy.deepcopy(params)
        current_params.update({"page": page, "pageSize": page_size, "totalPages": "false"})
        resp_data = api.get("tracker/enrollments", params=current_params).json()
        instances = resp_data.get("instances", resp_data.get("enrollments", []))
        if not instances:
            break
        all_instances.extend(instances)
        if len(instances) < page_size:
            break
        page += 1
    return all_instances


def get_tei(api, tei_id):
    try:
        return api.get(f"tracker/trackedEntities/{tei_id}", params={"fields": "*"}).json()
    except RequestException as e:
        print(f"  WARNING: Could not fetch TEI {tei_id} (code {e.code}).")
        return None


def cancel_enrollment(api, enrollment):
    enrollment_id = enrollment["enrollment"]
    payload = {
        "enrollments": [
            {
                "enrollment": enrollment_id,
                "trackedEntity": enrollment.get("trackedEntity"),
                "program": enrollment.get("program"),
                "orgUnit": enrollment.get("orgUnit"),
                "enrolledAt": enrollment.get("enrolledAt"),
                "occurredAt": enrollment.get("occurredAt"),
                "status": "CANCELLED",
            }
        ]
    }
    try:
        resp = api.post("tracker", data=payload, params={"async": "false"}).json()
        if resp.get("status") == "OK":
            print(f"  Cancelled '{enrollment_id}'.")
            return True
        errors = resp.get("validationReport", {}).get("errorReports", [])
        for err in errors:
            print(f"  Validation error: {err.get('message', err)}")
        print(f"  Cancel request returned status: {resp.get('status', 'UNKNOWN')}")
        return False
    except RequestException as e:
        print(f"  ERROR: Failed to cancel '{enrollment_id}' (code {e.code}): {e}")
        return False


def run(period="today", date=None):
    now = datetime.now()

    if period == "today":
        ref = now.date()
    elif period == "yesterday":
        ref = (now - timedelta(days=1)).date()
    elif period == "this_week":
        ref = (now - timedelta(days=now.weekday())).date()
    elif period == "custom":
        ref = datetime.strptime(date, "%Y-%m-%d").date()
    else:
        ref = None

    if ref:
        start_date = ref.replace(day=1).strftime("%Y-%m-%d")
        end_date = ref.replace(day=calendar.monthrange(ref.year, ref.month)[1]).strftime("%Y-%m-%d")
        month_label = ref.strftime("%Y-%m")
    else:
        start_date, end_date, month_label = "1900-01-01", None, "all_time"

    print(f"\n--- CANCEL UNVERIFIED ENROLLMENTS (Period: {period}, Month: {month_label}, Run: {now.strftime('%d %B %Y %H:%M')}) ---")

    api = Api.from_auth_file(ZEBRA_AUTH)
    if not check_auth(api, "Zebra"):
        sys.exit(1)

    print(f"Fetching enrollments for program {ZEBRA_PROG}, month {month_label}...")
    enr_params = {"program": ZEBRA_PROG, "ouMode": "ALL", "enrolledAfter": start_date}
    if end_date:
        enr_params["enrolledBefore"] = end_date
    enrollments = get_all_enrollments(api, enr_params)
    print(f"Retrieved {len(enrollments)} enrollment(s).")

    if not enrollments:
        print("No enrollments found for the given period. Nothing to do.")
        return

    cancelled = 0
    skipped_already_cancelled = 0
    skipped_verified = 0
    errors = 0

    for i, enrollment in enumerate(enrollments, 1):
        enrollment_id = enrollment.get("enrollment", "?")
        print(f"\n[{i}/{len(enrollments)}] Processing enrollment '{enrollment_id}'...")

        if enrollment.get("status") == "CANCELLED":
            print(f"  Already CANCELLED. Skipping.")
            skipped_already_cancelled += 1
            continue

        tei_id = enrollment.get("trackedEntity")
        tei = get_tei(api, tei_id)
        if not tei:
            errors += 1
            continue

        raw_value = next(
            (a.get("value") for a in tei.get("attributes", []) if a["attribute"] == VERIFICATION_STATUS),
            None,
        )

        print(f"  Verification Status code = '{raw_value}'")

        if raw_value == VERIFIED_CODE:
            print(f"  Verified. Skipping.")
            skipped_verified += 1
            continue

        if cancel_enrollment(api, enrollment):
            cancelled += 1
        else:
            errors += 1

    print(f"\n--- DONE ---")
    print(f"  Cancelled:              {cancelled}")
    print(f"  Skipped (verified):     {skipped_verified}")
    print(f"  Skipped (already cancelled): {skipped_already_cancelled}")
    print(f"  Errors:                 {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cancel all unverified ZEBRA enrollments for a given month."
    )
    parser.add_argument(
        "-p", "--period",
        choices=["today", "yesterday", "this_week", "all_time", "custom"],
        default="today",
        help="Time period to process (default: today)",
    )
    parser.add_argument("-d", "--date", help="Reference date for custom period (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.period == "custom" and not args.date:
        parser.error("--date is required when --period is custom")

    run(period=args.period, date=args.date)
