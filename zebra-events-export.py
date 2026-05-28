from __future__ import annotations
import os
import copy
import csv
import sys
import argparse
import calendar
from datetime import datetime, timedelta
from dhis2 import Api, RequestException

# ----------------------------
# Constants & Paths
# ----------------------------
CONFIG_DIR = "./config"
ZEBRA_AUTH = os.path.join(CONFIG_DIR, "zebra_auth.json")

# ZEBRA program ID — both eIDSR programs (EBS: JRuLW57woOB, IBS: xDsAFnQMmeU) map here
ZEBRA_PROG = "MQtbs8UkBxy"

VERIFICATION_STATUS = "HvgldgBK8Th"
VERIFIED_CODE = "RTSL_ZEB_AL_OS_VERIFICATION_VERIFIED"
# ----------------------------
# 1. Auth
# ----------------------------

def check_auth(api, name):
    try:
        _ = api.version
        return True
    except RequestException as e:
        if e.code == 401:
            print(f"ERROR: Credentials for {name} are incorrect (401 Unauthorized).")
        else:
            print(f"ERROR: Could not connect to {name} server (Code: {e.code}).")
        return False


# ----------------------------
# 2. Paginated Fetch
# ----------------------------

def get_all_enrollments(api, params):
    """Fetches enrollments page-by-page. enrolledAfter is correctly supported on this endpoint."""
    all_instances = []
    page = 1
    page_size = 50
    while True:
        print(f'Fetching enrollments for page {page}')
        current_params = copy.deepcopy(params)
        current_params.update({'page': page, 'pageSize': page_size, 'totalPages': 'false'})
        resp_data = api.get('tracker/enrollments', params=current_params).json()
        instances = resp_data.get('instances', resp_data.get('enrollments', []))
        if not instances:
            break
        all_instances.extend(instances)
        if len(instances) < page_size:
            break
        page += 1
    print(f'Done fetching enrollments for {page} page(s)')
    return all_instances


def get_tei(api, tei_id):
    """Fetches a single tracked entity with all fields, including attributes."""
    try:
        return api.get(f'tracker/trackedEntities/{tei_id}', params={'fields': '*'}).json()
    except RequestException as e:
        print(f"  Warning: could not fetch TEI {tei_id} (code {e.code})")
        return {}


def get_program_name(api, program_id):
    """Returns the display name of a program, falling back to the ID on error."""
    try:
        return api.get(f'programs/{program_id}', params={'fields': 'displayName'}).json().get('displayName', program_id)
    except RequestException:
        return program_id


def get_org_unit_names(api, ou_ids):
    """Batch-fetches display names for a collection of org unit IDs. Returns {id: displayName}."""
    if not ou_ids:
        return {}
    try:
        print(f"Starting response to get org units...")
        resp = api.get('organisationUnits', params={
            'filter': f'id:in:[{",".join(ou_ids)}]',
            'fields': 'id,displayName',
            'paging': 'false',
        }).json()
        print(f"Success getting response for org units...")
        return {ou['id']: ou['displayName'] for ou in resp.get('organisationUnits', [])}
    except RequestException as e:
        print(f"Error getting response for org units...{e}")
        return {}


def get_program_attributes(api, program_id):
    """Returns:
      - attrs: ordered list of (attr_id, display_name)
      - option_lookups: dict of attr_id → {code: display_name} for option-set attributes
    """
    try:
        resp = api.get(
            f'programs/{program_id}',
            params={
                'fields': 'programTrackedEntityAttributes['
                          'trackedEntityAttribute['
                          'id,displayName,'
                          'optionSet[options[code,displayName]]'
                          ']]'
            }
        ).json()
        pteas = resp.get('programTrackedEntityAttributes', [])
        attrs = []
        option_lookups = {}
        for p in pteas:
            tea = p['trackedEntityAttribute']
            aid, name = tea['id'], tea['displayName']
            attrs.append((aid, name))
            option_set = tea.get('optionSet')
            if option_set:
                option_lookups[aid] = {
                    opt['code']: opt['displayName']
                    for opt in option_set.get('options', [])
                }
        return attrs, option_lookups
    except RequestException:
        return [], {}


# ----------------------------
# 3. Export
# ----------------------------

def run_export(period="today", date=None):
    now = datetime.now()

    if period == "today":
        ref = now.date()
    elif period == "yesterday":
        ref = (now - timedelta(days=1)).date()
    elif period == "this_week":
        ref = (now - timedelta(days=now.weekday())).date()
    elif period == "custom":
        ref = datetime.strptime(date, '%Y-%m-%d').date()
    else:
        ref = None

    if ref:
        start_date = ref.replace(day=1).strftime('%Y-%m-%d')
        end_date = ref.replace(day=calendar.monthrange(ref.year, ref.month)[1]).strftime('%Y-%m-%d')
        month_label = ref.strftime('%Y-%m')
    else:
        start_date, end_date, month_label = "1900-01-01", None, "all_time"

    print(f"\n--- ZEBRA EXPORT (Period: {period}, Month: {month_label}, Run: {now.strftime('%d %B %Y %H:%M')}) ---")

    zebra_api = Api.from_auth_file(ZEBRA_AUTH)
    if not check_auth(zebra_api, "Zebra"):
        sys.exit(1)

    print(f"Fetching enrollments for program {ZEBRA_PROG} for month {month_label}...")
    enr_params = {'program': ZEBRA_PROG, 'ouMode': 'ALL', 'enrolledAfter': start_date}
    if end_date:
        enr_params['enrolledBefore'] = end_date
    enrollments = get_all_enrollments(zebra_api, enr_params)
    print(f"Retrieved {len(enrollments)} enrollments.")

    if not enrollments:
        print("No records found for the given period. No CSV written.")
        return

    print(f"Fetching all program attributes for {ZEBRA_PROG}...")
    program_attrs, option_lookups = get_program_attributes(zebra_api, ZEBRA_PROG)
    print(f"Program defines {len(program_attrs)} attributes ({len(option_lookups)} with option sets).")

    program_name = get_program_name(zebra_api, ZEBRA_PROG)
    print(f"Program name: {program_name}")

    ou_ids = {enr['orgUnit'] for enr in enrollments if enr.get('orgUnit')}
    print(f"Resolving {len(ou_ids)} org unit names...")
    ou_names = get_org_unit_names(zebra_api, ou_ids)

    # attr_id → column display name
    attr_names = {aid: name for aid, name in program_attrs}

    base_cols = ['trackedEntity', 'enrollment', 'program', 'orgUnit', 'status', 'enrolledAt', 'occurredAt']
    attr_cols = [name for _, name in program_attrs]
    fieldnames = base_cols + attr_cols

    os.makedirs("exports", exist_ok=True)
    seen_teis = set()
    output_file = os.path.join("exports", f"zebra_events_{month_label}.csv")
    written = 0
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for enr in enrollments:
            tei_id = enr.get('trackedEntity', '')
            if not tei_id or tei_id in seen_teis:
                continue
            seen_teis.add(tei_id)

            print(f"  Fetching TEI {tei_id} ({written + 1}/{len(enrollments)})...")
            tei = get_tei(zebra_api, tei_id)

            ou_id = enr.get('orgUnit', '')
            row = {
                'trackedEntity': tei_id,
                'enrollment': enr.get('enrollment', ''),
                'program': program_name,
                'orgUnit': ou_names.get(ou_id, ou_id),
                'status': enr.get('status', ''),
                'enrolledAt': enr.get('enrolledAt', ''),
                'occurredAt': enr.get('occurredAt', ''),
            }
            for attr in tei.get('attributes', []):
                aid = attr['attribute']
                col = attr_names.get(aid, aid)
                raw = attr.get('value', '')
                row[col] = option_lookups.get(aid, {}).get(raw, raw)
            writer.writerow(row)
            written += 1

    print(f"Exported {written} records to '{output_file}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export ZEBRA program events to CSV.")
    parser.add_argument(
        "-p", "--period",
        choices=["today", "yesterday", "this_week", "all_time", "custom"],
        default="today",
        help="Time period to export (default: today)"
    )
    parser.add_argument("-d", "--date", help="Start date for custom period (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.period == "custom" and not args.date:
        parser.error("--date is required when --period is custom")

    run_export(period=args.period, date=args.date)
