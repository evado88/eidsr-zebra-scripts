from __future__ import annotations
import os
import sys
import argparse
from dhis2 import Api, RequestException

CONFIG_DIR = "./config"
ZEBRA_AUTH = os.path.join(CONFIG_DIR, "zebra_auth.json")
VERIFICATION_STATUS_LABEL = "Verification Status"
VERIFIED_DISPLAY_VALUE = "Verified"


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


def get_enrollment(api, enrollment_id):
    try:
        return api.get(f"tracker/enrollments/{enrollment_id}", params={"fields": "*"}).json()
    except RequestException as e:
        if e.code == 404:
            print(f"ERROR: Enrollment '{enrollment_id}' not found.")
        else:
            print(f"ERROR: Could not fetch enrollment '{enrollment_id}' (code {e.code}).")
        return None


def get_tei(api, tei_id):
    try:
        return api.get(f"tracker/trackedEntities/{tei_id}", params={"fields": "*"}).json()
    except RequestException as e:
        print(f"ERROR: Could not fetch TEI {tei_id} (code {e.code}).")
        return None


def get_verification_attr(api, program_id):
    """Return (attr_id, option_lookup) for the Verification Status attribute."""
    try:
        resp = api.get(
            f"programs/{program_id}",
            params={
                "fields": "programTrackedEntityAttributes["
                          "trackedEntityAttribute["
                          "id,displayName,optionSet[options[code,displayName]]"
                          "]]"
            },
        ).json()
        for ptea in resp.get("programTrackedEntityAttributes", []):
            tea = ptea["trackedEntityAttribute"]
            if tea["displayName"] == VERIFICATION_STATUS_LABEL:
                option_lookup = {}
                if tea.get("optionSet"):
                    option_lookup = {
                        opt["code"]: opt["displayName"]
                        for opt in tea["optionSet"].get("options", [])
                    }
                return tea["id"], option_lookup
    except RequestException as e:
        print(f"WARNING: Could not fetch program attributes (code {e.code}).")
    return None, {}


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
            print(f"Enrollment '{enrollment_id}' successfully cancelled.")
        else:
            errors = resp.get("validationReport", {}).get("errorReports", [])
            for err in errors:
                print(f"  Validation error: {err.get('message', err)}")
            print(f"Cancel request returned status: {resp.get('status', 'UNKNOWN')}")
            sys.exit(1)
    except RequestException as e:
        print(f"ERROR: Failed to cancel enrollment (code {e.code}): {e}")
        sys.exit(1)


def run(enrollment_id):
    api = Api.from_auth_file(ZEBRA_AUTH)
    if not check_auth(api, "Zebra"):
        sys.exit(1)

    print(f"Fetching enrollment '{enrollment_id}'...")
    enrollment = get_enrollment(api, enrollment_id)
    if not enrollment:
        sys.exit(1)

    current_status = enrollment.get("status", "")
    if current_status == "CANCELLED":
        print(f"Enrollment '{enrollment_id}' is already CANCELLED. Nothing to do.")
        sys.exit(0)

    program_id = enrollment.get("program")
    tei_id = enrollment.get("trackedEntity")

    print(f"Fetching TEI '{tei_id}' to read attributes...")
    tei = get_tei(api, tei_id)
    if not tei:
        sys.exit(1)

    print(f"Looking up '{VERIFICATION_STATUS_LABEL}' attribute for program '{program_id}'...")
    attr_id, option_lookup = get_verification_attr(api, program_id)
    if not attr_id:
        print(
            f"ERROR: '{VERIFICATION_STATUS_LABEL}' attribute not found in program '{program_id}'. "
            "Aborting for safety."
        )
        sys.exit(1)

    raw_value = next(
        (a.get("value") for a in tei.get("attributes", []) if a["attribute"] == attr_id),
        None,
    )
    display_value = option_lookup.get(raw_value, raw_value) if raw_value is not None else None

    print(f"'{VERIFICATION_STATUS_LABEL}' = '{display_value}' (raw code: '{raw_value}')")

    if display_value == VERIFIED_DISPLAY_VALUE:
        print(
            f"\nWARNING: Enrollment '{enrollment_id}' has Verification Status = '{VERIFIED_DISPLAY_VALUE}'."
        )
        print("This enrollment is already verified and will NOT be cancelled.")
        sys.exit(0)

    print(
        f"\nVerification Status is '{display_value}' (not '{VERIFIED_DISPLAY_VALUE}'). "
        "Proceeding to cancel..."
    )
    cancel_enrollment(api, enrollment)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cancel a DHIS2 ZEBRA enrollment if Verification Status is not Verified."
    )
    parser.add_argument("enrollment_id", help="DHIS2 enrollment UID to cancel")
    args = parser.parse_args()
    run(args.enrollment_id)
