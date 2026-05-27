from __future__ import annotations
import os
import copy
import sqlite3
from datetime import datetime as dt
import time
import pandas as pd
from sqlalchemy import create_engine
from dhis2 import Api

# ----------------------------
# Local paths & constants
# ----------------------------
# Get the directory of the current script
script_dir = os.path.dirname(__file__)

DATA_DIR = "dags/data"
CONFIG_DIR = "dags/configs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

DB_FILE = os.path.join(script_dir, DATA_DIR, "middleware.db")
eConfig = os.path.join(script_dir, CONFIG_DIR, "eIDSR_auth.json")
zConfig = os.path.join(script_dir, CONFIG_DIR, "zebra_auth.json")


# ----------------------------
# Helper functions (no Airflow)
# ----------------------------

#---------------- NEW
def _normalize_ts(ts: str) -> str:
    """Normalize Zebra occurredAt / our report_date to 'YYYY-MM-DDTHH:MM:SS' (drop TZ)."""
    if pd.isna(ts):
        return None
    # pandas handles Z/offsets; we drop tz and keep seconds precision
    return pd.to_datetime(ts, utc=True, errors='coerce').tz_localize(None).strftime('%Y-%m-%dT%H:%M:%S')


def _dv_value(data_values: list, de_uid: str):
    """Get value of a dataElement from Zebra event dataValues."""
    if not isinstance(data_values, list):
        return None
    for dv in data_values:
        if dv.get('dataElement') == de_uid:
            return dv.get('value')
    return None
#-----------------------------------
def get_case_type(eIDSR_dataElement, condition_mapping):
    """
    Map an eIDSR data element to case status.
    Index: 0 Confirmed, 1 Death, 2 Sent to Lab, 3 Suspected
    """
    case_type = {0: 'Confirmed', 1: 'Death', 2: 'Sent to Lab', 3: 'Suspected'}
    if not isinstance(eIDSR_dataElement, str):
        return None
    for values in condition_mapping.values():
        if isinstance(values, list) and eIDSR_dataElement in values:
            try:
                idx = values.index(eIDSR_dataElement)
                return case_type.get(idx, "Unknown Case Type")
            except ValueError:
                return "Unknown Case Type"
    return None


def extract_attributes(data, filter_keys, filter_values, key_field, value_field):
    """Extract attributes (key_field -> value_field) for items whose filter_keys is in filter_values."""
    if not isinstance(data, list):
        return {}
    return {
        item.get(key_field): item.get(value_field)
        for item in data
        if item.get(filter_keys) in filter_values
    }


def extract_final_case_details(events):
    """Pull final case details from events of stage xwgco84MvE6."""
    data_elements = {'lRRrXjJmQXI', 'sK7ZVtCaP5m', 'ZMQRM044SJB'}
    final_case_details = {}
    for event in events:
        if event.get('programStage') == 'xwgco84MvE6':
            for item in event.get('dataValues', []):
                data_el = item.get('dataElement')
                value = item.get('value')
                if data_el in data_elements:
                    final_case_details[data_el] = value
    return final_case_details


def extract_events_with_stage(enrollments):
    """Return events with programStage == xwgco84MvE6 from enrollments list."""
    if not isinstance(enrollments, list):
        return []
    return [
        event for enrollment in enrollments
        for event in enrollment.get('events', [])
        if event.get('programStage') == 'xwgco84MvE6'
    ]


def update_report_id(row, report_log):
    """
    If a row (Event_id, report_date, orgUnit, data_source) exists in report_log,
    return matching report_id, else None.
    """
    log_values = report_log[['Event_id', 'report_date', 'orgUnit', 'data_source']]
    matched = (log_values.values == row.values).all(axis=1)
    if matched.any():
        return report_log.loc[matched, 'report_id'].values[0]
    return None


def get_updated_date(events, sync_log):
    """Return max(last_successful_sync, derived_date_of_emergence)."""
    try:
        last_event_sync_date = sync_log[
            (sync_log.get('Event ID') == str(events.get('Event ID'))) &
            (sync_log.get('orgUnit') == str(events.get('orgUnit')))
        ]['successful_sync_time'].max()

        if pd.to_datetime(last_event_sync_date) > pd.to_datetime(events.get('derived_date_of_emergence')):
            return last_event_sync_date
        else:
            return events.get('derived_date_of_emergence')
    except Exception as e:
        print(f"Error in updating last sync date: {e}")


def initialize_database():
    """
    Create local SQLite DB + tables if not present:
    - report_log(id, report_id, Event_id, report_date, orgUnit, data_source)
    - sync_log(id, [Event ID], orgUnit, successful_sync_time)
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS report_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT NOT NULL,
                Event_id TEXT NOT NULL,
                report_date TEXT NOT NULL,
                orgUnit TEXT NOT NULL,
                data_source TEXT NOT NULL
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                [Event ID] TEXT NOT NULL,
                orgUnit TEXT NOT NULL,
                successful_sync_time datetime
            );
        ''')
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error:", e)
        raise
    finally:
        conn.close()


def get_active_events_parameters(zebra_api, sync_log):
    """
    Get active Zebra national events + related alerts and derive:
    Event ID, orgUnit, Disease, derived_date_of_emergence, last_sync_date
    """
    try:
        # Step 1: active national events
        zebra_event_params = {
            'program': 'qkOTdxkte8V',
            'orgUnit': 'PS5JpkoHHio',
            'programStatus': 'ACTIVE',
            'fields': ['trackedEntity', 'attributes[attribute,value,displayName]'], 
            'skipPaging': 'true'
        }
        active_events_response = zebra_api.get('tracker/trackedEntities', params=zebra_event_params)
        active_events_data = active_events_response.json().get('instances', [])
        if not active_events_data:
            print("No active Zebra national events found.")
            return pd.DataFrame(columns=['Event ID', 'orgUnit', 'Disease', 'derived_date_of_emergence'])

        data = pd.DataFrame(active_events_data)

        # Extract suspected disease
        filter_keys = 'attribute'
        filter_values = ['jLvbkuvPdZ6']  # Suspected Disease
        attr_1 = 'displayName'
        attr_2 = 'value'

        national_active_events = pd.concat([
            data['trackedEntity'],
            pd.json_normalize(
                data['attributes'].apply(
                    lambda x: extract_attributes(x, filter_keys, filter_values, attr_1, attr_2)
                )
            )
        ], axis=1)

        if national_active_events.empty or 'trackedEntity' not in national_active_events or 'Suspected Disease' not in national_active_events:
            print("No valid national events or suspected disease info found.")
            return pd.DataFrame(columns=['Event ID', 'orgUnit', 'Disease', 'derived_date_of_emergence'])

        national_event_ids = ';'.join(national_active_events['trackedEntity'].astype(str))
        national_active_diseases = ';'.join(national_active_events['Suspected Disease'].astype(str))

        # Step 2: alerts related to national events
        zebra_alerts_params = {
            'program': 'MQtbs8UkBxy',
            'orgUnit': 'PS5JpkoHHio',
            'ouMode': 'DESCENDANTS',
            'filter': [
                'KeUbzfFQYCX:EQ:PHEOC_STATUS_RESPOND',
                f'Pq1drzz2HJk:IN:{national_event_ids}',
                f'agsVaIpit4S:IN:{national_active_diseases}'
            ],
            'fields': ['trackedEntity', 'orgUnit', 'createdAt', 'attributes[attribute,value,displayName]'],
            'skipPaging': 'true'
        }

        alerts_response = zebra_api.get('tracker/trackedEntities', params=zebra_alerts_params)
        alerts_data_raw = alerts_response.json().get('instances', [])
        if not alerts_data_raw:
            print("No Zebra alerts found for national events.")
            return pd.DataFrame(columns=['Event ID', 'orgUnit', 'Disease', 'derived_date_of_emergence'])

        alerts_df = pd.DataFrame(alerts_data_raw)

        filter_values_alerts = [
            'PlAQX9j3tBD', 'KeUbzfFQYCX', 'Pq1drzz2HJk', 'agsVaIpit4S',
            'ecYRs96jEwd', 'h9nPYaleCxE'
        ]  # Dates / status / NE id / Disease etc.

        alerts_attributes = pd.json_normalize(
            alerts_df['attributes'].apply(
                lambda x: extract_attributes(x, filter_keys, filter_values_alerts, attr_1, attr_2)
            )
        )

        combined_alerts = pd.concat(
            [alerts_df[['trackedEntity', 'orgUnit', 'createdAt']], alerts_attributes],
            axis=1
        )

        combined_alerts['derived_date_of_emergence'] = (
            combined_alerts.get('Date Emerged ')
            .combine_first(combined_alerts.get('Date Detected'))
            .combine_first(combined_alerts.get('Date Notified'))
            .combine_first(combined_alerts['createdAt'])
        )

        sync_log = pd.DataFrame(sync_log)

        # Create an explicit copy to avoid chained assignment warnings
        active_event = combined_alerts[['Event ID', 'orgUnit', 'Confirmed Disease', 'derived_date_of_emergence']].copy()

        # Rename safely without inplace=True
        active_event = active_event.rename(columns={'Confirmed Disease': 'Disease'})

        # Assign new column using .loc for clarity
        active_event.loc[:, 'last_sync_date'] = active_event.apply(lambda x: get_updated_date(x, sync_log), axis=1)

        return active_event
    except Exception as e:
        print(f"Error in get_active_events_parameters: {e}")
        return pd.DataFrame(columns=['Event ID', 'orgUnit', 'Disease', 'derived_date_of_emergence', 'last_sync_date'])


def get_nd1_data(event_parameters, report_log, eIDSR_api):
    """
    Retrieve and aggregate ND1 case-level data for events.
    """
    nd1_data = pd.DataFrame()
    zebra_eidsr_nd1_disease_mapping = {
        'RTSL_ZEB_OS_DISEASE_COVID19': 'DU071_COVID19',
        'RTSL_ZEB_OS_DISEASE_ANTHRAX': 'DA034_ANTHRAX',
        'RTSL_ZEB_OS_DISEASE_AFP': 'DA850_AFP',
        'RTSL_ZEB_OS_DISEASE_CHOLERA': 'DA000_CHOLERA',
        'RTSL_ZEB_OS_DISEASE_NEO_TETANUS': 'DA330_NNT',
        'RTSL_ZEB_OS_DISEASE_MONKEYPOX': 'DB040_MONKEYPOX',
        'RTSL_ZEB_OS_DISEASE_MEASLES': 'DB050_MEASLES',
        'RTSL_ZEB_OS_DISEASE_ZIKA_FEVER': 'DA928_ZIKA',
        'RTSL_ZEB_OS_DISEASE_BACTERIAL_MENINGITIS': 'DG009_BACTMENING',
        'RTSL_ZEB_OS_DISEASE_PLAGUE': 'DA200_PLAGUE',
        'RTSL_ZEB_OS_DISEASE_TYPHOID_FEVER': 'DA010_TYPHOID',
        'RTSL_ZEB_OS_DISEASE_ACUTE_VHF': 'DA910_VHF'
    }

    for _, event in event_parameters.iterrows():
        event_id = str(event.get('Event ID'))
        startDate = pd.to_datetime(event.get('derived_date_of_emergence')).strftime('%Y-%m-%dT%H:%M:%S')
        last_event_sync_date = pd.to_datetime(event.get('last_sync_date')).strftime('%Y-%m-%dT%H:%M:%S') if pd.notnull(event.get('last_sync_date')) else None
        zebra_disease = event.get('Disease')
        disease = zebra_eidsr_nd1_disease_mapping.get(zebra_disease)
        org_unit = str(event.get('orgUnit'))

        if not all([event_id, disease, org_unit]):
            continue

        try:
            nd1_query_params = {
                'program': 'gr3uWZVzPQT',
                'orgUnit': org_unit,
                'ouMode': 'CHILDREN',
                'fields': ['attributes', 'enrollments[trackedEntity,program,status,orgUnit,enrolledAt,events[event,status,program,programStage,orgUnit,occurredAt,dataValues[dataElement,value]]]'],
                # 'updatedAfter': last_event_sync_date,  # consider re-enabling if needed
                'filter': f'iSIhKjnlMkv:EQ:{disease}',
                'skipPaging': 'true'
            }
            nd1_response = eIDSR_api.get('tracker/trackedEntities', params=nd1_query_params)
            nd1 = pd.DataFrame(nd1_response.json().get('instances', []))
        except Exception as e:
            print(f"Error retrieving ND1 data: {e}")
            continue

        if nd1.empty:
            continue

        filter_keys = 'attribute'
        filter_values = ['RcCp8T4IWfS', 'iSIhKjnlMkv', 'RhyrqD3Ivd8', 'tx7MHfxB3sv', 'j6R3H6iIHsr']
        attribute_to_extract_1 = 'displayName'
        attribute_to_extract_2 = 'value'

        nd1 = pd.concat(
            [nd1, pd.json_normalize(nd1['attributes'].apply(
                lambda x: extract_attributes(x, filter_keys, filter_values, attribute_to_extract_1, attribute_to_extract_2)
            ))],
            axis=1
        )
        nd1['events'] = nd1['enrollments'].apply(extract_events_with_stage)

        final_case_details = pd.json_normalize(nd1['events'].apply(extract_final_case_details))
        if final_case_details.empty:
            nd1[['lRRrXjJmQXI', 'sK7ZVtCaP5m', 'ZMQRM044SJB']] = pd.DataFrame([[None, None, None]] * len(nd1))
        else:
            nd1 = pd.concat([nd1, final_case_details], axis=1)

        nd1[['orgUnit', 'Event_id', 'Zebra_condition']] = [event['orgUnit'], event_id, event['Disease']]

        nd1['report_date'] = nd1.get('Date of Onset/Event').combine_first(
            nd1.get('Date seen at Health Facility/Community')
        ).combine_first(nd1.get('Date HF/Community notified District'))

        nd1 = nd1[nd1['report_date'] > startDate]
        nd1_data = pd.concat([nd1_data, nd1], axis=0)

    if nd1_data.empty:
        return pd.DataFrame()

    nd1_data.rename(columns={'ZMQRM044SJB': 'confirmed_Disease', 'sK7ZVtCaP5m': 'Death/Alive', 'lRRrXjJmQXI': 'case_Classification'}, inplace=True)
    nd1_data = nd1_data[['orgUnit', 'Event_id', 'Zebra_condition', 'NMC Case ID', 'report_date',
                         'Disease/Condition/Event Notified', 'confirmed_Disease', 'case_Classification', 'Death/Alive']]
    nd1_data.loc[
        nd1_data['confirmed_Disease'].notnull() & (nd1_data['confirmed_Disease'] != nd1_data['Disease/Condition/Event Notified']),
        'case_Classification'
    ] = 'Confirmed a different disease'
    nd1_data.drop_duplicates(inplace=True)

    nd1_agg_data = nd1_data.groupby(['Event_id', 'orgUnit', 'Zebra_condition', 'report_date']).agg(
        Suspected=('Disease/Condition/Event Notified', 'count'),
        Confirmed=('case_Classification', lambda x: x.isin(['FIN_CASE_CLASS_CONFIRM_LAB', 'FIN_CASE_CLASS_CONFIRM_CLIN']).sum()),
        Probable=('case_Classification', lambda x: (x == 'FIN_CASE_CLASS_PROBABLE').sum()),
        Death=('Death/Alive', lambda x: (x == 'VITAL_STATUS_DEAD').sum())
    ).reset_index()

    nd1_agg_data['data_source'] = 'ND1'
    nd1_agg_data['report_id'] = nd1_agg_data[['Event_id', 'report_date', 'orgUnit', 'data_source']].apply(
        lambda x: update_report_id(x, report_log),
        axis=1
    )
    return nd1_agg_data


def get_nd2_data(events, report_log, condition_mapping, eIDSR_api, today):
    """
    Retrieve and transform ND2 analytics data per event & disease.
    """
    nd2_data = pd.DataFrame()
    event_parameters = pd.DataFrame(events)
    if event_parameters.empty:
        return nd2_data

    for _, event in event_parameters.iterrows():
        data = {}
        event_id = str(event.get('Event ID'))
        startDate = pd.to_datetime(event.get('derived_date_of_emergence'))
        period_to_retrieve = ';'.join([
            f"{x.start_time.isocalendar()[0]}W{x.start_time.isocalendar()[1]}"
            for x in pd.period_range(start=startDate, end=today, freq='W')
        ])
        disease = event.get('Disease')
        org_unit = event.get('orgUnit')

        if not all([event_id, disease, org_unit]):
            continue

        try:
            dataElement = ';'.join(condition_mapping.get(disease, []))
            if dataElement:
                dx_dimension = f"dx:{dataElement}"
                ou_dimension = f'ou:{org_unit}'
                pe_dimension = f'pe:{period_to_retrieve}'

                eidsr_analytics_params = {
                    'dimension': f'{dx_dimension},{ou_dimension},{pe_dimension}',
                    'displayProperty': 'NAME',
                    'includeNumDen': 'true',
                    'skipMeta': 'true',
                    'row': ['ou', 'pe'],
                    'column': 'dx'
                }
                response = eIDSR_api.get('analytics', params=eidsr_analytics_params)
                data = response.json()
        except Exception as e:
            print(f"Error retrieving ND2 data: {e}")
            continue

        headers = [header['name'] for header in data.get('headers', [])]
        headers.extend(['Event_id', 'Zebra_condition'])
        rows = data.get('rows', [])
        if len(rows) > 0:
            [row_item.extend([str(event_id), str(event.get('Disease'))]) for row_item in rows]

        df = pd.DataFrame(rows, columns=headers)
        if df.empty:
            continue

        df.rename(columns={'dx': 'dataElement', 'ou': 'orgUnit', 'pe': 'period'}, inplace=True)
        nd2_data = pd.concat([nd2_data, df])

        nd2_data['report_date'] = nd2_data['period'].apply(
            lambda x: pd.to_datetime(
                dt.fromisocalendar(
                    int(x.split('W')[0]),
                    int(x.split('W')[1]),
                    1
                )
            ).strftime('%Y-%m-%d')
        )

    if nd2_data.empty:
        return nd2_data

    nd2_data['Case_Type'] = nd2_data['dataElement'].apply(lambda x: get_case_type(x, condition_mapping))
    nd2_data.drop_duplicates(inplace=True)

    nd2_data = nd2_data.pivot(
        index=['orgUnit', 'Zebra_condition', 'period', 'report_date', 'Event_id'],
        columns=['Case_Type'],
        values='value'
    ).reset_index()

    for col in ['Confirmed', 'Sent to Lab', 'Suspected']:
        if col not in nd2_data.columns:
            nd2_data[col] = 0
    nd2_data[['Confirmed', 'Sent to Lab', 'Suspected']] = nd2_data[['Confirmed', 'Sent to Lab', 'Suspected']].fillna(0).astype(int)

    nd2_data['data_source'] = 'ND2'
    nd2_data['report_id'] = nd2_data[['Event_id', 'report_date', 'orgUnit', 'data_source']].apply(
        lambda x: update_report_id(x, report_log),
        axis=1
    )

    if 'Probable' not in nd2_data.columns:
        nd2_data['Probable'] = pd.Series([pd.NA] * len(nd2_data))
    if 'Death' not in nd2_data.columns:
        nd2_data['Death'] = pd.Series([pd.NA] * len(nd2_data))
    nd2_data['Death'] = pd.to_numeric(nd2_data['Death'], errors='coerce')

    return nd2_data


def get_rows_to_update_on_zebra(reports_data):
    """Return indices where report_id is NaN -> needs Zebra ID."""
    all_reports_data = pd.DataFrame(reports_data)
    if all_reports_data.empty:
        return []
    rows_to_updt = all_reports_data['report_id'].isna()
    return all_reports_data.index[rows_to_updt].tolist()


def update_reports_with_ids(reports_data, zebra_api):
    """
    For rows without report_id, request Zebra IDs and fill them in.
    """
    all_reports_data = pd.DataFrame(reports_data)
    if all_reports_data.empty:
        return all_reports_data

    total_ids_to_generate = all_reports_data['report_id'].isna().sum()
    if total_ids_to_generate > 0:
        id_list_response = zebra_api.get('system/id', params={'limit': total_ids_to_generate})
        id_list = id_list_response.json().get('codes', [])
        if not id_list or len(id_list) < total_ids_to_generate:
            raise ValueError("Not enough Ids returned from Zebra to match required report Ids")
        all_reports_data.loc[all_reports_data['report_id'].isna(), 'report_id'] = \
            id_list[:all_reports_data['report_id'].isna().sum()]
    return all_reports_data


def build_report_log(reports_data, indices_to_updt):
    """Subset reports_data to rows at indices_to_updt for report_log inserts."""
    all_reports_data = pd.DataFrame(reports_data)
    return all_reports_data.loc[indices_to_updt]


def build_zebra_event_payload(reports_data, signed_in_user, today):
    """
    Build Zebra tracker event payload from reports_data rows.
    """
    all_reports_data = pd.DataFrame(reports_data)
    events = []
    if all_reports_data.empty:
        return {'events': events}

    all_reports_data = all_reports_data.fillna(0)
    all_reports_data[['Suspected', 'Confirmed', 'Probable', 'Death']] = \
        all_reports_data[['Suspected', 'Confirmed', 'Probable', 'Death']].astype(int)

    zebra_data_values_template = [
        {'dataElement': 'ugUt0i4h7XI', 'value': ''},  # Disease/condition
        {'dataElement': 'ycN1hVp2M5f', 'value': signed_in_user},  # Signed-in user
        {'dataElement': 'YlgM9XtVgTz', 'value': str(today)},  # Event date
        {'dataElement': 'd4B5pN7ZTEu', 'value': ''},  # Suspected
        {'dataElement': 'bUMlIfyJEYK', 'value': ''},  # Probable
        {'dataElement': 'ApKJDLI5nHP', 'value': ''},  # Confirmed
        {'dataElement': 'Sfl82Bx0ZNz', 'value': ''},  # Deaths
        {'dataElement': 'ylPUzBomYdb', 'value': ''},  # Event ID
        {'dataElement': 'iAIz6uEzES9', 'value': ''},  # Data source
    ]
    data_element_mapping = {
        'ugUt0i4h7XI': 'Zebra_condition',
        'd4B5pN7ZTEu': 'Suspected',
        'ApKJDLI5nHP': 'Confirmed',
        'ylPUzBomYdb': 'Event_id',
        'Sfl82Bx0ZNz': 'Death',
        'bUMlIfyJEYK': 'Probable',
        'iAIz6uEzES9': 'data_source',
    }

    for index, row in all_reports_data.iterrows():
        try:
            org_unit = row.get('orgUnit')
            report_date_raw = row.get('report_date')
            report_date = pd.to_datetime(report_date_raw).strftime('%Y-%m-%dT%H:%M:%S') if pd.notnull(report_date_raw) else None
            if not org_unit or not report_date:
                print(f"Skipping row {index} due to missing orgUnit or report_date")
                continue

            data_values = copy.deepcopy(zebra_data_values_template)
            for value in data_values:
                de = value.get('dataElement')
                col = data_element_mapping.get(de)
                if col and col in row and pd.notnull(row[col]):
                    value['value'] = str(row[col])

            event_payload = {
                'event': row.get('report_id') or '',
                'program': 'A0fHWmkFPzX',
                'programStage': 'aEUOfKt3cNP',
                'orgUnit': org_unit,
                'occurredAt': report_date,
                'status': 'ACTIVE',
                'dataValues': data_values
            }
            events.append(event_payload)
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            continue

    return {'events': events}


def post_data_to_zebra(zebra_api, zebra_case_data, log=True):
    """
    POST payload to Zebra /tracker and print+return import statistics.

    Returns a dict like:
    {
      'status': 'OK'|'DONE'|...,
      'totals': {'created':0,'updated':12,'deleted':0,'ignored':0,'total':12},
      'per_type': {
         'EVENT': {'created':0,'updated':12,'deleted':0,'ignored':0,'total':12,
                   'error_count': 0, 'error_objects': [], 'object_uids': [...]},
         'ENROLLMENT': {...}, ...
      },
      'timings': {...},
      'validation': {...},
      'raw': <full response json>
    }
    """
    response = zebra_api.post(
        'tracker',
        json=zebra_case_data,
        params={
            'async': 'false',
            'importStrategy': 'CREATE_AND_UPDATE',
            'reportMode': 'FULL',
            'atomicMode': 'OBJECT',
            'validationMode': 'SKIP'
        }
    )
    rj = response.json() if hasattr(response, "json") else {}

    status = rj.get('status') or rj.get('httpStatus') or 'UNKNOWN'

    # Top-level totals (present in the response you pasted)
    totals = rj.get('stats') or {}

    per_type = {}
    timings = rj.get('timingsStats', {})
    validation = rj.get('validationReport', {})

    # --- Shape A: bundleReport/typeReportMap (your current server response) ---
    trm = (rj.get('bundleReport') or {}).get('typeReportMap', {})
    if trm:
        for type_name, type_report in trm.items():
            s = (type_report or {}).get('stats') or {}
            obj_reports = (type_report or {}).get('objectReports') or []
            # gather errors and uids
            error_objs = []
            uids = []
            for o in obj_reports:
                uid = o.get('uid')
                if uid:
                    uids.append(uid)
                errs = o.get('errorReports') or []
                if errs:
                    error_objs.append({'uid': uid, 'errors': errs})
            per_type[type_name] = {
                'created': s.get('created', 0),
                'updated': s.get('updated', 0),
                'deleted': s.get('deleted', 0),
                'ignored': s.get('ignored', 0),
                'total':   s.get('total', 0),
                'error_count': sum(len(eo['errors']) for eo in error_objs),
                'error_objects': error_objs,
                'object_uids': uids
            }

    # --- Shape B: results[] (older async pipeline shape or other endpoints) ---
    if not trm and isinstance(rj.get('results'), list):
        for item in rj['results']:
            t = item.get('type', 'unknown')
            s = item.get('stats', {}) or {}
            errs = item.get('errors', []) or []
            per_type[t] = {
                'created': s.get('imported', 0),  # sometimes 'imported' maps to created
                'updated': s.get('updated', 0),
                'deleted': s.get('deleted', 0),
                'ignored': s.get('ignored', 0),
                'total':   s.get('total', 0),
                'error_count': len(errs),
                'error_objects': errs,
                'object_uids': []
            }

    summary = {
        'status': status,
        'totals': totals,
        'per_type': per_type,
        'timings': timings,
        'validation': validation,
        'raw': rj
    }

    if log:
        # One-liners for quick visibility
        if totals:
            print(f"[ZEBRA] Status={status} | totals: "
                  f"created={totals.get('created',0)}, "
                  f"updated={totals.get('updated',0)}, "
                  f"ignored={totals.get('ignored',0)}, "
                  f"deleted={totals.get('deleted',0)}, total={totals.get('total',0)}")
        else:
            print(f"[ZEBRA] Status={status}")

        for t, s in per_type.items():
            if t == 'RELATIONSHIP':
                continue
            print(f"[ZEBRA] {t}: created={s.get('created',0)}, "
                  f"updated={s.get('updated',0)}, ignored={s.get('ignored',0)}, "
                  f"deleted={s.get('deleted',0)}, total={s.get('total',0)}, "
                  f"errors={s.get('error_count',0)}")

        # Optional timings line if present
        total_import = (timings.get('timers') or {}).get('totalImport')
        if total_import:
            print(f"[ZEBRA] Timings totalImport={total_import}")

        # Optional validation warnings/errors
        vr = validation or {}
        erc = len(vr.get('errorReports', []))
        wrc = len(vr.get('warningReports', []))
        if erc or wrc:
            print(f"[ZEBRA] Validation: errors={erc}, warnings={wrc}")

    return summary


def update_zebra_datastore(zebra_api, validation_report, today):
    """
    Store last sync status in Zebra datastore.
    """
    try:
        status = 'success' if not validation_report.get('errorReports') else 'error'
        zebra_api.put('dataStore/zebra/middleware-sync', json={'lastSyncTime': today, 'status': status})
    except Exception as e:
        print(f"Failed to update zebra data store: {e}")
        raise


def extract_sync_log_data(epi_data_query_parameter):
    """
    Return [{'Event ID':..., 'orgUnit':...}, ...] for sync_log updates.
    """
    df = pd.DataFrame(epi_data_query_parameter)
    return df[['Event ID', 'orgUnit']].to_dict(orient='records')


def combine_nd1_and_nd2_data(nd1_data, nd2_data):
    """Concat ND1 + ND2 and return list of records."""
    combined_df = pd.concat([nd1_data, nd2_data], axis=0).reset_index()
    return combined_df.to_dict(orient='records')


def update_sync_log(validation_report, sync_log_data, report_log_data, today, engine):
    """
    If no errors in validation_report, append to sync_log & report_log tables.
    """
    try:
        sync_log_updt = pd.DataFrame(sync_log_data)
        report_log_updt = pd.DataFrame(report_log_data)

        if not sync_log_updt.empty and not validation_report.get('errorReports'):
            sync_log_updt.loc[:, 'successful_sync_time'] = today
            sync_log_updt.to_sql('sync_log', engine, if_exists='append', index=False)

        if not report_log_updt.empty:
            report_log_updt = report_log_updt[['report_id', 'Event_id', 'report_date', 'orgUnit', 'data_source']]
            report_log_updt.to_sql('report_log', engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Failed to update middleware sync log: {e}")
        raise

# ------------ NEW -----
def _query_zebra_stage_events_for_orgunit(zebra_api, org_unit: str) -> list[dict]:
    """
    Fetch ACTIVE events for our program stage in a given orgUnit.
    Returns list of events with fields needed for matching.
    """
    params = {
        'programStage': 'aEUOfKt3cNP',
        'status': 'ACTIVE',
        'orgUnit': org_unit,
        'paging': 'false',
        'fields': 'event,status,orgUnit,orgUnitName,occurredAt,updatedAt,dataValues[dataElement,value]'
    }
    resp = zebra_api.get('tracker/events', params=params)  # same as /api/tracker/events
    js = resp.json()
    # DHIS2 may return {"events":[...]} or a plain list; handle both
    if isinstance(js, dict):
        return js.get('events', js.get('instances', []))
    return js if isinstance(js, list) else []


def _build_existing_event_lookup(zebra_api, df: pd.DataFrame) -> dict:
    """
    Build a dict keyed by (orgUnit, occurredAt_norm, source, alert_id) -> event_id
    by querying Zebra once per orgUnit found in df.
    """
    lookup = {}
    org_units = sorted(set(df['orgUnit'].dropna().astype(str)))
    for ou in org_units:
        try:
            events = _query_zebra_stage_events_for_orgunit(zebra_api, ou)
        except Exception as e:
            print(f"Warning: failed to fetch events for orgUnit {ou}: {e}")
            continue

        for ev in events:
            occurred_norm = _normalize_ts(ev.get('occurredAt'))
            dvs = ev.get('dataValues', [])
            source = _dv_value(dvs, 'iAIz6uEzES9')        # ND1 / ND2
            alert_id = _dv_value(dvs, 'ylPUzBomYdb')      # your Event_id
            if occurred_norm and source and alert_id:
                key = (ev.get('orgUnit'), occurred_norm, source, alert_id)
                lookup[key] = ev.get('event')
    return lookup


def assign_event_ids_by_lookup(reports_data, zebra_api):
    """
    For each row in reports_data, try to re-use an existing Zebra event (match on
    orgUnit + occurredAt + data_source + Event_id). If not found, request new IDs.

    Returns:
      rows_with_ids (DataFrame)
      created_mask (Series[bool])  True where a NEW id was minted (i.e., no prior event)
    """
    df = pd.DataFrame(reports_data).copy()
    if df.empty:
        return df, pd.Series([], dtype=bool)

    # Ensure required columns exist
    for col in ['orgUnit', 'report_date', 'data_source', 'Event_id']:
        if col not in df.columns:
            df[col] = pd.NA

    # Normalize report_date for matching with occurredAt
    df['report_date_norm'] = df['report_date'].apply(_normalize_ts)

    # Build lookup from Zebra
    existing = _build_existing_event_lookup(zebra_api, df)

    # Try to reuse existing event ids
    df['report_id'] = df.get('report_id')  # keep if present, else create
    df['report_id'] = df['report_id'].where(df['report_id'].notna(), None)  # normalize NaNs to None first

    for idx, row in df.iterrows():
        if row.get('report_id'):  # already set; keep it
            continue
        key = (str(row.get('orgUnit')),
               row.get('report_date_norm'),
               str(row.get('data_source')),
               str(row.get('Event_id')))
        ev = existing.get(key)
        if ev:
            df.at[idx, 'report_id'] = ev  # reuse!

    # Identify still-missing -> mint new IDs
    missing_mask = df['report_id'].isna()
    to_create = int(missing_mask.sum())
    if to_create > 0:
        try:
            resp = zebra_api.get('system/id', params={'limit': to_create})
            codes = (resp.json() or {}).get('codes', [])
            if len(codes) < to_create:
                raise ValueError(f"Requested {to_create} ids, got {len(codes)}")
            df.loc[missing_mask, 'report_id'] = codes[:to_create]
            created_mask = pd.Series(False, index=df.index)
            created_mask.loc[missing_mask.index[missing_mask]] = True
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Zebra IDs: {e}")
    else:
        created_mask = pd.Series(False, index=df.index)

    # Clean up
    df.drop(columns=['report_date_norm'], inplace=True)
    return df, created_mask


def build_report_log_from_mask(rows_with_ids: pd.DataFrame, created_mask: pd.Series):
    """Return only rows that received a NEW report_id (created on Zebra)."""
    if rows_with_ids.empty or created_mask.empty:
        return pd.DataFrame()
    cols = ['report_id', 'Event_id', 'report_date', 'orgUnit', 'data_source']
    missing = [c for c in cols if c not in rows_with_ids.columns]
    for c in missing:
        rows_with_ids[c] = pd.NA
    return rows_with_ids.loc[created_mask, cols].copy()

#----------------------------------

# ----------------------------
# Main flow (no Airflow)
# ----------------------------
def main():
    # Connect APIs
    eIDSR_api = Api.from_auth_file(eConfig, user_agent='myApp/1.0')
    zebra_api = Api.from_auth_file(zConfig, user_agent='myApp/1.0')

    try:
        signed_in_user = zebra_api.get('me', params={'fields': ['id', 'username']}).json().get('username', 'unknown_user')
    except Exception as e:
        print(f"Error retrieving signed-in user: {e}")
        signed_in_user = 'unknown_user'

    today = pd.Timestamp.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    # Zebra condition -> eIDSR ND2 data elements
    condition_mapping = {
        'RTSL_ZEB_OS_DISEASE_AFP': ['tH7rBoGPODP', 'fndkeftTxcT', 'Ltc8nhQ16Hd', 'gpBshPHOB9X'],
        'RTSL_ZEB_OS_DISEASE_COVID19': ['ISOfhCZH39T', 'b14JBXwgjby', 'EKWw4vPCtIQ', 'mH1mYNOZGtT'],
        'RTSL_ZEB_OS_DISEASE_ANTHRAX': ['z8Wr32jkmuh', 'OZ92TezIshw', 'HkR2BEwlni3', 'zBFmdwpO2LZ'],
        'RTSL_ZEB_OS_DISEASE_CHOLERA': ['AzkTqGTlreJ', 'Ay3ZqJMukSP', 'gMy3ugmaS8z', 'norzLTZKFeO'],
        'RTSL_ZEB_OS_DISEASE_NEO_TETANUS': ['unIlUxwkP60', 'nJC1o19lYM4', 'wWf1EoPbbw1', 'nOKJtOGVdxP'],
        'RTSL_ZEB_OS_DISEASE_MONKEYPOX': ['Y0Y9XZciWLm', 'Qr82Z2j6xuq', 'RS5cqRXIrAg', 'IN3dD9WFSzC'],
        'RTSL_ZEB_OS_DISEASE_ZIKA_FEVER': ['tUYzSeIr1bB', 'beeexIOlGpb', 'wG5OEP4RzDh', 'XpZUVAmPktQ'],
        'RTSL_ZEB_OS_DISEASE_BACTERIAL_MENINGITIS': ['m944LIuDuVl', 'tCvCvaQh8fS', 'KcABATEZzIT', 'Pxgwl8HHd2S'],
        'RTSL_ZEB_OS_DISEASE_MEASLES': ['tGO71vb8X4C', 'qhTdkAWNjBG', 'o5nSN7w2ILg', 'DJZWhsgbmYU'],
        'RTSL_ZEB_OS_DISEASE_PLAGUE': ['JomkOuESeN0', 'wx9magDEvgE', 'ntfkbOMshyg', 'vs682sigkW8'],
        'RTSL_ZEB_OS_DISEASE_TYPHOID_FEVER': ['dPDY8A7XP4V', 'xmlxu51X5FI', 'lF4NZZjRxhq', 'IDEVv6Epwi9'],
        'RTSL_ZEB_OS_DISEASE_ACUTE_VHF': ['xH18qiCnErl', 'qcHOE3lvdLR', 'EwCXFOsqwrI', 'lI3EMAbSqPD'],
        'RTSL_ZEB_OS_DISEASE_DIARRHOEA_W_BLOOD': ['bgoWzjuDr1w', 'jG1mkERdcq2', 'XA4KmKIddX1', 'AW99TH1knkR'],
        'RTSL_ZEB_OS_DISEASE_SARIS': ['ZO0ULwawUDZ', 'OHW0PXmuv2q', 'dMt4DePB5Hx', 'g1G7GO0uUho']
    }

    # DB setup
    engine = create_engine(f"sqlite:///{DB_FILE}")
    initialize_database()

    try:
        report_log = pd.read_sql('SELECT report_id, Event_id, report_date, orgUnit, data_source FROM report_log;', engine)
        sync_log = pd.read_sql('SELECT [Event ID], orgUnit, successful_sync_time FROM sync_log;', engine)
    except Exception as e:
        print(f"A database error occurred: {e}")
        raise

    try:
        # 1) Active events and sync base
        epi_data_query_parameter = get_active_events_parameters(zebra_api, sync_log)
        sync_log_updt = extract_sync_log_data(epi_data_query_parameter)

        # 2) ND1 + ND2
        nd1_data = get_nd1_data(epi_data_query_parameter, report_log, eIDSR_api)
        nd2_data = get_nd2_data(epi_data_query_parameter, report_log, condition_mapping, eIDSR_api, pd.Timestamp.utcnow())

        # 3) Combine
        all_reports_data = combine_nd1_and_nd2_data(nd1_data, nd2_data)

        # 4–5) Reuse existing events or mint new IDs
        rows_with_ids, created_mask = assign_event_ids_by_lookup(all_reports_data, zebra_api)

        # 6) (optional) Only log *newly created* events to report_log
        report_log_updt = build_report_log_from_mask(rows_with_ids, created_mask)

        # 7) Build payload as before
        zebra_case_data = build_zebra_event_payload(rows_with_ids, signed_in_user, today)

        # 8) POST to Zebra + update datastore & logs
        validation_report = post_data_to_zebra(zebra_api, zebra_case_data)
        update_zebra_datastore(zebra_api, validation_report, today)
        update_sync_log(validation_report, sync_log_updt, report_log_updt, today, engine)
        print(f"End Executing Middleware task at: {dt.now()}")
    except Exception as e:
        print(f"An error occurred during Zebra sync: {e}")
        raise
    finally:
        try:
            engine.dispose()
            time.sleep(1800)  # Sleep for 300 seconds (5 minutes)
        except Exception:
            time.sleep(1800)  # Sleep for 300 seconds (5 minutes)


if __name__ == "__main__":
    while True:
        print(f"Start Executing Middleware task at: {dt.now()}")
        main()
