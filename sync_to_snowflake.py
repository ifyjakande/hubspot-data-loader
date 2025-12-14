#!/usr/bin/env python3
"""
HubSpot to Snowflake Sync - Incremental data synchronization with validation
"""

import os
import sys
import requests
import snowflake.connector
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# Configuration from environment variables
HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'HUBSPOT_DATA')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')

# Reconciliation optimization settings
# Run full reconciliation every N syncs (set to 1 to run every time)
RECONCILIATION_FREQUENCY = int(os.environ.get('RECONCILIATION_FREQUENCY', '10'))
# Skip reconciliation if counts match (smart optimization)
SKIP_RECONCILIATION_IF_COUNTS_MATCH = os.environ.get('SKIP_RECONCILIATION_IF_COUNTS_MATCH', 'true').lower() == 'true'

# Validate required environment variables
required_vars = {
    'HUBSPOT_API_KEY': HUBSPOT_API_KEY,
    'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
    'SNOWFLAKE_USER': SNOWFLAKE_USER,
    'SNOWFLAKE_PASSWORD': SNOWFLAKE_PASSWORD,
    'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

HUBSPOT_API_URL = 'https://api.hubapi.com'

# Rate limiting configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2  # seconds
MAX_RETRY_DELAY = 60  # seconds

def make_hubspot_request(
    url: str,
    headers: dict,
    params: dict,
    method: str = 'GET',
    timeout_seconds: int = 30,
) -> dict:
    """Make HubSpot API request with exponential backoff retry logic"""
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=params, timeout=timeout_seconds)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Handle rate limiting (429) and server errors (5xx)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                print(f"⚠️  Rate limited. Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            
            if response.status_code >= 500:
                print(f"⚠️  Server error {response.status_code}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            
            # Handle 400 errors (bad request) - don't retry, log details and fail
            if response.status_code == 400:
                error_detail = "No error details"
                try:
                    error_detail = response.json()
                except:
                    error_detail = response.text
                print(f"❌ Bad Request (400) - HubSpot API Error:")
                print(f"   URL: {url}")
                print(f"   Error details: {error_detail}")
                raise requests.exceptions.HTTPError(f"400 Bad Request: {error_detail}", response=response)

            # Raise for other HTTP errors
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                # Log response details if available for debugging
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_body = e.response.json()
                        print(f"❌ Final error response: {error_body}")
                    except:
                        print(f"❌ Final error response: {e.response.text if hasattr(e.response, 'text') else 'No response body'}")
                raise
            print(f"⚠️  Request failed: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
    
    raise Exception(f"Failed to complete request after {MAX_RETRIES} attempts")

def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def initialize_snowflake_schema(conn):
    """Create database, schema, and tables if they don't exist"""
    cursor = conn.cursor()
    
    try:
        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        
        # Create CONTACTS table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CONTACTS (
                HUBSPOT_ID VARCHAR(50) PRIMARY KEY,
                EMAIL VARCHAR(255),
                FIRSTNAME VARCHAR(255),
                LASTNAME VARCHAR(255),
                PHONE VARCHAR(50),
                JOBTITLE VARCHAR(255),
                COMPANY VARCHAR(255),
                HS_CREATEDATE TIMESTAMP_NTZ,
                HS_LASTMODIFIEDDATE TIMESTAMP_NTZ,
                SYNCED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Create COMPANIES table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS COMPANIES (
                HUBSPOT_ID VARCHAR(50) PRIMARY KEY,
                NAME VARCHAR(255),
                DOMAIN VARCHAR(255),
                INDUSTRY VARCHAR(255),
                CITY VARCHAR(255),
                COUNTRY VARCHAR(255),
                HS_CREATEDATE TIMESTAMP_NTZ,
                HS_LASTMODIFIEDDATE TIMESTAMP_NTZ,
                SYNCED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Create SYNC_METADATA table with reconciliation tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SYNC_METADATA (
                OBJECT_TYPE VARCHAR(50) PRIMARY KEY,
                LAST_SYNC_TIMESTAMP TIMESTAMP_NTZ,
                RECORDS_SYNCED INTEGER,
                HUBSPOT_TOTAL_COUNT INTEGER,
                SNOWFLAKE_TOTAL_COUNT INTEGER,
                COUNTS_MATCH BOOLEAN,
                RECONCILIATION_RUN_COUNT INTEGER DEFAULT 0,
                LAST_RECONCILIATION_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        # Add reconciliation columns if they don't exist (for existing tables)
        try:
            cursor.execute("""
                ALTER TABLE SYNC_METADATA
                ADD COLUMN IF NOT EXISTS RECONCILIATION_RUN_COUNT INTEGER DEFAULT 0
            """)
            cursor.execute("""
                ALTER TABLE SYNC_METADATA
                ADD COLUMN IF NOT EXISTS LAST_RECONCILIATION_AT TIMESTAMP_NTZ
            """)
        except Exception as e:
            # Columns might already exist, ignore error
            pass

        # Ensure soft delete columns exist (used throughout the sync)
        try:
            cursor.execute("""
                ALTER TABLE CONTACTS
                ADD COLUMN IF NOT EXISTS IS_DELETED BOOLEAN DEFAULT FALSE
            """)
            cursor.execute("""
                ALTER TABLE CONTACTS
                ADD COLUMN IF NOT EXISTS DELETED_AT TIMESTAMP_NTZ
            """)
            cursor.execute("""
                ALTER TABLE COMPANIES
                ADD COLUMN IF NOT EXISTS IS_DELETED BOOLEAN DEFAULT FALSE
            """)
            cursor.execute("""
                ALTER TABLE COMPANIES
                ADD COLUMN IF NOT EXISTS DELETED_AT TIMESTAMP_NTZ
            """)
        except Exception:
            pass
        
        print("✓ Snowflake schema initialized successfully")
        
    finally:
        cursor.close()

def get_last_sync_timestamp(conn, object_type: str) -> Optional[str]:
    """Get the last sync timestamp for an object type"""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT LAST_SYNC_TIMESTAMP FROM SYNC_METADATA WHERE OBJECT_TYPE = %s",
            (object_type,)
        )
        result = cursor.fetchone()
        if not result or not result[0]:
            return None

        # SYNC_METADATA stores TIMESTAMP_NTZ, so the returned datetime is timezone-naive.
        # We treat these timestamps as UTC throughout the sync for consistency.
        ts: datetime = result[0]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat().replace('+00:00', 'Z')
    finally:
        cursor.close()


def parse_hubspot_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    v = str(value)
    # HubSpot sometimes returns milliseconds timestamps (string). Handle that as UTC.
    if v.isdigit():
        return datetime.fromtimestamp(int(v) / 1000.0, tz=timezone.utc)

    # Most CRM timestamps are ISO-8601 with a 'Z' suffix.
    try:
        dt = datetime.fromisoformat(v.replace('Z', '+00:00'))
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_utc_ntz(dt: Optional[datetime]) -> Optional[datetime]:
    """Convert a datetime to a timezone-naive UTC datetime for TIMESTAMP_NTZ storage."""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def should_run_reconciliation(conn, object_type: str) -> bool:
    """
    Determine if reconciliation should run for this sync

    Reconciliation runs if:
    1. It's never been run (first sync)
    2. It's been N syncs since last reconciliation (based on RECONCILIATION_FREQUENCY)
    3. Counts don't match (always run when there's a mismatch)
    """
    cursor = conn.cursor()
    try:
        # Get sync count and last reconciliation info
        cursor.execute("""
            SELECT
                COALESCE(RECONCILIATION_RUN_COUNT, 0) as run_count,
                COALESCE(LAST_RECONCILIATION_AT, '1970-01-01'::TIMESTAMP_NTZ) as last_recon,
                HUBSPOT_TOTAL_COUNT,
                SNOWFLAKE_TOTAL_COUNT
            FROM SYNC_METADATA
            WHERE OBJECT_TYPE = %s
        """, (object_type,))
        result = cursor.fetchone()

        if not result:
            # First sync ever - run reconciliation
            return True

        run_count, last_recon, hubspot_count, snowflake_count = result

        # If counts are unknown, run reconciliation to establish a baseline
        if hubspot_count is None or snowflake_count is None:
            return True

        # Always run if counts don't match
        if hubspot_count != snowflake_count:
            print(f"  Reconciliation triggered: Count mismatch detected ({hubspot_count} vs {snowflake_count})")
            return True

        # Run if we've reached the frequency threshold
        if run_count >= RECONCILIATION_FREQUENCY:
            print(f"  Reconciliation triggered: Periodic check (every {RECONCILIATION_FREQUENCY} syncs)")
            return True

        # Skip reconciliation
        if SKIP_RECONCILIATION_IF_COUNTS_MATCH:
            print(f"  Reconciliation skipped: Counts match ({hubspot_count}) - next full check in {RECONCILIATION_FREQUENCY - run_count} syncs")
            return False
        else:
            return True

    finally:
        cursor.close()

def update_reconciliation_metadata(conn, object_type: str, ran_reconciliation: bool):
    """Update reconciliation tracking metadata"""
    cursor = conn.cursor()
    try:
        if ran_reconciliation:
            # Reset counter, update last reconciliation time
            cursor.execute("""
                UPDATE SYNC_METADATA
                SET RECONCILIATION_RUN_COUNT = 0,
                    LAST_RECONCILIATION_AT = CURRENT_TIMESTAMP()
                WHERE OBJECT_TYPE = %s
            """, (object_type,))
        else:
            # Increment counter
            cursor.execute("""
                UPDATE SYNC_METADATA
                SET RECONCILIATION_RUN_COUNT = COALESCE(RECONCILIATION_RUN_COUNT, 0) + 1
                WHERE OBJECT_TYPE = %s
            """, (object_type,))
        conn.commit()
    finally:
        cursor.close()

def fetch_hubspot_data(object_type: str, properties: List[str], modified_since: Optional[str] = None) -> List[Dict]:
    """Fetch data from HubSpot API with server-side filtering and rate limiting"""
    
    # Add lastmodifieddate to properties for incremental loading
    # Note: contacts use 'lastmodifieddate', companies use 'hs_lastmodifieddate'
    mod_date_property = 'lastmodifieddate' if object_type == 'contacts' else 'hs_lastmodifieddate'
    all_properties = properties + [mod_date_property]
    
    # Use Search API for incremental loading (server-side filtering)
    if modified_since:
        print(f"  Using incremental load: fetching records modified since {modified_since}")
        url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}/search'
        headers = {
            'Authorization': f'Bearer {HUBSPOT_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Convert modified_since to milliseconds timestamp for HubSpot
        modified_dt = datetime.fromisoformat(modified_since.replace('Z', '+00:00') if 'Z' in modified_since else modified_since)
        if modified_dt.tzinfo is None:
            # Snowflake TIMESTAMP_NTZ values are timezone-less; we treat stored sync timestamps as UTC.
            modified_dt = modified_dt.replace(tzinfo=timezone.utc)
        start_ms = int(modified_dt.timestamp() * 1000)
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000) + 1

        # Windowed search to avoid HubSpot Search API's 10,000 total results cap per query.
        max_window_ms = int(os.environ.get('HUBSPOT_SEARCH_WINDOW_MS', str(6 * 60 * 60 * 1000)))
        min_window_ms = int(os.environ.get('HUBSPOT_SEARCH_WINDOW_MIN_MS', '1000'))

        def fetch_search_window(window_start_ms: int, window_end_ms: int) -> Tuple[List[Dict], bool, int]:
            payload = {
                'filterGroups': [{
                    'filters': [
                        {
                            'propertyName': mod_date_property,
                            'operator': 'GTE',
                            'value': str(window_start_ms)
                        },
                        {
                            'propertyName': mod_date_property,
                            'operator': 'LT',
                            'value': str(window_end_ms)
                        },
                    ]
                }],
                'properties': all_properties,
                'limit': 200,
            }

            window_records: List[Dict] = []
            after = None
            page_count = 0
            hit_limit = False
            total_matches = 0

            while True:
                if after is not None:
                    payload['after'] = str(after)
                else:
                    payload.pop('after', None)

                data = make_hubspot_request(url, headers, payload, method='POST')
                total_matches = int(data.get('total', 0) or 0)

                results = data.get('results', [])
                window_records.extend(results)
                page_count += 1

                # If we exceed the Search API cap, shrink the window (if possible) rather than paging further.
                if total_matches >= 10000:
                    hit_limit = True
                    if (window_end_ms - window_start_ms) > min_window_ms:
                        break

                if len(window_records) >= 10000:
                    hit_limit = True
                    break

                if 'paging' in data and 'next' in data['paging']:
                    after = data['paging']['next'].get('after', 0)
                    time.sleep(0.1)
                else:
                    break

            return window_records, hit_limit, page_count

        all_records: List[Dict] = []
        cursor_ms = start_ms
        current_window_ms = max_window_ms
        total_pages = 0
        window_count = 0

        print(f"  Using windowed Search API to avoid 10,000 result cap (window={max_window_ms}ms)")

        while cursor_ms < end_ms:
            window_end = min(cursor_ms + current_window_ms, end_ms)
            window_records, hit_limit, pages = fetch_search_window(cursor_ms, window_end)
            total_pages += pages

            if hit_limit and (window_end - cursor_ms) > min_window_ms:
                current_window_ms = max(min_window_ms, (window_end - cursor_ms) // 2)
                continue

            if hit_limit and (window_end - cursor_ms) <= min_window_ms:
                print("  ⚠️  Search window still hit 10,000 cap at minimum window size; results may be incomplete.")
                print("  Reconciliation will catch any missing records.")

            all_records.extend(window_records)
            window_count += 1
            cursor_ms = window_end

            # If we didn't hit the limit, try increasing window size back toward max for efficiency.
            if not hit_limit:
                current_window_ms = min(max_window_ms, max(current_window_ms, min_window_ms) * 2)

        if total_pages > 1:
            print(f"  (Fetched {total_pages} pages across {window_count} window(s) via Search API)")

        return all_records
    
    else:
        # Full sync - use standard objects API
        print(f"  Using full load: fetching all records")
        url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}'
        headers = {
            'Authorization': f'Bearer {HUBSPOT_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'limit': 100,
            'properties': ','.join(all_properties)
        }
        
        all_records = []
        after = None
        page_count = 0
        
        while True:
            if after:
                params['after'] = after
            
            # Use rate-limited request function
            data = make_hubspot_request(url, headers, params, method='GET')
            
            results = data.get('results', [])
            all_records.extend(results)
            page_count += 1
            
            paging = data.get('paging', {})
            if 'next' in paging:
                after = paging['next'].get('after')
                time.sleep(0.1)
            else:
                break
        
        if page_count > 1:
            print(f"  (Fetched {page_count} pages)")
        
        return all_records

def get_hubspot_total_count(object_type: str) -> int:
    """Get total count of records in HubSpot by paginating through all results with rate limiting"""
    url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    params = {'limit': 100}
    total_count = 0
    after = None
    
    while True:
        if after:
            params['after'] = after
        
        # Use rate-limited request function
        data = make_hubspot_request(url, headers, params, method='GET')
        
        results = data.get('results', [])
        total_count += len(results)
        
        paging = data.get('paging', {})
        if 'next' in paging:
            after = paging['next'].get('after')
            time.sleep(0.1)  # Small delay between pages
        else:
            break
    
    return total_count

def get_all_hubspot_ids(object_type: str) -> set:
    """Get all current HubSpot IDs for an object type (for soft delete detection)"""
    url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    params = {
        'limit': 100,
        'properties': 'id'  # Only fetch IDs, minimal data transfer
    }

    all_ids = set()
    after = None

    while True:
        if after:
            params['after'] = after

        # Use rate-limited request function
        data = make_hubspot_request(url, headers, params, method='GET')

        results = data.get('results', [])
        for record in results:
            all_ids.add(record['id'])

        paging = data.get('paging', {})
        if 'next' in paging:
            after = paging['next'].get('after')
            time.sleep(0.1)  # Small delay between pages
        else:
            break

    return all_ids

def fetch_records_by_ids(object_type: str, record_ids: List[str], properties: List[str]) -> List[Dict]:
    """Fetch specific records from HubSpot by their IDs"""
    if not record_ids:
        return []

    url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}/batch/read'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    # Determine modification date property
    mod_date_property = 'lastmodifieddate' if object_type == 'contacts' else 'hs_lastmodifieddate'
    all_properties = properties + [mod_date_property]

    all_records = []

    # HubSpot batch API accepts max 100 IDs per request
    batch_size = 100
    for i in range(0, len(record_ids), batch_size):
        batch_ids = record_ids[i:i + batch_size]

        payload = {
            'inputs': [{'id': rid} for rid in batch_ids],
            'properties': all_properties
        }

        try:
            data = make_hubspot_request(url, headers, payload, method='POST')
            results = data.get('results', [])
            all_records.extend(results)
            time.sleep(0.1)  # Rate limiting
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to fetch batch starting at index {i}: {e}")
            # Continue with other batches
            continue

    return all_records

def sync_contacts(conn):
    """
    Sync contacts from HubSpot to Snowflake with self-healing reconciliation

    Three-Phase Approach:
    - Phase 1: Incremental Sync - Fetch records modified since last sync
    - Phase 2: Reconciliation - Detect and sync any missing records (self-healing)
    - Phase 3: Soft Delete - Mark records deleted in HubSpot

    This ensures data consistency even if previous syncs were interrupted.
    """
    print("\n" + "=" * 70)
    print("SYNCING CONTACTS (Multi-Phase Self-Healing)")
    print("=" * 70)
    
    cursor = conn.cursor()

    try:
        # PHASE 1: INCREMENTAL SYNC - Fetch modified records
        print("\n" + "-" * 70)
        print("PHASE 1: INCREMENTAL SYNC - Fetching modified records")
        print("-" * 70)

        # Get last sync timestamp
        last_sync = get_last_sync_timestamp(conn, 'contacts')
        is_initial_full_sync = last_sync is None
        if last_sync:
            print(f"Last sync timestamp: {last_sync}")
            print(f"  (This will fetch contacts modified on or after this time)")
        else:
            print("Last sync timestamp: Never (performing full sync)")

        # Fetch contacts from HubSpot
        print("Fetching contacts from HubSpot...")
        properties = ['email', 'firstname', 'lastname', 'phone', 'jobtitle', 'company', 'createdate']
        contacts = fetch_hubspot_data('contacts', properties, last_sync)
        print(f"✓ Fetched {len(contacts)} contacts to sync")
        
        if len(contacts) > 0 and last_sync:
            # Show sample of what's being synced (contacts use 'lastmodifieddate')
            sample_dates = [c['properties'].get('lastmodifieddate') for c in contacts[:3] if c['properties'].get('lastmodifieddate')]
            if sample_dates:
                print(f"  Sample modification dates: {', '.join(sample_dates[:3])}")
        
        records_synced = len(contacts)
        latest_modified_dt: Optional[datetime] = None
        
        # Only perform insert/merge if there are contacts to sync
        if contacts:
            # Batch insert/update using temp table for better performance
            print("Creating temporary staging table...")
            cursor.execute("CREATE TEMPORARY TABLE CONTACTS_STAGE LIKE CONTACTS")
            
            # Insert all records into staging table
            print("Inserting records into staging table...")
            insert_sql = """
                INSERT INTO CONTACTS_STAGE (
                    HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, PHONE, JOBTITLE, COMPANY,
                    HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """
            batch_size = 5000
            batch_params = []
            for contact in contacts:
                props = contact['properties']
                hubspot_id = contact['id']
                batch_params.append((
                    hubspot_id,
                    props.get('email'),
                    props.get('firstname'),
                    props.get('lastname'),
                    props.get('phone'),
                    props.get('jobtitle'),
                    props.get('company'),
                    props.get('createdate'),
                    props.get('lastmodifieddate')  # contacts use 'lastmodifieddate'
                ))

                if len(batch_params) >= batch_size:
                    cursor.executemany(insert_sql, batch_params)
                    batch_params.clear()

            if batch_params:
                cursor.executemany(insert_sql, batch_params)
            
            # Perform bulk MERGE
            print("Merging staged records into CONTACTS table...")
            cursor.execute("""
                MERGE INTO CONTACTS AS target
                USING CONTACTS_STAGE AS source
                ON target.HUBSPOT_ID = source.HUBSPOT_ID
                WHEN MATCHED THEN UPDATE SET
                    EMAIL = source.EMAIL,
                    FIRSTNAME = source.FIRSTNAME,
                    LASTNAME = source.LASTNAME,
                    PHONE = source.PHONE,
                    JOBTITLE = source.JOBTITLE,
                    COMPANY = source.COMPANY,
                    HS_CREATEDATE = source.HS_CREATEDATE,
                    HS_LASTMODIFIEDDATE = source.HS_LASTMODIFIEDDATE,
                    SYNCED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, PHONE, JOBTITLE, COMPANY,
                    HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                ) VALUES (
                    source.HUBSPOT_ID, source.EMAIL, source.FIRSTNAME, source.LASTNAME,
                    source.PHONE, source.JOBTITLE, source.COMPANY, source.HS_CREATEDATE,
                    source.HS_LASTMODIFIEDDATE, CURRENT_TIMESTAMP()
                )
            """)
            
            print(f"✓ Merged {records_synced} records")
            
            # Get latest modified timestamp (UTC) - contacts use 'lastmodifieddate'
            for c in contacts:
                dt = parse_hubspot_datetime(c['properties'].get('lastmodifieddate'))
                if dt and (latest_modified_dt is None or dt > latest_modified_dt):
                    latest_modified_dt = dt
        else:
            print("No new or updated contacts to sync")

        current_hubspot_ids = set()
        run_reconciliation = False
        missing_ids = set()

        # PHASE 2: RECONCILIATION - Smart self-healing mechanism
        print("\n" + "-" * 70)
        print("PHASE 2: RECONCILIATION - Smart self-healing check")
        print("-" * 70)

        if is_initial_full_sync:
            print("Initial full sync detected - skipping reconciliation (baseline will be established from fetched records)")
            current_hubspot_ids = {c['id'] for c in contacts}
        else:
            run_reconciliation = should_run_reconciliation(conn, 'contacts')
            if run_reconciliation:
                print("Running full reconciliation...")
                print("Fetching all contact IDs from HubSpot...")
                current_hubspot_ids = get_all_hubspot_ids('contacts')
                print(f"  HubSpot total: {len(current_hubspot_ids)} contacts")

                cursor.execute("SELECT HUBSPOT_ID FROM CONTACTS WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
                snowflake_ids = {row[0] for row in cursor.fetchall()}
                print(f"  Snowflake total: {len(snowflake_ids)} active contacts")

                missing_ids = current_hubspot_ids - snowflake_ids
            else:
                print("Reconciliation skipped this cycle")

        if missing_ids:
            print(f"\n  Found {len(missing_ids)} missing contacts in Snowflake!")
            print(f"  These records exist in HubSpot but were never synced.")
            print(f"  Fetching and syncing missing contacts...")

            # Fetch full details for missing contacts
            properties = ['email', 'firstname', 'lastname', 'phone', 'jobtitle', 'company', 'createdate']
            missing_contacts = fetch_records_by_ids('contacts', list(missing_ids), properties)
            print(f"  Retrieved {len(missing_contacts)} missing contact records")

            if missing_contacts:
                # Create temp staging table for missing records
                print("  Creating temporary staging table for missing records...")
                cursor.execute("CREATE TEMPORARY TABLE CONTACTS_RECONCILE_STAGE LIKE CONTACTS")

                # Insert missing records into staging
                print("  Inserting missing records into staging table...")
                insert_sql = """
                    INSERT INTO CONTACTS_RECONCILE_STAGE (
                        HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, PHONE, JOBTITLE, COMPANY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """
                batch_size = 5000
                batch_params = []
                for contact in missing_contacts:
                    props = contact['properties']
                    hubspot_id = contact['id']
                    batch_params.append((
                        hubspot_id,
                        props.get('email'),
                        props.get('firstname'),
                        props.get('lastname'),
                        props.get('phone'),
                        props.get('jobtitle'),
                        props.get('company'),
                        props.get('createdate'),
                        props.get('lastmodifieddate')
                    ))

                    if len(batch_params) >= batch_size:
                        cursor.executemany(insert_sql, batch_params)
                        batch_params.clear()

                if batch_params:
                    cursor.executemany(insert_sql, batch_params)

                # Merge missing records into main table
                print("  Merging missing records into CONTACTS table...")
                cursor.execute("""
                    MERGE INTO CONTACTS AS target
                    USING CONTACTS_RECONCILE_STAGE AS source
                    ON target.HUBSPOT_ID = source.HUBSPOT_ID
                    WHEN NOT MATCHED THEN INSERT (
                        HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, PHONE, JOBTITLE, COMPANY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (
                        source.HUBSPOT_ID, source.EMAIL, source.FIRSTNAME, source.LASTNAME,
                        source.PHONE, source.JOBTITLE, source.COMPANY, source.HS_CREATEDATE,
                        source.HS_LASTMODIFIEDDATE, CURRENT_TIMESTAMP()
                    )
                """)

                print(f"  Self-healing sync completed: {len(missing_contacts)} missing contacts recovered")
                records_synced += len(missing_contacts)

                # Update latest_modified if needed
                missing_modified_dates = [c['properties'].get('lastmodifieddate') for c in missing_contacts if c['properties'].get('lastmodifieddate')]
                if missing_modified_dates:
                    for v in missing_modified_dates:
                        dt = parse_hubspot_datetime(v)
                        if dt and (latest_modified_dt is None or dt > latest_modified_dt):
                            latest_modified_dt = dt
        elif run_reconciliation:
            print("  No missing records detected - Snowflake is in sync")

        # Update reconciliation metadata (skip on initial full sync, since metadata row may not exist yet)
        if not is_initial_full_sync:
            update_reconciliation_metadata(conn, 'contacts', run_reconciliation)

        # PHASE 3: SOFT DELETE - Handle deletions
        print("\n" + "-" * 70)
        print("PHASE 3: SOFT DELETE - Checking for deleted contacts")
        print("-" * 70)

        if is_initial_full_sync:
            print("Initial full sync detected - skipping soft delete detection")
        elif run_reconciliation:
            cursor.execute("SELECT HUBSPOT_ID FROM CONTACTS WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
            snowflake_ids = {row[0] for row in cursor.fetchall()}
            print(f"  HubSpot IDs: {len(current_hubspot_ids)}")
            print(f"  Active Snowflake IDs: {len(snowflake_ids)}")

            deleted_ids = snowflake_ids - current_hubspot_ids
            if deleted_ids:
                print(f"Found {len(deleted_ids)} deleted contacts, marking as deleted...")
                cursor.execute("CREATE TEMPORARY TABLE DELETED_CONTACTS_STAGE (HUBSPOT_ID VARCHAR(50))")
                insert_sql = "INSERT INTO DELETED_CONTACTS_STAGE (HUBSPOT_ID) VALUES (%s)"
                batch_size = 5000
                batch_params = []
                for hubspot_id in deleted_ids:
                    batch_params.append((hubspot_id,))
                    if len(batch_params) >= batch_size:
                        cursor.executemany(insert_sql, batch_params)
                        batch_params.clear()
                if batch_params:
                    cursor.executemany(insert_sql, batch_params)

                cursor.execute("""
                    UPDATE CONTACTS AS t
                    SET IS_DELETED = TRUE,
                        DELETED_AT = CURRENT_TIMESTAMP()
                    FROM DELETED_CONTACTS_STAGE AS s
                    WHERE t.HUBSPOT_ID = s.HUBSPOT_ID
                """)
                print(f"✓ Marked {len(deleted_ids)} contacts as deleted")
            else:
                print("✓ No deletions detected")
        else:
            print("Soft delete detection skipped this cycle (will run on reconciliation cycles)")

        # PHASE 4: DUPLICATE CLEANUP - Remove any duplicates created during sync
        print("\n" + "-" * 70)
        print("PHASE 4: DUPLICATE CLEANUP - Ensuring data integrity")
        print("-" * 70)

        # Check for duplicates
        cursor.execute("""
            SELECT COUNT(*) as total, COUNT(DISTINCT HUBSPOT_ID) as unique_ids
            FROM CONTACTS
        """)
        total_rows, unique_ids = cursor.fetchone()

        if total_rows > unique_ids:
            duplicates_count = total_rows - unique_ids
            print(f"  Found {duplicates_count} duplicate records, cleaning up...")

            # Remove duplicates keeping most recent SYNCED_AT
            cursor.execute("""
                CREATE TEMPORARY TABLE CONTACTS_DEDUP AS
                SELECT HUBSPOT_ID,
                       MAX(EMAIL) as EMAIL,
                       MAX(FIRSTNAME) as FIRSTNAME,
                       MAX(LASTNAME) as LASTNAME,
                       MAX(PHONE) as PHONE,
                       MAX(JOBTITLE) as JOBTITLE,
                       MAX(COMPANY) as COMPANY,
                       MAX(HS_CREATEDATE) as HS_CREATEDATE,
                       MAX(HS_LASTMODIFIEDDATE) as HS_LASTMODIFIEDDATE,
                       MAX(SYNCED_AT) as SYNCED_AT,
                       MAX(IS_DELETED) as IS_DELETED,
                       MAX(DELETED_AT) as DELETED_AT
                FROM CONTACTS
                GROUP BY HUBSPOT_ID
            """)

            cursor.execute("TRUNCATE TABLE CONTACTS")
            cursor.execute("INSERT INTO CONTACTS SELECT * FROM CONTACTS_DEDUP")
            print(f"  ✓ Removed {duplicates_count} duplicate records")
        else:
            print("  ✓ No duplicates found")

        # Validate counts
        print("\n" + "-" * 70)
        print("FINAL VALIDATION")
        print("-" * 70)
        perform_full_validation = is_initial_full_sync or run_reconciliation

        if perform_full_validation:
            hubspot_total = len(current_hubspot_ids) if current_hubspot_ids else 0
            cursor.execute("SELECT COUNT(*) FROM CONTACTS WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
            snowflake_total = cursor.fetchone()[0]
            counts_match = (hubspot_total == snowflake_total)
        else:
            hubspot_total = None
            snowflake_total = None
            counts_match = None
        
        # Update sync metadata
        latest_modified = as_utc_ntz(latest_modified_dt)
        if perform_full_validation and latest_modified:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS LAST_SYNC_TIMESTAMP,
                    %s AS RECORDS_SYNCED,
                    %s AS HUBSPOT_TOTAL_COUNT,
                    %s AS SNOWFLAKE_TOTAL_COUNT,
                    %s AS COUNTS_MATCH
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    LAST_SYNC_TIMESTAMP = source.LAST_SYNC_TIMESTAMP,
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT = source.HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT = source.SNOWFLAKE_TOTAL_COUNT,
                    COUNTS_MATCH = source.COUNTS_MATCH,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, LAST_SYNC_TIMESTAMP, RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT, SNOWFLAKE_TOTAL_COUNT, COUNTS_MATCH, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.LAST_SYNC_TIMESTAMP, source.RECORDS_SYNCED,
                    source.HUBSPOT_TOTAL_COUNT, source.SNOWFLAKE_TOTAL_COUNT,
                    source.COUNTS_MATCH, CURRENT_TIMESTAMP()
                )
            """, ('contacts', latest_modified, records_synced, hubspot_total, snowflake_total, counts_match))
        elif perform_full_validation:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS RECORDS_SYNCED,
                    %s AS HUBSPOT_TOTAL_COUNT,
                    %s AS SNOWFLAKE_TOTAL_COUNT,
                    %s AS COUNTS_MATCH
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT = source.HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT = source.SNOWFLAKE_TOTAL_COUNT,
                    COUNTS_MATCH = source.COUNTS_MATCH,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, RECORDS_SYNCED, HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT, COUNTS_MATCH, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.RECORDS_SYNCED, source.HUBSPOT_TOTAL_COUNT,
                    source.SNOWFLAKE_TOTAL_COUNT, source.COUNTS_MATCH, CURRENT_TIMESTAMP()
                )
            """, ('contacts', records_synced, hubspot_total, snowflake_total, counts_match))
        else:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS LAST_SYNC_TIMESTAMP,
                    %s AS RECORDS_SYNCED
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    LAST_SYNC_TIMESTAMP = COALESCE(source.LAST_SYNC_TIMESTAMP, target.LAST_SYNC_TIMESTAMP),
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, LAST_SYNC_TIMESTAMP, RECORDS_SYNCED, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.LAST_SYNC_TIMESTAMP, source.RECORDS_SYNCED, CURRENT_TIMESTAMP()
                )
            """, ('contacts', latest_modified, records_synced))
        
        conn.commit()
        
        # Print reconciliation report
        print("\n--- CONTACTS RECONCILIATION REPORT ---")
        print(f"Records synced in this batch: {records_synced}")
        if perform_full_validation:
            print(f"HubSpot total count: {hubspot_total}")
            print(f"Snowflake total count: {snowflake_total}")
            print(f"Status: {'✅ COUNTS MATCH' if counts_match else '❌ COUNT MISMATCH'}")

            if not counts_match:
                raise ValueError(
                    f"Data validation failed: HubSpot has {hubspot_total} contacts "
                    f"but Snowflake has {snowflake_total} contacts. "
                    f"Difference: {hubspot_total - snowflake_total} records."
                )
        else:
            print("Count validation skipped (will be checked on reconciliation cycles)")
        
    finally:
        cursor.close()

def sync_companies(conn):
    """
    Sync companies from HubSpot to Snowflake with self-healing reconciliation

    Three-Phase Approach:
    - Phase 1: Incremental Sync - Fetch records modified since last sync
    - Phase 2: Reconciliation - Detect and sync any missing records (self-healing)
    - Phase 3: Soft Delete - Mark records deleted in HubSpot

    This ensures data consistency even if previous syncs were interrupted.
    """
    print("\n" + "=" * 70)
    print("SYNCING COMPANIES (Multi-Phase Self-Healing)")
    print("=" * 70)

    cursor = conn.cursor()

    try:
        # PHASE 1: INCREMENTAL SYNC - Fetch modified records
        print("\n" + "-" * 70)
        print("PHASE 1: INCREMENTAL SYNC - Fetching modified records")
        print("-" * 70)

        # Get last sync timestamp
        last_sync = get_last_sync_timestamp(conn, 'companies')
        is_initial_full_sync = last_sync is None
        if last_sync:
            print(f"Last sync timestamp: {last_sync}")
            print(f"  (This will fetch companies modified on or after this time)")
        else:
            print("Last sync timestamp: Never (performing full sync)")

        # Fetch companies from HubSpot
        print("Fetching companies from HubSpot...")
        properties = ['name', 'domain', 'industry', 'city', 'country', 'createdate']
        companies = fetch_hubspot_data('companies', properties, last_sync)
        print(f"✓ Fetched {len(companies)} companies to sync")
        
        if len(companies) > 0 and last_sync:
            # Show sample of what's being synced
            sample_dates = [c['properties'].get('hs_lastmodifieddate') for c in companies[:3] if c['properties'].get('hs_lastmodifieddate')]
            if sample_dates:
                print(f"  Sample modification dates: {', '.join(sample_dates[:3])}")
        
        records_synced = len(companies)
        latest_modified_dt: Optional[datetime] = None
        
        # Only perform insert/merge if there are companies to sync
        if companies:
            # Batch insert/update using temp table for better performance
            print("Creating temporary staging table...")
            cursor.execute("CREATE TEMPORARY TABLE COMPANIES_STAGE LIKE COMPANIES")
            
            # Insert all records into staging table
            print("Inserting records into staging table...")
            insert_sql = """
                INSERT INTO COMPANIES_STAGE (
                    HUBSPOT_ID, NAME, DOMAIN, INDUSTRY, CITY, COUNTRY,
                    HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """
            batch_size = 5000
            batch_params = []
            for company in companies:
                props = company['properties']
                hubspot_id = company['id']
                batch_params.append((
                    hubspot_id,
                    props.get('name'),
                    props.get('domain'),
                    props.get('industry'),
                    props.get('city'),
                    props.get('country'),
                    props.get('createdate'),
                    props.get('hs_lastmodifieddate')
                ))

                if len(batch_params) >= batch_size:
                    cursor.executemany(insert_sql, batch_params)
                    batch_params.clear()

            if batch_params:
                cursor.executemany(insert_sql, batch_params)
            
            # Perform bulk MERGE
            print("Merging staged records into COMPANIES table...")
            cursor.execute("""
                MERGE INTO COMPANIES AS target
                USING COMPANIES_STAGE AS source
                ON target.HUBSPOT_ID = source.HUBSPOT_ID
                WHEN MATCHED THEN UPDATE SET
                    NAME = source.NAME,
                    DOMAIN = source.DOMAIN,
                    INDUSTRY = source.INDUSTRY,
                    CITY = source.CITY,
                    COUNTRY = source.COUNTRY,
                    HS_CREATEDATE = source.HS_CREATEDATE,
                    HS_LASTMODIFIEDDATE = source.HS_LASTMODIFIEDDATE,
                    SYNCED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    HUBSPOT_ID, NAME, DOMAIN, INDUSTRY, CITY, COUNTRY,
                    HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                ) VALUES (
                    source.HUBSPOT_ID, source.NAME, source.DOMAIN, source.INDUSTRY,
                    source.CITY, source.COUNTRY, source.HS_CREATEDATE,
                    source.HS_LASTMODIFIEDDATE, CURRENT_TIMESTAMP()
                )
            """)
            
            print(f"✓ Merged {records_synced} records")
            
            # Get latest modified timestamp (UTC)
            for c in companies:
                dt = parse_hubspot_datetime(c['properties'].get('hs_lastmodifieddate'))
                if dt and (latest_modified_dt is None or dt > latest_modified_dt):
                    latest_modified_dt = dt
        else:
            print("No new or updated companies to sync")

        current_hubspot_ids = set()
        run_reconciliation = False
        missing_ids = set()

        # PHASE 2: RECONCILIATION - Smart self-healing mechanism
        print("\n" + "-" * 70)
        print("PHASE 2: RECONCILIATION - Smart self-healing check")
        print("-" * 70)

        if is_initial_full_sync:
            print("Initial full sync detected - skipping reconciliation (baseline will be established from fetched records)")
            current_hubspot_ids = {c['id'] for c in companies}
        else:
            run_reconciliation = should_run_reconciliation(conn, 'companies')
            if run_reconciliation:
                print("Running full reconciliation...")
                print("Fetching all company IDs from HubSpot...")
                current_hubspot_ids = get_all_hubspot_ids('companies')
                print(f"  HubSpot total: {len(current_hubspot_ids)} companies")

                cursor.execute("SELECT HUBSPOT_ID FROM COMPANIES WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
                snowflake_ids = {row[0] for row in cursor.fetchall()}
                print(f"  Snowflake total: {len(snowflake_ids)} active companies")

                missing_ids = current_hubspot_ids - snowflake_ids
            else:
                print("Reconciliation skipped this cycle")

        if missing_ids:
            print(f"\n  Found {len(missing_ids)} missing companies in Snowflake!")
            print(f"  These records exist in HubSpot but were never synced.")
            print(f"  Fetching and syncing missing companies...")

            # Fetch full details for missing companies
            properties = ['name', 'domain', 'industry', 'city', 'country', 'createdate']
            missing_companies = fetch_records_by_ids('companies', list(missing_ids), properties)
            print(f"  Retrieved {len(missing_companies)} missing company records")

            if missing_companies:
                # Create temp staging table for missing records
                print("  Creating temporary staging table for missing records...")
                cursor.execute("CREATE TEMPORARY TABLE COMPANIES_RECONCILE_STAGE LIKE COMPANIES")

                # Insert missing records into staging
                print("  Inserting missing records into staging table...")
                insert_sql = """
                    INSERT INTO COMPANIES_RECONCILE_STAGE (
                        HUBSPOT_ID, NAME, DOMAIN, INDUSTRY, CITY, COUNTRY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """
                batch_size = 5000
                batch_params = []
                for company in missing_companies:
                    props = company['properties']
                    hubspot_id = company['id']
                    batch_params.append((
                        hubspot_id,
                        props.get('name'),
                        props.get('domain'),
                        props.get('industry'),
                        props.get('city'),
                        props.get('country'),
                        props.get('createdate'),
                        props.get('hs_lastmodifieddate')
                    ))

                    if len(batch_params) >= batch_size:
                        cursor.executemany(insert_sql, batch_params)
                        batch_params.clear()

                if batch_params:
                    cursor.executemany(insert_sql, batch_params)

                # Merge missing records into main table
                print("  Merging missing records into COMPANIES table...")
                cursor.execute("""
                    MERGE INTO COMPANIES AS target
                    USING COMPANIES_RECONCILE_STAGE AS source
                    ON target.HUBSPOT_ID = source.HUBSPOT_ID
                    WHEN NOT MATCHED THEN INSERT (
                        HUBSPOT_ID, NAME, DOMAIN, INDUSTRY, CITY, COUNTRY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (
                        source.HUBSPOT_ID, source.NAME, source.DOMAIN, source.INDUSTRY,
                        source.CITY, source.COUNTRY, source.HS_CREATEDATE,
                        source.HS_LASTMODIFIEDDATE, CURRENT_TIMESTAMP()
                    )
                """)

                print(f"  Self-healing sync completed: {len(missing_companies)} missing companies recovered")
                records_synced += len(missing_companies)

                # Update latest_modified if needed
                missing_modified_dates = [c['properties'].get('hs_lastmodifieddate') for c in missing_companies if c['properties'].get('hs_lastmodifieddate')]
                if missing_modified_dates:
                    for v in missing_modified_dates:
                        dt = parse_hubspot_datetime(v)
                        if dt and (latest_modified_dt is None or dt > latest_modified_dt):
                            latest_modified_dt = dt
        elif run_reconciliation:
            print("  No missing records detected - Snowflake is in sync")

        # Update reconciliation metadata (skip on initial full sync, since metadata row may not exist yet)
        if not is_initial_full_sync:
            update_reconciliation_metadata(conn, 'companies', run_reconciliation)

        # PHASE 3: SOFT DELETE - Handle deletions
        print("\n" + "-" * 70)
        print("PHASE 3: SOFT DELETE - Checking for deleted companies")
        print("-" * 70)

        if is_initial_full_sync:
            print("Initial full sync detected - skipping soft delete detection")
        elif run_reconciliation:
            cursor.execute("SELECT HUBSPOT_ID FROM COMPANIES WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
            snowflake_ids = {row[0] for row in cursor.fetchall()}
            print(f"  HubSpot IDs: {len(current_hubspot_ids)}")
            print(f"  Active Snowflake IDs: {len(snowflake_ids)}")

            deleted_ids = snowflake_ids - current_hubspot_ids
            if deleted_ids:
                print(f"Found {len(deleted_ids)} deleted companies, marking as deleted...")
                cursor.execute("CREATE TEMPORARY TABLE DELETED_COMPANIES_STAGE (HUBSPOT_ID VARCHAR(50))")
                insert_sql = "INSERT INTO DELETED_COMPANIES_STAGE (HUBSPOT_ID) VALUES (%s)"
                batch_size = 5000
                batch_params = []
                for hubspot_id in deleted_ids:
                    batch_params.append((hubspot_id,))
                    if len(batch_params) >= batch_size:
                        cursor.executemany(insert_sql, batch_params)
                        batch_params.clear()
                if batch_params:
                    cursor.executemany(insert_sql, batch_params)

                cursor.execute("""
                    UPDATE COMPANIES AS t
                    SET IS_DELETED = TRUE,
                        DELETED_AT = CURRENT_TIMESTAMP()
                    FROM DELETED_COMPANIES_STAGE AS s
                    WHERE t.HUBSPOT_ID = s.HUBSPOT_ID
                """)
                print(f"✓ Marked {len(deleted_ids)} companies as deleted")
            else:
                print("✓ No deletions detected")
        else:
            print("Soft delete detection skipped this cycle (will run on reconciliation cycles)")

        # PHASE 4: DUPLICATE CLEANUP - Remove any duplicates created during sync
        print("\n" + "-" * 70)
        print("PHASE 4: DUPLICATE CLEANUP - Ensuring data integrity")
        print("-" * 70)

        # Check for duplicates
        cursor.execute("""
            SELECT COUNT(*) as total, COUNT(DISTINCT HUBSPOT_ID) as unique_ids
            FROM COMPANIES
        """)
        total_rows, unique_ids = cursor.fetchone()

        if total_rows > unique_ids:
            duplicates_count = total_rows - unique_ids
            print(f"  Found {duplicates_count} duplicate records, cleaning up...")

            # Remove duplicates keeping most recent SYNCED_AT
            cursor.execute("""
                CREATE TEMPORARY TABLE COMPANIES_DEDUP AS
                SELECT HUBSPOT_ID,
                       MAX(NAME) as NAME,
                       MAX(DOMAIN) as DOMAIN,
                       MAX(INDUSTRY) as INDUSTRY,
                       MAX(CITY) as CITY,
                       MAX(COUNTRY) as COUNTRY,
                       MAX(HS_CREATEDATE) as HS_CREATEDATE,
                       MAX(HS_LASTMODIFIEDDATE) as HS_LASTMODIFIEDDATE,
                       MAX(SYNCED_AT) as SYNCED_AT,
                       MAX(IS_DELETED) as IS_DELETED,
                       MAX(DELETED_AT) as DELETED_AT
                FROM COMPANIES
                GROUP BY HUBSPOT_ID
            """)

            cursor.execute("TRUNCATE TABLE COMPANIES")
            cursor.execute("INSERT INTO COMPANIES SELECT * FROM COMPANIES_DEDUP")
            print(f"  ✓ Removed {duplicates_count} duplicate records")
        else:
            print("  ✓ No duplicates found")

        # Validate counts
        print("\n" + "-" * 70)
        print("FINAL VALIDATION")
        print("-" * 70)
        perform_full_validation = is_initial_full_sync or run_reconciliation

        if perform_full_validation:
            hubspot_total = len(current_hubspot_ids) if current_hubspot_ids else 0
            cursor.execute("SELECT COUNT(*) FROM COMPANIES WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
            snowflake_total = cursor.fetchone()[0]
            counts_match = (hubspot_total == snowflake_total)
        else:
            hubspot_total = None
            snowflake_total = None
            counts_match = None
        
        # Update sync metadata
        latest_modified = as_utc_ntz(latest_modified_dt)
        if perform_full_validation and latest_modified:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS LAST_SYNC_TIMESTAMP,
                    %s AS RECORDS_SYNCED,
                    %s AS HUBSPOT_TOTAL_COUNT,
                    %s AS SNOWFLAKE_TOTAL_COUNT,
                    %s AS COUNTS_MATCH
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    LAST_SYNC_TIMESTAMP = source.LAST_SYNC_TIMESTAMP,
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT = source.HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT = source.SNOWFLAKE_TOTAL_COUNT,
                    COUNTS_MATCH = source.COUNTS_MATCH,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, LAST_SYNC_TIMESTAMP, RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT, SNOWFLAKE_TOTAL_COUNT, COUNTS_MATCH, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.LAST_SYNC_TIMESTAMP, source.RECORDS_SYNCED,
                    source.HUBSPOT_TOTAL_COUNT, source.SNOWFLAKE_TOTAL_COUNT,
                    source.COUNTS_MATCH, CURRENT_TIMESTAMP()
                )
            """, ('companies', latest_modified, records_synced, hubspot_total, snowflake_total, counts_match))
        elif perform_full_validation:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS RECORDS_SYNCED,
                    %s AS HUBSPOT_TOTAL_COUNT,
                    %s AS SNOWFLAKE_TOTAL_COUNT,
                    %s AS COUNTS_MATCH
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    HUBSPOT_TOTAL_COUNT = source.HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT = source.SNOWFLAKE_TOTAL_COUNT,
                    COUNTS_MATCH = source.COUNTS_MATCH,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, RECORDS_SYNCED, HUBSPOT_TOTAL_COUNT,
                    SNOWFLAKE_TOTAL_COUNT, COUNTS_MATCH, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.RECORDS_SYNCED, source.HUBSPOT_TOTAL_COUNT,
                    source.SNOWFLAKE_TOTAL_COUNT, source.COUNTS_MATCH, CURRENT_TIMESTAMP()
                )
            """, ('companies', records_synced, hubspot_total, snowflake_total, counts_match))
        else:
            cursor.execute("""
                MERGE INTO SYNC_METADATA AS target
                USING (SELECT 
                    %s AS OBJECT_TYPE,
                    %s AS LAST_SYNC_TIMESTAMP,
                    %s AS RECORDS_SYNCED
                ) AS source
                ON target.OBJECT_TYPE = source.OBJECT_TYPE
                WHEN MATCHED THEN UPDATE SET
                    LAST_SYNC_TIMESTAMP = COALESCE(source.LAST_SYNC_TIMESTAMP, target.LAST_SYNC_TIMESTAMP),
                    RECORDS_SYNCED = source.RECORDS_SYNCED,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    OBJECT_TYPE, LAST_SYNC_TIMESTAMP, RECORDS_SYNCED, UPDATED_AT
                ) VALUES (
                    source.OBJECT_TYPE, source.LAST_SYNC_TIMESTAMP, source.RECORDS_SYNCED, CURRENT_TIMESTAMP()
                )
            """, ('companies', latest_modified, records_synced))
        
        conn.commit()
        
        # Print reconciliation report
        print("\n--- COMPANIES RECONCILIATION REPORT ---")
        print(f"Records synced in this batch: {records_synced}")
        if perform_full_validation:
            print(f"HubSpot total count: {hubspot_total}")
            print(f"Snowflake total count: {snowflake_total}")
            print(f"Status: {'✅ COUNTS MATCH' if counts_match else '❌ COUNT MISMATCH'}")

            if not counts_match:
                raise ValueError(
                    f"Data validation failed: HubSpot has {hubspot_total} companies "
                    f"but Snowflake has {snowflake_total} companies. "
                    f"Difference: {hubspot_total - snowflake_total} records."
                )
        else:
            print("Count validation skipped (will be checked on reconciliation cycles)")
        
    finally:
        cursor.close()

def main():
    """Main sync process"""
    print("=" * 70)
    print("HUBSPOT TO SNOWFLAKE SYNC")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    print(f"Database: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    
    try:
        # Connect to Snowflake
        print("\nConnecting to Snowflake...")
        conn = get_snowflake_connection()
        print("✓ Connected successfully")
        
        # Initialize schema
        initialize_snowflake_schema(conn)
        
        # Sync contacts and companies
        sync_contacts(conn)
        sync_companies(conn)
        
        print("\n" + "=" * 70)
        print("✅ SYNC COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"Completed at: {datetime.now()}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
