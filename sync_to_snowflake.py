#!/usr/bin/env python3
"""
HubSpot to Snowflake Sync - Incremental data synchronization with validation
"""

import os
import sys
import requests
import snowflake.connector
import time
from datetime import datetime
from typing import List, Dict, Optional

# Configuration from environment variables
HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'HUBSPOT_DATA')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')

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

def make_hubspot_request(url: str, headers: dict, params: dict, method: str = 'GET') -> dict:
    """Make HubSpot API request with exponential backoff retry logic"""
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=params)
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
            
            # Raise for other HTTP errors
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
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
        
        # Create SYNC_METADATA table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SYNC_METADATA (
                OBJECT_TYPE VARCHAR(50) PRIMARY KEY,
                LAST_SYNC_TIMESTAMP TIMESTAMP_NTZ,
                RECORDS_SYNCED INTEGER,
                HUBSPOT_TOTAL_COUNT INTEGER,
                SNOWFLAKE_TOTAL_COUNT INTEGER,
                COUNTS_MATCH BOOLEAN,
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
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
        return result[0].isoformat() if result and result[0] else None
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
        from datetime import datetime as dt
        modified_dt = dt.fromisoformat(modified_since.replace('Z', '+00:00') if 'Z' in modified_since else modified_since)
        modified_ms = int(modified_dt.timestamp() * 1000)
        
        payload = {
            'filterGroups': [{
                'filters': [{
                    'propertyName': mod_date_property,
                    'operator': 'GTE',  # Greater than or equal to
                    'value': str(modified_ms)
                }]
            }],
            'properties': all_properties,
            'limit': 100
        }
        
        all_records = []
        after = 0
        page_count = 0
        
        while True:
            payload['after'] = after
            
            # Use rate-limited request function
            data = make_hubspot_request(url, headers, payload, method='POST')
            
            results = data.get('results', [])
            all_records.extend(results)
            page_count += 1
            
            # Check if there are more results
            if 'paging' in data and 'next' in data['paging']:
                after = data['paging']['next'].get('after', 0)
                time.sleep(0.1)
            else:
                break
        
        if page_count > 1:
            print(f"  (Fetched {page_count} pages via Search API)")
        
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

def sync_contacts(conn):
    """Sync contacts from HubSpot to Snowflake"""
    print("\n" + "=" * 70)
    print("SYNCING CONTACTS")
    print("=" * 70)
    
    cursor = conn.cursor()
    
    try:
        # Get last sync timestamp
        last_sync = get_last_sync_timestamp(conn, 'contacts')
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
        latest_modified = None
        
        # Only perform insert/merge if there are contacts to sync
        if contacts:
            # Batch insert/update using temp table for better performance
            print("Creating temporary staging table...")
            cursor.execute("CREATE TEMPORARY TABLE CONTACTS_STAGE LIKE CONTACTS")
            
            # Insert all records into staging table
            print("Inserting records into staging table...")
            for contact in contacts:
                props = contact['properties']
                hubspot_id = contact['id']
                
                cursor.execute("""
                    INSERT INTO CONTACTS_STAGE (
                        HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, PHONE, JOBTITLE, COMPANY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """, (
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
            
            # Get latest modified timestamp (filter out None values) - contacts use 'lastmodifieddate'
            modified_dates = [c['properties'].get('lastmodifieddate') for c in contacts if c['properties'].get('lastmodifieddate')]
            latest_modified = max(modified_dates) if modified_dates else None
        else:
            print("No new or updated contacts to sync")
        
        # Handle soft deletes: Mark records deleted in HubSpot
        print("\nChecking for deleted contacts...")
        current_hubspot_ids = get_all_hubspot_ids('contacts')
        print(f"Current HubSpot IDs: {len(current_hubspot_ids)}")
        
        # Get all Snowflake IDs that are not already marked as deleted
        cursor.execute("SELECT HUBSPOT_ID FROM CONTACTS WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
        snowflake_ids = {row[0] for row in cursor.fetchall()}
        print(f"Active Snowflake IDs: {len(snowflake_ids)}")
        
        # Find IDs in Snowflake but not in HubSpot (deleted records)
        deleted_ids = snowflake_ids - current_hubspot_ids
        
        if deleted_ids:
            print(f"Found {len(deleted_ids)} deleted contacts, marking as deleted...")
            # Mark as deleted in batches
            for hubspot_id in deleted_ids:
                cursor.execute("""
                    UPDATE CONTACTS 
                    SET IS_DELETED = TRUE, DELETED_AT = CURRENT_TIMESTAMP()
                    WHERE HUBSPOT_ID = %s
                """, (hubspot_id,))
            print(f"✓ Marked {len(deleted_ids)} contacts as deleted")
        else:
            print("✓ No deletions detected")
        
        # Validate counts by comparing with actual HubSpot total
        print("\nValidating data...")
        print("Getting actual HubSpot total count...")
        hubspot_total = get_hubspot_total_count('contacts')
        
        # Count only active (non-deleted) records in Snowflake
        cursor.execute("SELECT COUNT(*) FROM CONTACTS WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
        snowflake_total = cursor.fetchone()[0]
        
        counts_match = (hubspot_total == snowflake_total)
        
        # Update sync metadata (only update timestamp if we actually synced records)
        if latest_modified:
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
        else:
            # No records synced, only update counts and match status
            cursor.execute("""
                UPDATE SYNC_METADATA
                SET HUBSPOT_TOTAL_COUNT = %s,
                    SNOWFLAKE_TOTAL_COUNT = %s,
                    COUNTS_MATCH = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE OBJECT_TYPE = %s
            """, (hubspot_total, snowflake_total, counts_match, 'contacts'))
        
        conn.commit()
        
        # Print reconciliation report
        print("\n--- CONTACTS RECONCILIATION REPORT ---")
        print(f"Records synced in this batch: {records_synced}")
        print(f"HubSpot total count: {hubspot_total}")
        print(f"Snowflake total count: {snowflake_total}")
        print(f"Status: {'✅ COUNTS MATCH' if counts_match else '❌ COUNT MISMATCH'}")
        
        # Fail if counts don't match
        if not counts_match:
            raise ValueError(
                f"Data validation failed: HubSpot has {hubspot_total} contacts "
                f"but Snowflake has {snowflake_total} contacts. "
                f"Difference: {hubspot_total - snowflake_total} records."
            )
        
    finally:
        cursor.close()

def sync_companies(conn):
    """Sync companies from HubSpot to Snowflake"""
    print("\n" + "=" * 70)
    print("SYNCING COMPANIES")
    print("=" * 70)
    
    cursor = conn.cursor()
    
    try:
        # Get last sync timestamp
        last_sync = get_last_sync_timestamp(conn, 'companies')
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
        latest_modified = None
        
        # Only perform insert/merge if there are companies to sync
        if companies:
            # Batch insert/update using temp table for better performance
            print("Creating temporary staging table...")
            cursor.execute("CREATE TEMPORARY TABLE COMPANIES_STAGE LIKE COMPANIES")
            
            # Insert all records into staging table
            print("Inserting records into staging table...")
            for company in companies:
                props = company['properties']
                hubspot_id = company['id']
                
                cursor.execute("""
                    INSERT INTO COMPANIES_STAGE (
                        HUBSPOT_ID, NAME, DOMAIN, INDUSTRY, CITY, COUNTRY,
                        HS_CREATEDATE, HS_LASTMODIFIEDDATE, SYNCED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """, (
                    hubspot_id,
                    props.get('name'),
                    props.get('domain'),
                    props.get('industry'),
                    props.get('city'),
                    props.get('country'),
                    props.get('createdate'),
                    props.get('hs_lastmodifieddate')
                ))
            
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
            
            # Get latest modified timestamp (filter out None values)
            modified_dates = [c['properties'].get('hs_lastmodifieddate') for c in companies if c['properties'].get('hs_lastmodifieddate')]
            latest_modified = max(modified_dates) if modified_dates else None
        else:
            print("No new or updated companies to sync")
        
        # Handle soft deletes: Mark records deleted in HubSpot
        print("\nChecking for deleted companies...")
        current_hubspot_ids = get_all_hubspot_ids('companies')
        print(f"Current HubSpot IDs: {len(current_hubspot_ids)}")
        
        # Get all Snowflake IDs that are not already marked as deleted
        cursor.execute("SELECT HUBSPOT_ID FROM COMPANIES WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
        snowflake_ids = {row[0] for row in cursor.fetchall()}
        print(f"Active Snowflake IDs: {len(snowflake_ids)}")
        
        # Find IDs in Snowflake but not in HubSpot (deleted records)
        deleted_ids = snowflake_ids - current_hubspot_ids
        
        if deleted_ids:
            print(f"Found {len(deleted_ids)} deleted companies, marking as deleted...")
            # Mark as deleted in batches
            for hubspot_id in deleted_ids:
                cursor.execute("""
                    UPDATE COMPANIES 
                    SET IS_DELETED = TRUE, DELETED_AT = CURRENT_TIMESTAMP()
                    WHERE HUBSPOT_ID = %s
                """, (hubspot_id,))
            print(f"✓ Marked {len(deleted_ids)} companies as deleted")
        else:
            print("✓ No deletions detected")
        
        # Validate counts by comparing with actual HubSpot total
        print("\nValidating data...")
        print("Getting actual HubSpot total count...")
        hubspot_total = get_hubspot_total_count('companies')
        
        # Count only active (non-deleted) records in Snowflake
        cursor.execute("SELECT COUNT(*) FROM COMPANIES WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL")
        snowflake_total = cursor.fetchone()[0]
        
        counts_match = (hubspot_total == snowflake_total)
        
        # Update sync metadata (only update timestamp if we actually synced records)
        if latest_modified:
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
        else:
            # No records synced, only update counts and match status
            cursor.execute("""
                UPDATE SYNC_METADATA
                SET HUBSPOT_TOTAL_COUNT = %s,
                    SNOWFLAKE_TOTAL_COUNT = %s,
                    COUNTS_MATCH = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE OBJECT_TYPE = %s
            """, (hubspot_total, snowflake_total, counts_match, 'companies'))
        
        conn.commit()
        
        # Print reconciliation report
        print("\n--- COMPANIES RECONCILIATION REPORT ---")
        print(f"Records synced in this batch: {records_synced}")
        print(f"HubSpot total count: {hubspot_total}")
        print(f"Snowflake total count: {snowflake_total}")
        print(f"Status: {'✅ COUNTS MATCH' if counts_match else '❌ COUNT MISMATCH'}")
        
        # Fail if counts don't match
        if not counts_match:
            raise ValueError(
                f"Data validation failed: HubSpot has {hubspot_total} companies "
                f"but Snowflake has {snowflake_total} companies. "
                f"Difference: {hubspot_total - snowflake_total} records."
            )
        
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
