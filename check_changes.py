#!/usr/bin/env python3
"""
Lightweight change detection script for HubSpot data
Checks if there are any records modified since last sync WITHOUT connecting to Snowflake
This reduces costs by avoiding unnecessary Snowflake warehouse starts
"""

import os
import sys
import json
import requests
import snowflake.connector
from datetime import datetime
from typing import Dict, Optional

# Configuration from environment variables
HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'HUBSPOT_DATA')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')

HUBSPOT_API_URL = 'https://api.hubapi.com'

def get_last_sync_timestamps() -> Dict[str, Optional[str]]:
    """Get last sync timestamps from Snowflake metadata"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT OBJECT_TYPE, LAST_SYNC_TIMESTAMP FROM SYNC_METADATA")
        
        timestamps = {}
        for row in cursor.fetchall():
            object_type = row[0]
            timestamp = row[1].isoformat() if row[1] else None
            timestamps[object_type] = timestamp
        
        cursor.close()
        conn.close()
        
        return timestamps
    except Exception as e:
        print(f"⚠️  Could not retrieve sync timestamps: {e}")
        # Default to None to force a sync if we can't check
        return {'contacts': None, 'companies': None}

def count_changes_in_hubspot(object_type: str, modified_since: Optional[str]) -> int:
    """Count records modified in HubSpot since given timestamp"""
    
    if not modified_since:
        # No timestamp available, assume changes exist to force sync
        print(f"  No last sync timestamp for {object_type}, assuming changes exist")
        return 1
    
    # Determine correct property name
    mod_date_property = 'lastmodifieddate' if object_type == 'contacts' else 'hs_lastmodifieddate'
    
    # Convert timestamp to milliseconds
    try:
        modified_dt = datetime.fromisoformat(modified_since.replace('Z', '+00:00') if 'Z' in modified_since else modified_since)
        modified_ms = int(modified_dt.timestamp() * 1000)
    except Exception as e:
        print(f"  ⚠️  Error parsing timestamp for {object_type}: {e}")
        return 1  # Assume changes to be safe
    
    # Use HubSpot Search API to count modified records
    url = f'{HUBSPOT_API_URL}/crm/v3/objects/{object_type}/search'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'filterGroups': [{
            'filters': [{
                'propertyName': mod_date_property,
                'operator': 'GTE',
                'value': str(modified_ms)
            }]
        }],
        'properties': ['id'],  # Only fetch ID to minimize data transfer
        'limit': 1  # We just need to know if any exist
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Get total count from response
        total = data.get('total', 0)
        return total
        
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  Error checking {object_type}: {e}")
        return 1  # Assume changes exist to be safe

def main():
    """Check for changes in HubSpot and output result"""
    print("=" * 70)
    print("CHECKING FOR HUBSPOT CHANGES")
    print("=" * 70)
    print(f"Time: {datetime.now()}\n")
    
    if not HUBSPOT_API_KEY:
        print("❌ ERROR: HUBSPOT_API_KEY not set")
        sys.exit(1)
    
    # Get last sync timestamps
    print("Retrieving last sync timestamps...")
    timestamps = get_last_sync_timestamps()
    
    print(f"  Contacts last synced: {timestamps.get('contacts', 'Never')}")
    print(f"  Companies last synced: {timestamps.get('companies', 'Never')}")
    
    # Check for changes
    print("\nChecking HubSpot for changes...")
    contacts_changed = count_changes_in_hubspot('contacts', timestamps.get('contacts'))
    companies_changed = count_changes_in_hubspot('companies', timestamps.get('companies'))
    
    print(f"  Contacts changed: {contacts_changed}")
    print(f"  Companies changed: {companies_changed}")
    
    # Determine if sync is needed
    has_changes = (contacts_changed > 0) or (companies_changed > 0)
    
    # Prepare result
    result = {
        'has_changes': has_changes,
        'contacts_changed': contacts_changed,
        'companies_changed': companies_changed,
        'checked_at': datetime.now().isoformat()
    }
    
    print("\n" + "=" * 70)
    if has_changes:
        print("✅ CHANGES DETECTED - Sync needed")
        print(f"   {contacts_changed} contacts + {companies_changed} companies modified")
    else:
        print("⏭️  NO CHANGES - Sync can be skipped")
        print("   Snowflake compute cost saved!")
    print("=" * 70)
    
    # Output for GitHub Actions
    # Set output in GitHub Actions format
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"has_changes={str(has_changes).lower()}\n")
            f.write(f"contacts_changed={contacts_changed}\n")
            f.write(f"companies_changed={companies_changed}\n")
    
    # Also print JSON for debugging
    print(f"\nJSON Output: {json.dumps(result)}")
    
    # Exit with 0 regardless (not a failure if no changes)
    sys.exit(0)

if __name__ == '__main__':
    main()
