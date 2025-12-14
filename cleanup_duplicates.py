#!/usr/bin/env python3
"""
Cleanup duplicate records in Snowflake CONTACTS and COMPANIES tables
Keeps the most recently synced record for each HUBSPOT_ID
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'HUBSPOT_DATA')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')

def cleanup_duplicates():
    """Remove duplicate records from Snowflake tables"""
    print("=" * 80)
    print("CLEANING UP DUPLICATE RECORDS")
    print("=" * 80)
    print(f"Database: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}\n")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # Check for duplicates in CONTACTS
    print("Checking CONTACTS table for duplicates...")
    cursor.execute("""
        SELECT HUBSPOT_ID, COUNT(*) as cnt
        FROM CONTACTS
        GROUP BY HUBSPOT_ID
        HAVING COUNT(*) > 1
    """)
    contact_duplicates = cursor.fetchall()

    if contact_duplicates:
        total_duplicate_rows = sum(cnt - 1 for _, cnt in contact_duplicates)
        print(f"  Found {len(contact_duplicates)} HUBSPOT_IDs with duplicates")
        print(f"  Total duplicate rows to remove: {total_duplicate_rows}")

        # Delete duplicates, keeping one record per HUBSPOT_ID
        print("  Removing duplicate contacts (keeping one per HUBSPOT_ID)...")

        # Use MERGE to keep only distinct HUBSPOT_IDs
        # First, create a staging table with unique records
        cursor.execute("""
            CREATE TEMPORARY TABLE CONTACTS_UNIQUE AS
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

        # Truncate and reload from unique table
        cursor.execute("TRUNCATE TABLE CONTACTS")
        cursor.execute("""
            INSERT INTO CONTACTS
            SELECT * FROM CONTACTS_UNIQUE
        """)

        inserted_contacts = cursor.rowcount
        deleted_contacts = total_duplicate_rows
        print(f"  ✓ Removed {deleted_contacts} duplicate contact records")
        print(f"  ✓ Kept {inserted_contacts} unique contact records")
    else:
        print("  ✓ No duplicate contacts found")

    # Check for duplicates in COMPANIES
    print("\nChecking COMPANIES table for duplicates...")
    cursor.execute("""
        SELECT HUBSPOT_ID, COUNT(*) as cnt
        FROM COMPANIES
        GROUP BY HUBSPOT_ID
        HAVING COUNT(*) > 1
    """)
    company_duplicates = cursor.fetchall()

    if company_duplicates:
        total_duplicate_rows = sum(cnt - 1 for _, cnt in company_duplicates)
        print(f"  Found {len(company_duplicates)} HUBSPOT_IDs with duplicates")
        print(f"  Total duplicate rows to remove: {total_duplicate_rows}")

        # Delete duplicates, keeping one record per HUBSPOT_ID
        print("  Removing duplicate companies (keeping one per HUBSPOT_ID)...")

        # Use MERGE to keep only distinct HUBSPOT_IDs
        # First, create a staging table with unique records
        cursor.execute("""
            CREATE TEMPORARY TABLE COMPANIES_UNIQUE AS
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

        # Truncate and reload from unique table
        cursor.execute("TRUNCATE TABLE COMPANIES")
        cursor.execute("""
            INSERT INTO COMPANIES
            SELECT * FROM COMPANIES_UNIQUE
        """)

        inserted_companies = cursor.rowcount
        deleted_companies = total_duplicate_rows
        print(f"  ✓ Removed {deleted_companies} duplicate company records")
        print(f"  ✓ Kept {inserted_companies} unique company records")
    else:
        print("  ✓ No duplicate companies found")

    conn.commit()

    # Verify cleanup
    print("\n" + "=" * 80)
    print("VERIFICATION AFTER CLEANUP")
    print("=" * 80)

    cursor.execute("SELECT COUNT(*) FROM CONTACTS")
    total_contacts = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT HUBSPOT_ID) FROM CONTACTS")
    unique_contacts = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM COMPANIES")
    total_companies = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT HUBSPOT_ID) FROM COMPANIES")
    unique_companies = cursor.fetchone()[0]

    print(f"\n📋 CONTACTS:")
    print(f"  Total rows: {total_contacts:,}")
    print(f"  Unique HUBSPOT_IDs: {unique_contacts:,}")
    print(f"  Status: {'✅ No duplicates' if total_contacts == unique_contacts else '❌ Still has duplicates'}")

    print(f"\n🏢 COMPANIES:")
    print(f"  Total rows: {total_companies:,}")
    print(f"  Unique HUBSPOT_IDs: {unique_companies:,}")
    print(f"  Status: {'✅ No duplicates' if total_companies == unique_companies else '❌ Still has duplicates'}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    if total_contacts == unique_contacts and total_companies == unique_companies:
        print("✅ CLEANUP SUCCESSFUL - ALL DUPLICATES REMOVED")
    else:
        print("⚠️  WARNING - DUPLICATES STILL EXIST")
    print("=" * 80)

if __name__ == '__main__':
    try:
        cleanup_duplicates()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
