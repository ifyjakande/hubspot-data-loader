#!/usr/bin/env python3
"""
Add IS_DELETED columns to CONTACTS and COMPANIES tables for soft delete functionality
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

print("=" * 70)
print("ADDING SOFT DELETE COLUMNS")
print("=" * 70)

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()

# Check if columns already exist
print("\nChecking existing columns...")
cursor.execute("DESCRIBE TABLE CONTACTS")
contacts_columns = [row[0] for row in cursor.fetchall()]

cursor.execute("DESCRIBE TABLE COMPANIES")
companies_columns = [row[0] for row in cursor.fetchall()]

# Add IS_DELETED column to CONTACTS if not exists
if 'IS_DELETED' not in contacts_columns:
    print("\nAdding IS_DELETED column to CONTACTS table...")
    cursor.execute("""
        ALTER TABLE CONTACTS 
        ADD COLUMN IS_DELETED BOOLEAN DEFAULT FALSE
    """)
    print("✅ IS_DELETED column added to CONTACTS")
else:
    print("\n✓ IS_DELETED column already exists in CONTACTS")

# Add IS_DELETED column to COMPANIES if not exists
if 'IS_DELETED' not in companies_columns:
    print("Adding IS_DELETED column to COMPANIES table...")
    cursor.execute("""
        ALTER TABLE COMPANIES 
        ADD COLUMN IS_DELETED BOOLEAN DEFAULT FALSE
    """)
    print("✅ IS_DELETED column added to COMPANIES")
else:
    print("✓ IS_DELETED column already exists in COMPANIES")

# Add DELETED_AT column for tracking when deletion occurred
if 'DELETED_AT' not in contacts_columns:
    print("\nAdding DELETED_AT column to CONTACTS table...")
    cursor.execute("""
        ALTER TABLE CONTACTS 
        ADD COLUMN DELETED_AT TIMESTAMP_NTZ
    """)
    print("✅ DELETED_AT column added to CONTACTS")
else:
    print("✓ DELETED_AT column already exists in CONTACTS")

if 'DELETED_AT' not in companies_columns:
    print("Adding DELETED_AT column to COMPANIES table...")
    cursor.execute("""
        ALTER TABLE COMPANIES 
        ADD COLUMN DELETED_AT TIMESTAMP_NTZ
    """)
    print("✅ DELETED_AT column added to COMPANIES")
else:
    print("✓ DELETED_AT column already exists in COMPANIES")

conn.commit()

# Verify columns were added
print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

cursor.execute("DESCRIBE TABLE CONTACTS")
contacts_cols = cursor.fetchall()
print("\nCONTACTS table columns:")
for col in contacts_cols:
    if col[0] in ['IS_DELETED', 'DELETED_AT']:
        print(f"  ✅ {col[0]}: {col[1]}")

cursor.execute("DESCRIBE TABLE COMPANIES")
companies_cols = cursor.fetchall()
print("\nCOMPANIES table columns:")
for col in companies_cols:
    if col[0] in ['IS_DELETED', 'DELETED_AT']:
        print(f"  ✅ {col[0]}: {col[1]}")

cursor.close()
conn.close()

print("\n" + "=" * 70)
print("✅ SOFT DELETE COLUMNS ADDED SUCCESSFULLY")
print("=" * 70)
