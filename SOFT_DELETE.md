# Soft Delete Implementation

## 🎯 Objective
Handle record deletions from HubSpot without losing historical data in Snowflake.

## ✅ What Was Implemented

### 1. Database Schema Changes

Added two columns to both `CONTACTS` and `COMPANIES` tables:

```sql
IS_DELETED BOOLEAN DEFAULT FALSE
DELETED_AT TIMESTAMP_NTZ
```

**Purpose:**
- `IS_DELETED`: Flag indicating if record is deleted in HubSpot
- `DELETED_AT`: Timestamp of when deletion was detected

### 2. Soft Delete Logic in `sync_to_snowflake.py`

#### New Function: `get_all_hubspot_ids(object_type)`
- Fetches all current IDs from HubSpot
- Lightweight query (only fetches IDs, not full records)
- Used to detect deletions

#### Deletion Detection Process:
1. **Fetch current IDs** from HubSpot
2. **Get active IDs** from Snowflake (WHERE IS_DELETED = FALSE)
3. **Find deleted IDs** = Snowflake IDs - HubSpot IDs
4. **Mark as deleted** in Snowflake

```python
# Mark records deleted in HubSpot
UPDATE CONTACTS 
SET IS_DELETED = TRUE, DELETED_AT = CURRENT_TIMESTAMP()
WHERE HUBSPOT_ID NOT IN (current_hubspot_ids)
```

### 3. Updated Validation Logic

**Before:**
```sql
SELECT COUNT(*) FROM CONTACTS
```

**After:**
```sql
SELECT COUNT(*) FROM CONTACTS 
WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL
```

**Result:** Validation now compares HubSpot total count with **active** Snowflake records only.

## 📊 How It Works

### Scenario: Contact Deleted in HubSpot

```
1. Initial State:
   HubSpot:   [Contact #123, #124, #125]
   Snowflake: [Contact #123, #124, #125] (IS_DELETED=FALSE)
   
2. User deletes Contact #124 in HubSpot:
   HubSpot:   [Contact #123, #125]
   Snowflake: [Contact #123, #124, #125] (IS_DELETED=FALSE)
   
3. Next sync runs:
   - Fetches current HubSpot IDs: [123, 125]
   - Gets active Snowflake IDs: [123, 124, 125]
   - Detects deletion: 124 is in Snowflake but not HubSpot
   - Marks as deleted:
     UPDATE CONTACTS SET IS_DELETED=TRUE, DELETED_AT='2025-12-11 14:48:00' WHERE HUBSPOT_ID='124'
   
4. Final State:
   HubSpot:   [Contact #123, #125] (2 records)
   Snowflake: [Contact #123 (active), #124 (deleted), #125 (active)]
   Active count: 2 ✅ Matches HubSpot
   Total count: 3 (includes historical)
```

## 🔍 Querying Data

### Get Active Records Only
```sql
SELECT * FROM CONTACTS 
WHERE IS_DELETED = FALSE OR IS_DELETED IS NULL;
```

### Get Deleted Records
```sql
SELECT * FROM CONTACTS 
WHERE IS_DELETED = TRUE;
```

### Get All Records (Including Deleted)
```sql
SELECT *, 
       CASE WHEN IS_DELETED THEN '❌ Deleted' ELSE '✅ Active' END AS STATUS
FROM CONTACTS;
```

### Audit Deletions
```sql
SELECT HUBSPOT_ID, EMAIL, FIRSTNAME, LASTNAME, DELETED_AT
FROM CONTACTS 
WHERE IS_DELETED = TRUE
ORDER BY DELETED_AT DESC;
```

## ✅ Benefits

### 1. **Data Preservation**
- No data loss
- Complete historical record
- Can track when deletions occurred

### 2. **Accurate Validation**
- HubSpot count always matches active Snowflake count
- Pipeline won't fail due to deletions
- Validation remains reliable

### 3. **Analytics Friendly**
```sql
-- Current state view
CREATE VIEW ACTIVE_CONTACTS AS
SELECT * FROM CONTACTS WHERE IS_DELETED = FALSE;

-- Historical analysis
SELECT 
    DATE_TRUNC('month', DELETED_AT) AS month,
    COUNT(*) AS contacts_deleted
FROM CONTACTS
WHERE IS_DELETED = TRUE
GROUP BY month;
```

### 4. **Audit Trail**
- Track who was deleted and when
- Useful for compliance and reporting
- Can restore if deletion was accidental (in HubSpot)

## 📈 Sync Output Example

```
Checking for deleted contacts...
Current HubSpot IDs: 885
Active Snowflake IDs: 890
Found 5 deleted contacts, marking as deleted...
✓ Marked 5 contacts as deleted

Validating data...
HubSpot total count: 885
Snowflake total count: 885 (active records only)
Status: ✅ COUNTS MATCH
```

## 🔧 One-Time Setup

The soft delete columns were added using `add_soft_delete_columns.py`:

```bash
python3 add_soft_delete_columns.py
```

**This script:**
- Checks if columns already exist
- Adds `IS_DELETED` and `DELETED_AT` columns if needed
- Defaults all existing records to `IS_DELETED = FALSE`
- Safe to run multiple times (idempotent)

## 🚀 Future Enhancements

### Option 1: Archive Old Deletions
Move very old deleted records to archive table:

```sql
-- Archive deletions older than 1 year
INSERT INTO CONTACTS_ARCHIVE 
SELECT * FROM CONTACTS 
WHERE IS_DELETED = TRUE 
  AND DELETED_AT < DATEADD(year, -1, CURRENT_TIMESTAMP());

DELETE FROM CONTACTS 
WHERE IS_DELETED = TRUE 
  AND DELETED_AT < DATEADD(year, -1, CURRENT_TIMESTAMP());
```

### Option 2: Restore Capability
If record is recreated in HubSpot with same ID:

```sql
-- Automatically restore if ID reappears
UPDATE CONTACTS 
SET IS_DELETED = FALSE, DELETED_AT = NULL
WHERE HUBSPOT_ID = '123' AND IS_DELETED = TRUE;
```

## 📝 Files Modified

1. **sync_to_snowflake.py** - Added soft delete logic
2. **check_changes.py** - Updated to handle soft deletes
3. **add_soft_delete_columns.py** - Schema migration script

---

**Implementation Date:** 2025-12-11  
**Status:** ✅ Active  
**Approach:** Soft Delete (mark as deleted, preserve data)
