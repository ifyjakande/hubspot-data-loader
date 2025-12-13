# HubSpot → Snowflake: One-time 200k Backfill Notes

This repo’s current `sync_to_snowflake.py` design can *likely* sync ~200k records into Snowflake, but the **risk is mainly runtime and rate-limits**, not correctness. Snowflake itself can handle 200k-row upserts; the bottlenecks are the Python/Snowflake insert method and HubSpot API paging work done on every run.

## What should handle 200k fine

- **Snowflake compute**: A `MERGE` of ~200k rows is generally OK on a reasonably sized warehouse.
- **Runner memory**: Holding ~200k HubSpot records (and ID sets) in memory is typically manageable on GitHub Actions runners.

## What is likely to cause issues at 200k with the current code

### 1) Row-by-row inserts into Snowflake

In `sync_to_snowflake.py`, records are inserted into staging tables using `cursor.execute(...)` inside a Python loop.

- At ~200k rows, this becomes **~200k separate round-trips** to Snowflake before the final `MERGE`.
- This is commonly the biggest cause of long runtimes and can lead to job timeouts and/or higher warehouse cost.

### 2) “Incremental” runs still perform full HubSpot scans for IDs and counts

Even if only a small number of records changed, the sync currently does heavy full-scan operations:

- **Soft delete detection**: `get_all_hubspot_ids()` pages through *all* objects to build an ID set.
- **Validation counts**: `get_hubspot_total_count()` paginates through *all* objects every run.

At ~200k records, this can mean thousands of HubSpot API calls per run, increasing:

- runtime
- likelihood of HubSpot rate limiting
- overall operational fragility

### 3) HubSpot Search API 10,000 result cap for incremental fetches

For incremental loads (`/search`), the code warns about the Search API’s **10,000 results limit**.

- If >10k records are modified between syncs, incremental fetch may be incomplete.
- The system relies on reconciliation (self-healing) to catch missing records later.

This is usually acceptable if daily deltas are small, but risky if deltas can exceed 10k.

## Recommendation for a one-time 200k backfill

1. Run the backfill as a **manual** job (workflow_dispatch), not on a tight schedule.
2. Prefer to **chunk** the backfill (e.g., 20k–50k per chunk) to reduce blast radius and make retries cheaper.
3. After the backfill, turn on incremental sync, but consider reducing the frequency of full reconciliation/soft-delete work.

## Minimal changes to make 200k+ backfills much safer

### A) Stop inserting one row at a time

Use one of these patterns (in order of typical effectiveness):

- `write_pandas` (Snowflake connector helper) to load a DataFrame into a staging table, then `MERGE`
- `executemany` for batch inserts (still not as fast as bulk load, but far better than per-row)
- Stage to a file and use `COPY INTO` (best for very large loads)

### B) Reduce “full scan” checks during steady-state incremental sync

- Run reconciliation/soft-delete logic less frequently (or in a separate workflow), especially once counts are stable.
- Keep the “self-healing” design, but avoid doing the most expensive HubSpot scans on every incremental run.

### C) Plan around the Search API cap

If it’s possible to have >10k changes between runs:

- shorten the interval between incremental syncs, or
- use alternative pagination strategies that don’t rely on `/search` returning >10k, and/or
- ensure reconciliation runs often enough to close gaps.

## Practical go/no-go summary

- **One-time 200k backfill**: likely OK, but **expect slow runtime** with the current per-row Snowflake inserts.
- **Ongoing incremental**: will work, but today it still performs expensive full scans each run; this may become slow and rate-limit prone as data grows.
