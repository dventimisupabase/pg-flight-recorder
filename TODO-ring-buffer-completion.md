# Ring Buffer Integration - Remaining Work

## ✅ Completed

1. **Ring Buffer Tables (TIER 1)** - UNLOGGED, 120 slots, fixed memory
   - samples_ring
   - wait_samples_ring
   - activity_samples_ring
   - lock_samples_ring

2. **Aggregate Tables (TIER 2)** - REGULAR (durable), crash-resistant
   - wait_event_aggregates
   - lock_aggregates
   - query_aggregates

3. **sample() Function** - Completely rewritten for ring buffers
   - Keeps all safety features (circuit breaker, job deduplication, adaptive sampling)
   - Writes to ring buffers using modular arithmetic
   - Removed progress tracking (not essential for crash diagnosis)

4. **flush_ring_to_aggregates() Function** - Flushes TIER 1 → TIER 2
   - Aggregates wait events, locks, queries
   - Runs every 5 minutes

5. **Views Updated** - All use ring buffers now
   - recent_waits → uses samples_ring
   - recent_activity → uses samples_ring
   - recent_locks → uses samples_ring
   - recent_progress → REMOVED (not in ring buffer)
   - recent_replication → unchanged (doesn't use samples)

## ⚠️ TODO (Critical for working system)

### 1. Update pg_cron Jobs

**File:** `install.sql` around lines 3036, 3389, 3414, 3418, 3423

**Changes needed:**

```sql
-- Change sample job to 60 seconds (not configurable interval)
PERFORM cron.schedule('flight_recorder_sample', '60 seconds',
    'SELECT flight_recorder.sample()');

-- Add NEW flush job (every 5 minutes)
PERFORM cron.schedule('flight_recorder_flush', '*/5 * * * *',
    'SELECT flight_recorder.flush_ring_to_aggregates()');

-- REMOVE partition job (not needed for ring buffers)
-- DELETE: cron.schedule('flight_recorder_partition', ...)

-- Update cleanup job to clean aggregates, not partitions
PERFORM cron.schedule('flight_recorder_cleanup', '0 3 * * *',
    'SELECT flight_recorder.cleanup_aggregates()');  -- Need to create this function
```

### 2. Create cleanup_aggregates() Function

Ring buffers self-clean, but aggregates need periodic cleanup:

```sql
CREATE OR REPLACE FUNCTION flight_recorder.cleanup_aggregates()
RETURNS VOID AS $$
BEGIN
    -- Delete aggregates older than retention period
    DELETE FROM flight_recorder.wait_event_aggregates
    WHERE start_time < now() - (SELECT value FROM flight_recorder.config WHERE key = 'aggregate_retention_days')::interval;

    DELETE FROM flight_recorder.lock_aggregates
    WHERE start_time < now() - (SELECT value FROM flight_recorder.config WHERE key = 'aggregate_retention_days')::interval;

    DELETE FROM flight_recorder.query_aggregates
    WHERE start_time < now() - (SELECT value FROM flight_recorder.config WHERE key = 'aggregate_retention_days')::interval;
END;
$$ LANGUAGE plpgsql;
```

### 3. Remove Partition Management

**Functions to remove or stub out:**
- `create_partitions()` - Not needed for ring buffers
- All partition creation logic

**Config to add:**
- `aggregate_retention_days` (default: 7)

### 4. Update uninstall.sql

Add new tables to DROP:

```sql
DROP TABLE IF EXISTS flight_recorder.samples_ring CASCADE;
DROP TABLE IF EXISTS flight_recorder.wait_samples_ring CASCADE;
DROP TABLE IF EXISTS flight_recorder.activity_samples_ring CASCADE;
DROP TABLE IF EXISTS flight_recorder.lock_samples_ring CASCADE;
DROP TABLE IF EXISTS flight_recorder.wait_event_aggregates CASCADE;
DROP TABLE IF EXISTS flight_recorder.lock_aggregates CASCADE;
DROP TABLE IF EXISTS flight_recorder.query_aggregates CASCADE;
```

Remove old:
```sql
-- DELETE these (no longer exist):
DROP TABLE IF EXISTS flight_recorder.samples CASCADE;
DROP TABLE IF EXISTS flight_recorder.wait_samples CASCADE;
DROP TABLE IF EXISTS flight_recorder.activity_samples CASCADE;
DROP TABLE IF EXISTS flight_recorder.progress_samples CASCADE;
DROP TABLE IF EXISTS flight_recorder.lock_samples CASCADE;
```

### 5. Update "Done" Message

Update the completion message to mention ring buffers:

```
- Samples: Ring buffer (60s intervals, 2 hours retention, fixed 120KB memory)
- Aggregates: Flushed every 5 minutes (durable, crash-resistant)
```

### 6. Cleanup Files

```bash
rm install-tiered-storage-addon.sql  # Merged into main
rm install-backup-before-sample-replace.sql  # Temporary backup
```

---

## Testing Plan

After completing above:

1. **Install test:**
   ```bash
   psql -f install.sql
   ```

2. **Verify tables:**
   ```sql
   SELECT tablename FROM pg_tables WHERE schemaname = 'flight_recorder' ORDER BY tablename;
   -- Should see: samples_ring, wait_samples_ring, activity_samples_ring, lock_samples_ring
   -- Should see: wait_event_aggregates, lock_aggregates, query_aggregates
   -- Should NOT see: samples (partitioned), wait_samples, activity_samples, progress_samples, lock_samples
   ```

3. **Verify jobs:**
   ```sql
   SELECT jobname, schedule FROM cron.job WHERE jobname LIKE 'flight_recorder%';
   -- Should see: flight_recorder_sample (60 seconds)
   -- Should see: flight_recorder_flush (*/5 * * * *)
   -- Should see: flight_recorder_snapshot (*/5 * * * *)
   -- Should see: flight_recorder_cleanup (0 3 * * *)
   -- Should NOT see: flight_recorder_partition
   ```

4. **Test collection:**
   ```sql
   SELECT flight_recorder.sample();
   SELECT count(*) FROM flight_recorder.samples_ring;  -- Should be 1
   SELECT count(*) FROM flight_recorder.wait_samples_ring;  -- Should have data
   ```

5. **Test flush:**
   ```sql
   SELECT flight_recorder.flush_ring_to_aggregates();
   SELECT count(*) FROM flight_recorder.wait_event_aggregates;  -- Should have data
   ```

6. **Test views:**
   ```sql
   SELECT count(*) FROM flight_recorder.recent_waits;
   SELECT count(*) FROM flight_recorder.recent_activity;
   SELECT count(*) FROM flight_recorder.recent_locks;
   ```

---

## Current Status

**85% Complete**

Major architecture implemented, needs configuration updates and cleanup to be fully functional.
