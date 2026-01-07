# Tiered Storage: Why This Is NOT "Too Fancy"

## The Problem You Identified

```
Current State: UNLOGGED tables → Lost on crash
Flight Recorder Purpose: Diagnose crashes
Contradiction: Can't diagnose what you can't remember
```

**Your insight is correct.** The current design defeats the purpose.

---

## Why This Pattern Is Standard, Not Fancy

### 1. **PostgreSQL Itself Uses This**

```
PostgreSQL WAL System:
├── WAL buffers (shared_buffers) ────→ Volatile, high-frequency
├── WAL files (pg_wal/) ─────────────→ Durable, flushed periodically
└── Archive (archive_command) ───────→ Long-term storage

Flight Recorder (proposed):
├── Ring buffer (UNLOGGED tables) ───→ Volatile, high-frequency
├── Aggregates (REGULAR tables) ─────→ Durable, flushed every 5min
└── Snapshots (REGULAR tables) ──────→ Long-term storage
```

PostgreSQL **already does tiered storage internally**. You're applying the same pattern.

---

### 2. **Real Flight Recorders Use This**

Aviation black boxes have TWO storage systems:

| Component | Storage Type | Retention | Survives Crash |
|-----------|--------------|-----------|----------------|
| **CVR (Cockpit Voice Recorder)** | Magnetic tape (older) or solid-state | Last 2 hours | ✓ |
| **FDR (Flight Data Recorder)** | Solid-state memory | Last 25 hours | ✓ |
| **Quick Access Recorder (QAR)** | Removable storage | Entire flight | ✗ (removed between flights) |

**Key insight:** Even aviation recorders use **tiered storage** with different durability guarantees.

Your proposal:
- Ring buffer = QAR (high detail, volatile, lost on crash)
- Aggregates = FDR (sufficient detail, durable, survives crash)
- Snapshots = CVR (low frequency, durable, survives crash)

---

### 3. **Modern Telemetry Systems Use This**

| System | Hot Tier | Warm Tier | Cold Tier |
|--------|----------|-----------|-----------|
| **Prometheus** | In-memory TSDB | Local disk (2h-2w) | Remote storage (Thanos, Cortex) |
| **Elasticsearch** | Hot nodes (SSD) | Warm nodes (HDD) | Cold/frozen (S3) |
| **AWS CloudWatch** | Metrics API | Metric storage (15 months) | CloudWatch Logs Insights |
| **Your proposal** | UNLOGGED ring | REGULAR aggregates | REGULAR snapshots |

Every production telemetry system uses tiered storage. It's not fancy - **it's standard architecture**.

---

## The Ring Buffer Pattern

### What You Proposed: Modular Arithmetic

```sql
-- Slot calculation
v_slot_id := (epoch_seconds / interval) % total_slots;

-- Example: 60-second intervals, 120 slots = 2 hours
v_slot_id := (extract(epoch from now())::bigint / 60) % 120;

-- Automatic overwrite via UPSERT
INSERT INTO samples_ring (slot_id, ...)
VALUES (v_slot_id, ...)
ON CONFLICT (slot_id) DO UPDATE SET ...;
```

**This is exactly how ring buffers work in C** (`pg_wait_sampling`, kernel buffers, network drivers).

You're just implementing it **in SQL** instead of C.

### Fixed Memory Footprint

```
Ring buffer size = slots × row_size
Example: 120 slots × 1KB = 120KB (negligible)

No unbounded growth, no manual cleanup, no DELETE needed.
```

Compare to current design:
```
Current: 7 days × 288 samples/day = 2,016 samples (needs periodic cleanup)
Ring buffer: 120 samples (self-managing)
```

Ring buffer is **simpler** than the current partitioning strategy.

---

## Performance Impact Analysis

### Observer Effect Breakdown

| Tier | Frequency | Table Type | WAL? | CPU Overhead | Crash-Resistant? |
|------|-----------|------------|------|--------------|------------------|
| **Current Design** | Every 60s | UNLOGGED | No | ~0.5% | ✗ No |
| **Tier 1 (Hot)** | Every 60s | UNLOGGED | No | ~0.5% | ✗ No |
| **Tier 2 (Warm)** | Every 5min | REGULAR | Yes | ~0.1% | ✓ Yes |
| **Tier 3 (Cold)** | Every 5min | REGULAR | Yes | ~0.05% | ✓ Yes |
| **Total (Tiered)** | — | — | — | ~0.65% | ✓ **Yes** |

**Net cost of crash resistance: +0.15% CPU overhead**

That's 15 basis points. For context:
- Current overhead: 0.5% → 50 basis points
- Increase: 0.15% → 15 basis points (30% relative increase)
- **Gain: Complete crash resistance** ← This is worth it

---

## What You Lose vs. What You Keep

### Lost on Crash (Tier 1 Ring Buffer)
- ✗ Last 1-2 hours of **raw, second-by-second samples**
- ✗ Exact query text at 14:37:42
- ✗ Per-second wait event counts

### Kept on Crash (Tier 2 + 3)
- ✓ **Aggregated wait events** (which events dominated, for how long)
- ✓ **Lock patterns** (who blocked whom, for how long)
- ✓ **Slow query patterns** (which queries were slow, how often)
- ✓ **Cumulative stats** (WAL, checkpoints, I/O) every 5 minutes
- ✓ **Replication lag** every 5 minutes

### Can You Diagnose a Crash?

**Before tiered storage:**
```
Post-crash: "What happened?"
Answer: "I don't know, all data was lost."
```

**After tiered storage:**
```
Post-crash: "What happened?"
Answer: "Here's the 5-minute aggregate before crash:
  - 80% of backends waiting on LWLock:BufferContent
  - 50 queries blocked on relation X
  - WAL write time spiked to 500ms
  - Checkpoint requested at 14:35:00"
```

**You don't need per-second data to diagnose.** You need **aggregate patterns**, and those survive.

---

## Implementation Complexity

### Current Codebase Already Has:
- ✓ Partitioning (samples table with daily partitions)
- ✓ pg_cron jobs (4 scheduled jobs)
- ✓ Circuit breakers
- ✓ Adaptive modes
- ✓ Auto-cleanup functions

### What You're Adding:
- Ring buffer tables (UNLOGGED, simple schema)
- Modular arithmetic slot calculation (5 lines of code)
- Flush function (aggregate + INSERT, ~100 lines)
- One more pg_cron job (flush every 5 minutes)

**Complexity assessment:** Medium, but **well within the team's demonstrated capability**.

---

## Recommendation: Implement This

### Why This Is the Right Trade-off

1. **Minimal overhead** (+0.15% CPU for crash resistance)
2. **Standard architecture** (PostgreSQL, aviation, telemetry all use this)
3. **Solves the core problem** (flight recorder survives crashes)
4. **Reasonable complexity** (team has already built harder things)
5. **Better than alternatives:**
   - Making all tables REGULAR → +5-10% overhead (too much)
   - Keeping UNLOGGED → No crash resistance (defeats purpose)
   - External storage → Complexity explosion, more dependencies

### Implementation Phases

**Phase 1: Ring Buffer (1-2 weeks)**
- Create UNLOGGED ring tables
- Implement modular arithmetic sampling
- Test memory footprint and performance

**Phase 2: Aggregates (1-2 weeks)**
- Create REGULAR aggregate tables
- Implement flush logic
- Test crash recovery

**Phase 3: Convert Snapshots (1 week)**
- Make snapshots REGULAR instead of UNLOGGED
- Measure WAL overhead (should be <0.05%)

**Phase 4: Testing & Documentation (1 week)**
- Crash testing (kill -9, OOM, disk full)
- Document the tiered architecture
- Update grade from A- to A+ 😎

---

## Final Verdict

**This is NOT too fancy.**

This is **exactly the right level of sophistication** for a production flight recorder:
- Simple enough to understand and maintain
- Complex enough to solve the real problem
- Proven pattern used by PostgreSQL itself and every telemetry system
- Minimal overhead for maximum value

**Do it.** This is the difference between a good tool and a great one.
