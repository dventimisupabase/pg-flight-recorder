# Observer Effect Benchmarks

Additional benchmarks measuring flight_recorder overhead: TPS impact, storage growth, and bloat.

## Quick Start

```bash
cd benchmark2

# Prerequisites: pg_cron, pgstattuple, flight_recorder installed
./setup.sh  # if pgbench tables not initialized

# Quick validation
ITERATIONS=1 TEST_DURATION=60 ./measure_observer_effect.sh
DURATION_HOURS=0.05 ./measure_storage.sh
DURATION_HOURS=0.05 ./measure_bloat.sh
```

## Files

- `SPEC.md` - Methodology and success criteria
- `RESULT_*.md` - Benchmark results
- `measure_observer_effect.sh` - TPS/latency A-B comparison
- `measure_storage.sh` - Storage growth tracking
- `measure_bloat.sh` - HOT update ratio monitoring
