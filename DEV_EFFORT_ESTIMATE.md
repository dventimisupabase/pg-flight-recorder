# Development Effort Estimate

Rough estimate of the effort required to create pg-flight-recorder from scratch.

## Codebase Size (as of January 2026)

| File                     | Lines       | Description                    |
|--------------------------|-------------|--------------------------------|
| install.sql              | ~6,900      | Core implementation (PL/pgSQL) |
| flight_recorder_test.sql | ~4,100      | 451 pgTAP tests                |
| REFERENCE.md             | ~900        | Comprehensive documentation    |
| README.md                | ~200        | Quick start guide              |
| uninstall.sql            | ~150        | Clean removal                  |
| **Total**                | **~12,000** |                                |

## Complexity Factors

This isn't simple CRUD code. The implementation includes:

- **Three-tier architecture**: Ring buffers (UNLOGGED), aggregates, durable snapshots
- **Version-specific branching**: Separate code paths for PostgreSQL 15, 16, and 17
- **Safety mechanisms**: Circuit breakers, load shedding, job deduplication, DDL lock detection
- **Adaptive modes**: Normal, light, and emergency modes with automatic switching
- **Configuration profiles**: 6 pre-built profiles for different use cases
- **Capacity planning**: Statistical analysis across 6 resource dimensions
- **Comprehensive testing**: 451 pgTAP tests covering edge cases and safety mechanisms

## Effort Estimates

**Assumptions:**

- Expert PostgreSQL developer (deep PL/pgSQL, internals, pg_cron knowledge)
- Production-quality code with tests and documentation
- Complex systems code productivity: ~50-100 lines/day

**Estimates:**

| Scenario                           | Duration    |
|------------------------------------|-------------|
| Pure coding (ideal conditions)     | 3-4 months  |
| With design and research           | 6-9 months  |
| Including iteration and refinement | 9-12 months |

**Note:** These estimates assume familiarity with PostgreSQL performance monitoring concepts. For someone learning the domain simultaneously, double the estimates.

## What This Doesn't Include

- Initial research and requirements gathering
- Blind alleys and abandoned approaches
- Production hardening from real-world feedback
- Community feedback and feature requests

---

*Generated January 2026 with assistance from Claude*
