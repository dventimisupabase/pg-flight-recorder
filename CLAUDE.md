# Project Guidelines

## Code Search (IMPORTANT)

**ALWAYS use `./tools/sql-find` instead of Grep/Glob for code search in this codebase.**

The `sql-find` script uses pglast (PostgreSQL's actual parser) to understand SQL symbols (tables, columns, functions, views) and provides accurate results. SQL_INDEX.json auto-generates on first use via Docker—no setup required.

### Preferred workflow

1. **Find definitions**: `./tools/sql-find def <symbol>` (NOT Grep)
2. **Find text occurrences**: `./tools/sql-find grep <pattern>` (NOT Grep)
3. **Find files by path**: `./tools/sql-find path <pattern>` (NOT Glob)
4. **View context**: `./tools/sql-find ctx <file>:<line>` (NOT Read with offset)

Only fall back to Grep/Glob if `sql-find` returns nothing or for patterns `sql-find` doesn't support.

### Command reference

| Command                        | Description                                           |
|--------------------------------|-------------------------------------------------------|
| `sql-find def <sym>`           | Find symbol definitions (tables, columns, functions)  |
| `sql-find grep <pattern>`      | Search file contents                                  |
| `sql-find path <pattern>`      | Find file paths matching pattern (e.g., `\.sql$`)     |
| `sql-find ctx <file>:<line>`   | Show ±20 lines of context around a location           |
| `sql-find stats`               | Show index statistics                                 |
| `sql-find regen`               | Regenerate SQL_INDEX.json                             |

### Examples

```bash
./tools/sql-find def snapshots
# flight_recorder.snapshots  39  install.sql  table

./tools/sql-find def captured_at
# flight_recorder.snapshots.captured_at  41  install.sql  column (timestamptz)

./tools/sql-find ctx install.sql:92
# Shows lines 72-112 with line numbers

./tools/sql-find grep snapshots
# Lists all files containing "snapshots"

./tools/sql-find stats
# Files indexed: 3
# Tables: 18
# Functions: 78
# Views: 9
```

### CI/CD Integration

A GitHub Actions workflow (`.github/workflows/sql-index.yml`) regenerates SQL_INDEX.json on every push to main when SQL files change:

- **sql-index Artifact**: Downloadable from the workflow run for local use
- **sql-index Branch**: SQL_INDEX.json is pushed to the `sql-index` branch

To generate locally:

```bash
./tools/setup-sql-index --docker
```

Or if you have pglast installed (`pip install pglast>=7.0`):

```bash
./tools/setup-sql-index
```

## Markdown Formatting

When writing or editing markdown files, follow these rules to pass linting:

- **Blank lines around blocks**: Always add a blank line before and after:
  - Lists (bulleted or numbered)
  - Headings
  - Fenced code blocks

- **List markers**: Use dashes (`-`) for unordered lists, not asterisks (`*`)

- **Indentation**: Use 2 spaces for nested list items

### Example

Wrong:

````markdown
**Some header text:**
- Item 1
- Item 2
#### Subheading
```code
example
```
````

Right:

````markdown
**Some header text:**

- Item 1
- Item 2

#### Subheading

```code
example
```
````

## Testing

Run tests with:

```bash
./test.sh
```

## Code Style

- Follow existing patterns in `install.sql`
- Use `flight_recorder.` schema prefix for all objects
- Include COMMENT ON statements for new functions and tables

## Schema Evolution

pg-flight-recorder uses **additive-only schema changes**:

- Add new nullable columns (never remove or rename existing ones)
- Create migration files (e.g., `migrations/2.2_to_2.3.sql`) for upgrades
- Historical data with NULL in new columns is correct ("not collected then")

**Why not JSONB + versioning?**

- Query performance matters during incident analysis
- Strong typing catches errors early
- Schema-as-documentation (`\d flight_recorder.snapshots` shows what's collected)
- Underlying pg_stat_* views evolve slowly and additively
- Migration burden is manageable with semantic versioning
