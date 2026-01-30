# Project Guidelines

## Code Search

The `./tools/gg` script wraps GNU Global for code navigation:

| Command | Description |
|---------|-------------|
| `gg def <sym>` | Find symbol definitions (tables, columns, functions) |
| `gg ref <sym>` | Find references (limited for SQL - often returns empty) |
| `gg path <pattern>` | Find file paths matching pattern (e.g., `\.sql$`) |
| `gg grep <pattern>` | List files containing pattern in content |
| `gg ctx <file>:<line>` | Show ±20 lines of context around a location |

### Examples

```bash
./tools/gg def snapshots
# snapshots          39 install.sql      CREATE TABLE IF NOT EXISTS flight_recorder.snapshots (

./tools/gg ctx install.sql:92
# Shows lines 72-112 with line numbers

./tools/gg grep snapshots
# Lists all files containing "snapshots"

./tools/gg path '\.sql$'
# Lists all SQL files in the project
```

### Limitations

- `gg ref` relies on GNU Global's reference tracking, which has poor support for SQL. Use `gg grep` as a fallback to find text occurrences.
- `gg path` searches file paths, not file contents.
- To list available symbols: `global -c | head -20`

### Multiple Definitions

If multiple definitions exist (common with PostgreSQL function overloading), show all candidates and use `ctx` to disambiguate.

### CI/CD Integration

A GitHub Actions workflow (`.github/workflows/gtags.yml`) regenerates the GTAGS database and HTML on every push to main:

- **HTML Code Browser**: Deployed to GitHub Pages (see repo Settings > Pages for URL)
- **GTAGS Artifact**: Downloadable from the workflow run for local use

To generate locally:

```bash
./tools/setup-gtags
```

This checks for dependencies (`global`, `universal-ctags`) and generates the GTAGS database.

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
