# Project Guidelines

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
