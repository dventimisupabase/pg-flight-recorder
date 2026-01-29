# pg-flight-recorder Migrations

This directory contains the upgrade infrastructure for pg-flight-recorder.

## Upgrade Workflow

### Check Current Version

```sql
SELECT value FROM flight_recorder.config WHERE key = 'schema_version';
```

### Before Upgrading

Review what data you have:

```sql
SELECT * FROM flight_recorder.export_for_upgrade();
```

### Run Upgrade

```bash
psql -f migrations/upgrade.sql
```

The upgrade script automatically detects your current version and applies any needed migrations.

### Reinstall Functions (after schema migrations)

If a migration only changes schema (tables/columns), you may need to reinstall functions:

```bash
psql -f install.sql
```

This is safe - it preserves all data and just updates functions/views.

## Files

| File | Purpose |
|------|---------|
| `upgrade.sql` | Main upgrade runner - detects version and runs migrations |
| `TEMPLATE.sql` | Template for creating new migration scripts |
| `X.Y_to_X.Z.sql` | Individual migration scripts |

See REFERENCE.md for version history.

## Creating New Migrations

1. Copy `TEMPLATE.sql` to `X.Y_to_X.Z.sql`
2. Update the version guards
3. Add your schema changes (preserving data!)
4. Add the migration to `upgrade.sql`
5. Update `schema_version` in `install.sql` for fresh installs

## Data Preservation Rules

- **Never DROP tables** containing customer data
- Use `ADD COLUMN IF NOT EXISTS` for new columns
- When renaming tables, create a view with the old name
- UNLOGGED ring buffer tables can be recreated (transient data)
- All LOGGED tables (snapshots, archives, aggregates) must be preserved

## Safe vs Full Uninstall

| Script | Data | Use Case |
|--------|------|----------|
| `uninstall.sql` | Preserved | Upgrading, reinstalling |
| `uninstall_full.sql` | Deleted | Complete removal |
