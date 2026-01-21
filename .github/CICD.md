# CI/CD Documentation

This document describes the GitHub Actions workflows configured for pg-flight-recorder.

## Workflows

### 1. Test Suite (`test.yml`)

**Triggers:**

- Push to `main` branch
- Pull requests to `main` branch
- Manual dispatch via GitHub Actions UI

**What it does:**

- Runs the full pgTAP test suite (118 tests) against PostgreSQL 15, 16, and 17
- Tests run in parallel using a matrix strategy
- Uses the existing Docker Compose infrastructure
- Each PostgreSQL version gets its own job

**Matrix:**

```yaml
PostgreSQL 15: ✓ 118 tests
PostgreSQL 16: ✓ 118 tests
PostgreSQL 17: ✓ 118 tests

```

**Failure handling:**

- Shows PostgreSQL logs on failure
- Fails the entire workflow if any version fails
- Always cleans up Docker containers

### 2. Lint (`lint.yml`)

**Triggers:**

- Push to `main` branch
- Pull requests to `main` branch
- Manual dispatch via GitHub Actions UI

**What it does:**

**SQL Syntax Check:**

- Validates `install.sql` and `uninstall.sql`
- Checks for unterminated strings and comments
- Basic syntax error detection

**Markdown Lint:**

- Runs `markdownlint-cli2` on all `*.md` files
- Configuration in `.markdownlint.json`
- Enforces consistent documentation style

**Shell Script Lint:**

- Runs ShellCheck on all shell scripts
- Catches common bash scripting errors
- Severity level: warning

### 3. Release (`release.yml`)

**Triggers:**

- Push of version tags matching `v*.*.*` (e.g., `v1.0.0`)
- Manual dispatch with version input

**What it does:**

**Validation:**

- Validates version tag format (must be `v#.#.#`)
- Runs the full test suite across all PostgreSQL versions
- Fails if tests don't pass

**Release Creation:**

- Generates changelog from git commits since last tag
- Creates release assets:
  - `pg-flight-recorder-v#.#.#.tar.gz`
  - `pg-flight-recorder-v#.#.#.zip`
  - Individual SQL files
- Creates GitHub Release with:
  - Generated changelog
  - Downloadable assets
  - Installation instructions

**Announcement:**

- Prints release information
- Provides direct download links

## How to Use

### Running Tests Locally

The CI workflows use the same scripts you can run locally:

```bash
# Test on all versions in parallel (default)
./test.sh

# Test on specific version
./test.sh 15
./test.sh 16
./test.sh 17
```

### Creating a Release

**Option 1: Tag and Push**

```bash
# Create and push a version tag
git tag v1.0.0
git push origin v1.0.0

# GitHub Actions will automatically:
# 1. Run all tests
# 2. Create GitHub Release
# 3. Attach release assets

```

**Option 2: Manual Dispatch**

1. Go to Actions → Release → Run workflow
2. Enter version tag (e.g., `v1.0.1`)
3. Click "Run workflow"

### Checking CI Status

**For Pull Requests:**

- View the "Checks" tab on your PR
- All workflows must pass before merging
- Click on failed checks to see logs

**For Main Branch:**

- Go to Actions tab in GitHub repository
- View recent workflow runs
- Download logs or artifacts if needed

## Workflow Files

All workflows are located in `.github/workflows/`:

```
.github/
├── workflows/
│   ├── test.yml      # Main test suite (118 pgTAP tests × 3 versions)
│   ├── lint.yml      # SQL, Markdown, and Shell linting
│   └── release.yml   # Version tagging and release creation
└── CICD.md           # This file

```

## Configuration Files

**`.markdownlint.json`**

- Configures markdown linting rules
- Disables line-length checks (MD013)
- Allows inline HTML (MD033) and bare URLs (MD034)

## CI Environment

**Test Environment:**

- OS: Ubuntu Latest
- PostgreSQL: 15, 16, 17 (from official Docker images)
- Extensions: pg_cron, pgTAP
- Test Framework: pg_prove (TAP protocol)

**Resources:**

- Standard GitHub Actions runners
- Docker + Docker Compose available
- ~2-4 minutes per test run (all versions in parallel)

## Troubleshooting

### Tests Failing in CI but Passing Locally

1. Check PostgreSQL version compatibility
2. Verify Docker Compose is using the correct PG version
3. Check for timing-dependent tests
4. Review CI logs: Actions → Failed workflow → Job → Step

### Release Creation Failing

1. Verify version tag format: `v#.#.#` (e.g., `v1.0.0`)
2. Ensure all tests pass before creating release
3. Check that `GITHUB_TOKEN` has write permissions
4. Review changelog generation in workflow logs

### Markdown Linting Errors

1. Check `.markdownlint.json` configuration
2. Run locally: `npx markdownlint-cli2 "**/*.md"`
3. Common issues:
   - Inconsistent heading styles
   - Missing blank lines around lists
   - Trailing spaces

### Shell Linting Errors

1. Run locally: `shellcheck *.sh benchmark/*.sh`
2. Common issues:
   - Unquoted variables
   - Missing error handling
   - Deprecated syntax

## Maintenance

### Updating PostgreSQL Versions

When PostgreSQL 18 is released and Docker volumes support it:

1. Update `test.yml` matrix:

```yaml
matrix:
  pg_version: [15, 16, 17, 18]

```

1. Update `test.sh`:

```bash
if [ "$VERSION" = "all" ]; then
    for v in 15 16 17 18; do

```

1. Test locally first: `./test.sh 18`

### Adding New Workflows

1. Create `*.yml` file in `.github/workflows/`
2. Use existing workflows as templates
3. Test with `act` (local GitHub Actions runner) if possible
4. Document in this file

## Best Practices

**For Contributors:**

- Run tests locally before pushing: `./test.sh`
- Run linters before committing
- Keep commit messages descriptive (used in changelog)
- Don't push broken code to `main`

**For Maintainers:**

- Always create releases from clean `main` branch
- Test release process on a branch first
- Review automatically generated changelogs before publishing
- Use semantic versioning strictly

## Security

**Token Permissions:**

- `GITHUB_TOKEN` is auto-generated per workflow run
- `contents: write` permission only for release workflow
- No manual secrets required

**Docker Security:**

- Official PostgreSQL images only
- Extensions built from source (pg_cron, pgTAP)
- No external dependencies beyond official repositories

## Future Enhancements

Potential additions to CI/CD:

- [ ] Benchmark regression testing (measure absolute costs)
- [ ] Code coverage reporting for SQL functions
- [ ] Automated security scanning
- [ ] Performance testing on each PR
- [ ] Docker image publishing
- [ ] Homebrew formula auto-update
- [ ] Documentation deployment (if we add a docs site)
