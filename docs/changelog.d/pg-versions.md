- **PostgreSQL sources: 15, 16, 17 and 18 are the tested versions**. The
  integration suite now runs against PostgreSQL 18 in CI and in the release
  workflow, and PostgreSQL 14, which reached end of life in November 2026,
  leaves the matrix. 14 may still work, as 13 does, but is best-effort only.
  See [requirements](postgres.md#requirements-at-a-glance).
