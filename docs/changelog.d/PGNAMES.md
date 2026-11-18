### Documentation

- **PostgreSQL: names the copy reads as its own words** (docs/postgres.md). A table or column named `at`, `by`, `pivot`, `qualify`, `show` and the like is refused by SQL on the copy when written without quotes; written in quotes, with its schema or with its table, it answers as PostgreSQL does. Measured: 73 of 169 statements refused, none answered differently.
