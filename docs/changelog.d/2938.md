### Changed

- **The PostgreSQL port (`pg-router`) runs 4 statements at once and takes 256 clients unless told otherwise**, where it ran 2 and took 16. With 16, a dashboard of 50 clients had connections refused; with 2 statements at once, a table of 10 million rows answered about 4 statements a second. Threads per statement stay 2. Each statement may take the SQL memory (2GB unless set), so four at once can take 8GB: on a 16GB machine set `PG_ROUTER_WORKERS=2` (`BINTRAIL_PG_ROUTER_WORKERS`, `--workers`) or give the service a memory ceiling. A setting you made is kept.

### Fixed

- After `bintrail-console snapshot run` of a PostgreSQL server, the last line names the PostgreSQL port, not the MySQL port; so does the error for a PostgreSQL server with no source database in `port mode`.
