### Fixed

- PostgreSQL port: `SELECT pg_backend_pid()` and `SELECT current_setting('…')`, alone or together in one select list, are answered by the port, as `version()` already was. Tools built on psycopg send them on connect; they reached the copy, which has no `pg_backend_pid`, and the tool did not connect. `pg_backend_pid()` is the process number a cancel request names the connection by, and `current_setting` reads the session's own settings (an unknown one is refused with 42704, or NULL with `missing_ok`).
