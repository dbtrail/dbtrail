### Added

- PostgreSQL port: the monitor login saved for the MySQL port also opens the PostgreSQL port, for a proxy's health checks (ProxySQL's `pgsql-monitor_username`/`pgsql-monitor_password`). A connection opened with it opens no server, whatever database it names, and answers only what ProxySQL's PostgreSQL monitor sends: an empty query, `SET`, and `SELECT pg_is_in_recovery()`. Every other statement is refused. Before, every one of those checks was refused with a wrong password, and ProxySQL counted the port as down.
- PostgreSQL port: `SELECT pg_is_in_recovery()` answers `true` on every connection, since the port is read-only, as a standby is.
