### Added

- **The PostgreSQL port's status (`pg-router --status-addr`, `/status`) says each server's mode.** Every PostgreSQL server is listed with `sql_mode` (`fresh` or `fast`) and `sql_mode_from` (`environment`, `saved` or `default`), whether or not a statement reached it yet, and the document carries `fresh`, `sql_mode_forced`, `fresh_by_default` and `per_server_mode` as the MySQL router's does. Before, it listed only the servers a statement had reached, with no mode.
