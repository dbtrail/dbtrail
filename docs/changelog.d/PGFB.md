### Fixed

- **`bintrail-pg flashback` reads `BINTRAIL_PG_FLASHBACK_LISTEN` and `BINTRAIL_PG_SHIM_CONFIG`.** Both were read only when their flag was empty, and both flags have a default (`127.0.0.1:5433`, `shim.yaml`), so neither variable ever applied. Now a variable applies unless its flag is given on the command line.
