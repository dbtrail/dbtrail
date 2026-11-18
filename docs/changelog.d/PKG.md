### Added

- **The option file ships with every install** (#2671). The `bintrail-console` package (`.deb` and `.rpm`) installs `/etc/dbtrail/dbtrail.cnf` with every option explained and commented out, and never replaces it. With Docker, put the file in `dbtrail-config/dbtrail.cnf` beside `docker-compose.yml`: the compose file (version 11) mounts that folder in every DBTrail service, and the installer leaves the commented copy there. A file mounted by hand before goes in that folder now. [Every option](docs/option-reference.md) is also on a page of its own, generated from the same list.
