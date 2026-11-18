### Added

- **The option file ships with every install** (#2671). The `.deb` and `.rpm` packages install `/etc/dbtrail/dbtrail.cnf` with every option explained and commented out, readable by the `bintrail` group only, and never replace it. With Docker, put the file in `dbtrail-config/dbtrail.cnf` beside `docker-compose.yml`: the compose file (version 11) mounts that folder in every DBTrail service, and the installer leaves the commented copy there. [Every option](docs/option-reference.md) is also on a page of its own, generated from the same list.
