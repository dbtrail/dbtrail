### Changed

- The installer gives each SQL statement on the copy a memory ceiling of its own when this Docker accepts it (Docker Engine 28 or later): a statement that outgrows its memory is then stopped alone, instead of the system stopping a whole router and every client connected to it. It is tried with DBTrail's own image before the stack starts, and left off, with the reason said, where Docker refuses it. Only a freshly downloaded compose file is changed.
- The PostgreSQL router takes a share of the machine's memory like the other services (`PG_ROUTER_MEMORY_SHARE` in `.env`), and has the same optional ceiling per statement. Compose file version 12.
- A process in a container that sizes its memory from the whole machine now says, at startup, that it shares the machine with the other services, and how to give each its part.
