### Added

- PostgreSQL port, through a forwarding account: `floor(a / b)` of numerics and whole numbers is answered on the copy as PostgreSQL takes it (the whole number below the exact quotient, `95` and not `95.0`; a quotient so close to a whole number that PostgreSQL's own rounding of the division could reach it is refused and goes to the source), and so is `ntile(n)`. Checked against PostgreSQL by `internal/pgcompare`. Eleven of the fourteen reports of a TPC-C analytics workload now stay on the copy.
