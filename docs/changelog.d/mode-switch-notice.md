### Fixed

- A connection on the MySQL or PostgreSQL port that had been answered with the newest changes (fresh mode) now gets a warning (a NOTICE on PostgreSQL) when a later answer comes from an older snapshot alone, for example after the server is changed to fast mode while the connection is open. Before, the answer went back in time on that connection with nothing said. The warning is sent once; the following answers from the same snapshot say nothing more.
