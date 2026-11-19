### Fixed

- PostgreSQL full read: the snapshot folder a read creates is now named to the job's journal as it is created, and removed when the read fails, as for a MySQL read (#1938, #2180). A scheduled read that failed (a full disk, a lost connection) used to leave its folder marked incomplete, which no listing shows and the retention never removes, one more at every slot; a daemon killed during the read now finds its folder at the next start and removes it too. A folder that already held files is left as it was.
