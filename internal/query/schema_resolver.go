package query

import (
	"database/sql"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// flavorPostgres is the stream_state.flavor a PostgreSQL capture stamps.
const flavorPostgres = "postgres"

// LatestSchemaResolver loads the whole-schema view of an index's newest
// schema snapshots, for read surfaces (the console, recover, query's
// --pk-min/--pk-max, the MCP tools, verify, reconstruct).
//
// A MySQL or MariaDB index writes the whole schema under one snapshot_id, so
// the newest snapshot is that view: metadata.NewResolver(db, 0), unchanged. A
// PostgreSQL index writes ONE table per snapshot_id (metadata.WritePGSnapshot),
// so the newest snapshot describes only the last table that saw a change, and
// the view is each table's own newest snapshot:
// metadata.NewLatestPerTableResolver (#3110).
//
// The flavor comes from stream_state (SourceFlavor). When it is not known (no
// stream_state row on a file-indexed index, or a read that failed) the MySQL
// path is taken, as before this fork existed.
func LatestSchemaResolver(db *sql.DB) (*metadata.Resolver, error) {
	return LatestSchemaResolverForFlavor(db, SourceFlavor(db))
}

// LatestSchemaResolverForFlavor is LatestSchemaResolver for a caller that has
// already read the index's flavor.
func LatestSchemaResolverForFlavor(db *sql.DB, flavor string) (*metadata.Resolver, error) {
	if flavor == flavorPostgres {
		return metadata.NewLatestPerTableResolver(db)
	}
	return metadata.NewResolver(db, 0)
}
