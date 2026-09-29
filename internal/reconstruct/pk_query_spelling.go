package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ErrPKTypeUnknown marks a refused key lookup: the key looks like a MariaDB
// UUID/INET value, and the schema snapshot that says whether the column is one
// could not be read. Surfaces map it to a client error, not a server fault.
var ErrPKTypeUnknown = errors.New("primary key type unknown")

// SpellIndexPKFilter rewrites the key filter of a query or recover request
// (opts.PKValues, opts.PKValuesIn) into the spelling binlog_events.pk_values
// holds, for the one case where the typed spelling and the stored one differ
// by design: a MariaDB UUID/INET4/INET6 key. The index keys those rows by the
// value's bytes (spelled "0xCC5C…" when they are not valid UTF-8), while a
// person types the text the application shows ("cc5c4e6e-bc5a-…"). Looked up
// as typed, such a key matched nothing and the answer was "no history",
// without an error. reconstruct already spelled it this way (IndexPKSpelling).
//
// The stored spelling becomes opts.PKValues and the typed one moves to
// opts.PKValuesAlt, so the live index matches either. The order matters: the
// Parquet archive reader filters on PKValues alone. For PKValuesIn the stored
// spellings are added beside the typed ones, and the returned map (typed to
// stored) lets a caller that groups rows by the typed key find them.
//
// It reads the schema snapshot only when some typed component parses as one
// of these types (in any spelling metadata.ParseMariaDBFixedKey accepts), so a
// numeric or plain text key costs nothing. When one does and the snapshot
// cannot say what the column is, the lookup is refused with ErrPKTypeUnknown
// instead of answering "no history" on a guess. A table the request's profile
// denies, or leaves out of its allow list, is left alone, so the answer cannot
// describe its key.
//
// Call it after the request's RBAC posture is on opts, and after any #957
// escape alternates are set: it replaces PKValuesAlt when it re-spells.
func SpellIndexPKFilter(ctx context.Context, db *sql.DB, opts *query.Options) (map[string]string, error) {
	if opts.Schema == "" || opts.Table == "" || (opts.PKValues == "" && len(opts.PKValuesIn) == 0) {
		return nil, nil
	}
	for _, dt := range opts.DenyTables {
		if strings.EqualFold(dt.Schema, opts.Schema) && strings.EqualFold(dt.Table, opts.Table) {
			return nil, nil
		}
	}
	if len(opts.AllowTables) > 0 {
		// Allow-list mode: the engine matches these exactly (BINARY), so the
		// check here is exact too. A table outside the list returns nothing
		// anyway, and must not have its key described.
		allowed := false
		for _, at := range opts.AllowTables {
			if at.Schema == opts.Schema && at.Table == opts.Table {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, nil
		}
	}
	typed := opts.PKValuesIn
	if opts.PKValues != "" {
		typed = []string{opts.PKValues}
	}
	var sample string
	for _, v := range typed {
		if looksLikeMariaDBFixedKey(v) {
			sample = v
			break
		}
	}
	if sample == "" {
		return nil, nil
	}
	metas, err := latestPKMetas(ctx, db, opts.Schema, opts.Table)
	if err != nil {
		return nil, fmt.Errorf("%w: could not read the schema snapshot for %s.%s to tell whether its primary key is a MariaDB UUID/INET column, "+
			"which the index stores as bytes; looked up as typed, the key %q could match nothing and read as a row with no history: %v",
			ErrPKTypeUnknown, opts.Schema, opts.Table, sample, err)
	}
	if len(metas) == 0 {
		return nil, fmt.Errorf("%w: no schema snapshot describes the primary key of %s.%s, so there is no way to tell whether the key %q is a MariaDB UUID/INET value, "+
			"which the index stores as bytes; looked up as typed it could match nothing and read as a row with no history, so the lookup is refused. "+
			"Take a schema snapshot (`bintrail snapshot`), or select the rows by table and time window instead",
			ErrPKTypeUnknown, opts.Schema, opts.Table, sample)
	}
	return spellPKFilter(opts, metas), nil
}

// spellPKFilter is SpellIndexPKFilter once the key's columns are known.
func spellPKFilter(opts *query.Options, metas []metadata.ColumnMeta) map[string]string {
	fixed := false
	for _, c := range metas {
		if metadata.MariaDBFixedWidth(c.DataType) > 0 {
			fixed = true
			break
		}
	}
	if !fixed {
		return nil
	}
	if opts.PKValues != "" {
		if sp := IndexPKSpelling(opts.PKValues, metas); sp != opts.PKValues {
			opts.PKValuesAlt = opts.PKValues
			opts.PKValues = sp
		}
		return nil
	}
	var spelled map[string]string
	out := opts.PKValuesIn
	seen := make(map[string]bool, len(opts.PKValuesIn))
	for _, v := range opts.PKValuesIn {
		seen[v] = true
	}
	for _, v := range opts.PKValuesIn {
		sp := IndexPKSpelling(v, metas)
		if sp == v {
			continue
		}
		if spelled == nil {
			spelled = map[string]string{}
			out = append([]string(nil), opts.PKValuesIn...)
		}
		spelled[v] = sp
		if !seen[sp] {
			seen[sp] = true
			out = append(out, sp)
		}
	}
	opts.PKValuesIn = out
	return spelled
}

// looksLikeMariaDBFixedKey reports whether some "|"-separated component of a
// typed key parses as a UUID, INET4 or INET6 value.
func looksLikeMariaDBFixedKey(v string) bool {
	for _, part := range strings.Split(v, "|") {
		for _, dt := range []string{"uuid", "inet4", "inet6"} {
			if _, err := metadata.ParseMariaDBFixedKey(dt, part); err == nil {
				return true
			}
		}
	}
	return false
}

// latestPKMetas loads the primary-key columns of schema.table from the newest
// schema snapshot that describes the table. Not simply the newest snapshot: a
// table dropped since is absent from it, and its history is still in the
// index. It returns nil, nil when no snapshot describes the table. The names
// are matched the way the index matches them (the column collation), then
// resolved under the spelling the snapshot stored.
func latestPKMetas(ctx context.Context, db *sql.DB, schema, table string) ([]metadata.ColumnMeta, error) {
	var (
		id          int
		sName, tNam string
	)
	err := db.QueryRowContext(ctx,
		"SELECT snapshot_id, schema_name, table_name FROM schema_snapshots "+
			"WHERE schema_name = ? AND table_name = ? ORDER BY snapshot_id DESC LIMIT 1",
		schema, table).Scan(&id, &sName, &tNam)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	res, err := metadata.NewResolver(db, id)
	if err != nil {
		return nil, err
	}
	tm, err := res.Resolve(sName, tNam)
	if err != nil {
		return nil, err
	}
	return tm.PKColumnMetas(), nil
}
