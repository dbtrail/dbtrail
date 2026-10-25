package icebergexport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

// catalogName is the filesystem catalog's name; it only labels the warehouse.
const catalogName = "bintrail"

// lockFileName is the single-writer lock at the warehouse root. The
// filesystem catalog commits by writing a new metadata file and then a
// version hint; two writers racing that would each believe they won. bintrail
// is the single writer of these tables by design, and the lock makes an
// accidental second run refuse instead of clobber.
const lockFileName = ".bintrail-export.lock"

// urlScheme matches a value that starts with a URL scheme ("s3://",
// "gs://", "file://", any case). A local path does not: its first separator
// comes before any "://".
var urlScheme = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+.-]*)://`)

// CheckWarehouse refuses a warehouse that is not a local directory. The
// export's catalog is a filesystem (Hadoop) catalog: it commits by writing
// a metadata file and then a version-hint file, and on object storage two
// writers race on that hint. Before this check, "s3://b/p" was not refused:
// filepath.Abs made it the relative directory "./s3:/b/p".
//
// file:// is refused too rather than stripped: stripping would have to
// handle "file://host/p" and percent-encoding, and the run's report echoes
// the value as given, so a stripped value would print one path and write
// another. Detection runs on the trimmed value; a value that passes is used
// untrimmed, as before.
func CheckWarehouse(warehouse string) error {
	v := strings.TrimSpace(warehouse)
	if v == "" {
		return errors.New("a warehouse directory is required")
	}
	m := urlScheme.FindStringSubmatch(v)
	if m == nil {
		return nil
	}
	if strings.EqualFold(m[1], "file") {
		return fmt.Errorf("warehouse %q is a file:// URL: drop the file:// prefix and write to a local directory", warehouse)
	}
	return fmt.Errorf("warehouse %q is an object-store URL: the export writes a filesystem (Hadoop) catalog, "+
		"which commits through a version-hint file that races on object storage; write to a local directory", warehouse)
}

// openWarehouse opens the catalog a run writes to and returns it with the
// release for whatever keeps a second writer out; release must be called
// when the run ends. Today the only catalog is the local Hadoop warehouse,
// whose guard is a flock (openHadoopWarehouse). A catalog with optimistic
// commits guards itself on commit and returns a no-op release.
// Run validates the value with CheckWarehouse first.
func openWarehouse(ctx context.Context, dir string) (catalog.Catalog, func(), error) {
	return openHadoopWarehouse(ctx, dir)
}

var _ catalog.Catalog = (*hadoop.Catalog)(nil)

// openHadoopWarehouse creates the local warehouse directory if needed, takes
// the single-writer lock and opens the filesystem catalog over it. The lock
// belongs to this catalog only: the filesystem catalog has no commit guard
// of its own.
func openHadoopWarehouse(_ context.Context, dir string) (catalog.Catalog, func(), error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create warehouse %s: %w", abs, err)
	}
	release, err := lockWarehouse(filepath.Join(abs, lockFileName))
	if err != nil {
		return nil, nil, err
	}
	cat, err := hadoop.NewCatalog(catalogName, abs, nil)
	if err != nil {
		release()
		return nil, nil, fmt.Errorf("open warehouse %s: %w", abs, err)
	}
	return cat, release, nil
}

// ensureNamespace creates the schema's namespace when it does not exist.
func ensureNamespace(ctx context.Context, cat catalog.Catalog, schema string) error {
	ns := catalog.ToIdentifier(schema)
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	if err != nil {
		return fmt.Errorf("check namespace %s: %w", schema, err)
	}
	if exists {
		return nil
	}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return fmt.Errorf("create namespace %s: %w", schema, err)
	}
	return nil
}

// loadTable returns the table when it exists, or (nil, false, nil) when the
// catalog has no such table.
func loadTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (*table.Table, bool, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("load table %v: %w", ident, err)
	}
	return tbl, true, nil
}

// tableProperties are the properties a new table is created with. Format
// version 2 is what equality deletes require; the merge-on-read modes make an
// engine that later runs DELETE or UPDATE on the table keep the same posture
// instead of rewriting files.
func tableProperties() iceberg.Properties {
	return iceberg.Properties{
		table.PropertyFormatVersion: "2",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		"write.update.mode":         table.WriteModeMergeOnRead,
		"write.merge.mode":          table.WriteModeMergeOnRead,
		propVersion:                 exportVersion,
	}
}
