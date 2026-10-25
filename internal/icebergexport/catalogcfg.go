package icebergexport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dbtrail/dbtrail/internal/storage"

	// Registers the s3:// (and gs://, abfs://) file IO the Glue and REST
	// catalogs write data files through.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// CatalogKind names where the export registers its tables.
type CatalogKind string

const (
	// CatalogHadoop is the local filesystem catalog: tables and metadata
	// under a local directory, one writer at a time (a flock). The default.
	CatalogHadoop CatalogKind = "hadoop"
	// CatalogGlue registers tables in the AWS Glue Data Catalog, with data
	// and metadata under an s3:// warehouse.
	CatalogGlue CatalogKind = "glue"
	// CatalogREST talks to an Iceberg REST catalog (Glue's Iceberg endpoint,
	// S3 Tables, Polaris, Nessie, Unity, ...).
	CatalogREST CatalogKind = "rest"
)

// CatalogConfig is the catalog a run writes to. The zero value is the local
// Hadoop catalog, today's behavior.
type CatalogConfig struct {
	Kind CatalogKind

	// URI is the REST catalog's base URL (rest only).
	URI string
	// Warehouse is the REST "warehouse" identifier the server resolves: a
	// catalog name, an account id, an S3 Tables bucket ARN (rest only).
	// It is not a location; Config.Warehouse is.
	Warehouse string

	// SigV4 signs REST requests with AWS credentials from the default chain
	// (rest only). Region and service default from the URI when it is a Glue
	// or S3 Tables endpoint (sigV4Defaults).
	SigV4        bool
	SigV4Region  string
	SigV4Service string

	// Credential is an OAuth2 client credential ("client_id:client_secret")
	// and Token a bearer token (rest only). They arrive from the environment
	// or a file, never from the command line; nothing here echoes them.
	Credential string
	Token      string
}

func (c CatalogConfig) kind() CatalogKind {
	if c.Kind == "" {
		return CatalogHadoop
	}
	return CatalogKind(strings.ToLower(string(c.Kind)))
}

// CheckCatalog validates the catalog settings together with the warehouse
// and returns the location tables are placed under: the directory as given
// for hadoop (CheckWarehouse's contract), the normalized s3:// prefix for
// glue, and for rest the normalized prefix or "" (the server decides).
//
// Every refusal names the setting and never a secret's value.
func CheckCatalog(c CatalogConfig, warehouse string) (string, error) {
	kind := c.kind()
	switch kind {
	case CatalogHadoop, CatalogGlue, CatalogREST:
	default:
		return "", fmt.Errorf("unknown catalog %q: use hadoop, glue or rest", c.Kind)
	}
	if kind != CatalogREST {
		for _, f := range []struct {
			set  bool
			name string
		}{
			{c.URI != "", "--catalog-uri"},
			{c.Warehouse != "", "--catalog-warehouse"},
			{c.SigV4, "--catalog-sigv4"},
			{c.SigV4Region != "", "--catalog-sigv4-region"},
			{c.SigV4Service != "", "--catalog-sigv4-service"},
			{c.Credential != "", "a catalog credential"},
			{c.Token != "", "a catalog token"},
		} {
			if f.set {
				return "", fmt.Errorf("%s only applies to --catalog rest", f.name)
			}
		}
	}

	switch kind {
	case CatalogHadoop:
		if err := CheckWarehouse(warehouse); err != nil {
			return "", err
		}
		return warehouse, nil

	case CatalogGlue:
		if strings.TrimSpace(warehouse) == "" {
			return "", errors.New("--catalog glue needs --warehouse s3://bucket/prefix: the location the tables' data and metadata are written under")
		}
		if m := locScheme.FindStringSubmatch(strings.TrimSpace(warehouse)); m == nil || !strings.EqualFold(m[1], "s3") {
			return "", fmt.Errorf("--warehouse %q must be an s3:// location (s3://bucket/prefix) with --catalog glue", strings.TrimSpace(warehouse))
		}
		loc, _, err := normalizeLocation(warehouse)
		if err != nil {
			return "", err
		}
		return loc, nil

	default: // rest
		if strings.TrimSpace(c.URI) == "" {
			return "", errors.New("--catalog rest needs --catalog-uri, the catalog's base URL")
		}
		u, err := url.Parse(strings.TrimSpace(c.URI))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			// Not echoed: a value that fails to parse may still hold a secret.
			return "", errors.New("--catalog-uri must be an http:// or https:// URL such as https://glue.us-west-2.amazonaws.com/iceberg")
		}
		if u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
			// The library builds every request from this URI, so a token in
			// the query would also surface in its own error messages.
			return "", fmt.Errorf("--catalog-uri %s carries a query or fragment; give the plain base URL, and put a token in %s or --catalog-token-file",
				RedactURI(c.URI), "BINTRAIL_ICEBERG_CATALOG_TOKEN")
		}
		if u.User != nil {
			return "", fmt.Errorf("--catalog-uri %s carries a user name or password; credentials never go in the URI: set %s or %s, or use --catalog-credential-file or --catalog-token-file",
				RedactURI(c.URI), "BINTRAIL_ICEBERG_CATALOG_CREDENTIAL", "BINTRAIL_ICEBERG_CATALOG_TOKEN")
		}
		methods := 0
		for _, set := range []bool{c.SigV4, c.Credential != "", c.Token != ""} {
			if set {
				methods++
			}
		}
		if methods > 1 {
			return "", errors.New("pick one way to authenticate to the REST catalog: --catalog-sigv4, a client credential, or a bearer token")
		}
		if (c.Credential != "" || c.Token != "") && u.Scheme == "http" && !isLoopback(u.Hostname()) {
			return "", fmt.Errorf("--catalog-uri %s would send the credential or token over plain http://, readable by anyone on the network path; use https://, or http:// only to this machine (localhost)",
				RedactURI(c.URI))
		}
		if !c.SigV4 && c.SigV4Region != "" {
			return "", errors.New("--catalog-sigv4-region needs --catalog-sigv4")
		}
		if !c.SigV4 && c.SigV4Service != "" {
			return "", errors.New("--catalog-sigv4-service needs --catalog-sigv4")
		}
		if strings.TrimSpace(warehouse) == "" {
			return "", nil
		}
		loc, _, err := normalizeLocation(warehouse)
		if err != nil {
			return "", err
		}
		return loc, nil
	}
}

// isLoopback reports whether host names this machine.
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var locScheme = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+.-]*)://(.*)$`)

// normalizeLocation turns an object-store location into one spelling:
// trimmed, scheme lower-cased, empty path segments dropped and no trailing
// slash. "s3://b//p/" and "s3://b/p" are the same place to S3 only by
// accident of how each engine joins keys, and a table location is joined
// onto, so the doubled or trailing slash would land in every object key.
//
// It refuses what it cannot normalize without guessing: no bucket, a query
// or fragment, and "." or ".." segments (S3 keeps them literally, Hadoop
// paths resolve them).
func normalizeLocation(raw string) (loc, scheme string, err error) {
	v := strings.TrimSpace(raw)
	m := locScheme.FindStringSubmatch(v)
	if m == nil {
		return "", "", fmt.Errorf("--warehouse %q must be an object-store location such as s3://bucket/prefix; a remote catalog cannot read a local path", v)
	}
	scheme = strings.ToLower(m[1])
	if scheme == "file" {
		return "", scheme, fmt.Errorf("--warehouse %q must be an object-store location such as s3://bucket/prefix; a remote catalog cannot read a local path", v)
	}
	rest := m[2]
	if strings.ContainsAny(rest, "?#") {
		return "", scheme, fmt.Errorf("--warehouse %q carries a query or fragment; give the plain location", v)
	}
	var segs []string
	for s := range strings.SplitSeq(rest, "/") {
		switch s {
		case "":
			continue
		case ".", "..":
			return "", scheme, fmt.Errorf(`--warehouse %q has a "." or ".." path segment; give the location without it`, v)
		}
		segs = append(segs, s)
	}
	if len(segs) == 0 || strings.HasPrefix(rest, "/") {
		return "", scheme, fmt.Errorf("--warehouse %q names no bucket", v)
	}
	return scheme + "://" + strings.Join(segs, "/"), scheme, nil
}

// tableLocation is where a table is created: <base>/<schema>/<table>, the
// same layout as the local warehouse. "" lets the catalog choose.
func tableLocation(base, schema, tbl string) string {
	if base == "" {
		return ""
	}
	return base + "/" + schema + "/" + tbl
}

var awsCatalogHost = regexp.MustCompile(`^(glue|s3tables)\.([a-z0-9-]+)\.amazonaws\.com$`)

// sigV4Defaults fills the signing region and service. For the two AWS
// catalogs bintrail documents, Glue's Iceberg endpoint and S3 Tables', the
// URI's host names both, so the documented command needs no extra flag and
// cannot sign for the wrong service. Any other host gets "execute-api",
// which is iceberg-go's own default and what a catalog behind API Gateway
// expects; its region comes from the AWS chain. Explicit values win.
func sigV4Defaults(uri, region, service string) (string, string) {
	var hostSvc, hostRegion string
	if u, err := url.Parse(strings.TrimSpace(uri)); err == nil {
		if m := awsCatalogHost.FindStringSubmatch(strings.ToLower(u.Hostname())); m != nil {
			hostSvc, hostRegion = m[1], m[2]
		}
	}
	if region == "" {
		region = hostRegion
	}
	if service == "" {
		service = hostSvc
	}
	if service == "" {
		service = "execute-api"
	}
	return region, service
}

// glueName is what Glue and Athena keep a database or table name as.
var glueName = regexp.MustCompile(`^[a-z0-9_]+$`)

// checkCatalogNames refuses, per schema.table entry, the names a catalog
// would not keep apart or as given. bintrail never renames: a renamed table would be
// found by no one who knows the source, and two sources could land on one.
//
//   - glue: Glue (and Athena over it) store database and table names in
//     lowercase letters, digits and underscores; anything else is folded or
//     rejected. Such a name is refused, and the refusal lists it.
//   - every catalog: two source schemas, or two tables of one schema, whose
//     names differ only in case would meet in a catalog that compares
//     without case (Glue always does; many REST servers do), and in one
//     directory of the local catalog on a case-insensitive filesystem
//     (macOS, Windows). Both are refused, each naming the other.
//
// This only sees the entries of one run; a collision with a table an
// earlier run created is caught by the table's recorded source name
// (checkProvenance). Entries that are not schema.table are left to Run,
// which refuses them.
func checkCatalogNames(kind CatalogKind, entries []string) map[string]error {
	out := map[string]error{}
	type ent struct{ raw, schema, tbl string }
	var ents []ent
	schemaSpell := map[string][]string{} // folded schema -> distinct spellings
	tableSpell := map[string][]string{}  // folded schema.table -> distinct spellings
	for _, e := range entries {
		s, t, ok := strings.Cut(e, ".")
		if !ok || s == "" || t == "" {
			continue
		}
		ents = append(ents, ent{e, s, t})
		fs := strings.ToLower(s)
		if !slices.Contains(schemaSpell[fs], s) {
			schemaSpell[fs] = append(schemaSpell[fs], s)
		}
		ft := fs + "." + strings.ToLower(t)
		if !slices.Contains(tableSpell[ft], e) {
			tableSpell[ft] = append(tableSpell[ft], e)
		}
	}
	for _, e := range ents {
		if sp := schemaSpell[strings.ToLower(e.schema)]; len(sp) > 1 {
			out[e.raw] = fmt.Errorf("source schemas %s would land in one catalog namespace, because the catalog compares names without case; export only one of them",
				joinAnd(sp))
			continue
		}
		if sp := tableSpell[strings.ToLower(e.schema)+"."+strings.ToLower(e.tbl)]; len(sp) > 1 {
			out[e.raw] = fmt.Errorf("source tables %s would land in one catalog table, because the catalog compares names without case; export only one of them",
				joinAnd(sp))
			continue
		}
		if kind == CatalogGlue {
			var bad []string
			if !glueName.MatchString(e.schema) {
				bad = append(bad, e.schema+" (schema)")
			}
			if !glueName.MatchString(e.tbl) {
				bad = append(bad, e.tbl+" (table)")
			}
			if len(bad) > 0 {
				out[e.raw] = fmt.Errorf("Glue keeps database and table names in lowercase letters, digits and underscores, and bintrail does not rename them: %s do not fit; export this table with --catalog rest or the local catalog",
					strings.Join(bad, ", "))
			}
		}
	}
	return out
}

// joinAnd renders "a and b" or "a, b and c".
func joinAnd(xs []string) string {
	if len(xs) <= 1 {
		return strings.Join(xs, "")
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}

// catalogRemedy rewrites a refusal's remedy for the catalog it applies to.
// The refusals in this package tell a Hadoop user to remove the table's
// directory; under Glue or REST the table is a catalog entry, and the same
// remedy is dropping it there (DROP TABLE in Athena, Spark or Trino, or the
// catalog's own API). catalogcfg_test reads the phrases from the source.
func catalogRemedy(kind CatalogKind, msg string) string {
	if kind == CatalogHadoop || kind == "" {
		return msg
	}
	return strings.NewReplacer(
		"remove the table directory", "drop the table from the catalog",
		"Remove the table directory", "Drop the table from the catalog",
		"the table directory must be removed", "the table must be dropped from the catalog",
	).Replace(msg)
}

// openCatalog opens the catalog a run writes to and returns it guarded
// (guard.go), with the release for whatever keeps a second writer out and
// the context the run must use from here on: for glue and SigV4 REST it
// carries the AWS config, so the S3 file IO writes with the same
// credentials and region the catalog signs with instead of building its own.
//
// Only the local catalog takes a lock. Glue and REST commit optimistically
// and the guard refuses a lost race, so their release is a no-op.
func openCatalog(ctx context.Context, c CatalogConfig, location string) (catalog.Catalog, func(), context.Context, error) {
	noop := func() {}
	switch c.kind() {
	case CatalogGlue:
		files, cfg, err := awsConfigs(ctx, "", "set AWS_REGION, or a region in the AWS profile, to the region of the Glue catalog")
		if err != nil {
			return nil, nil, ctx, err
		}
		ctx = utils.WithAwsConfig(ctx, &files)
		return guard(glue.NewCatalog(glue.WithAwsConfig(cfg))), noop, ctx, nil

	case CatalogREST:
		var opts []rest.Option
		if c.Warehouse != "" {
			opts = append(opts, rest.WithWarehouseLocation(c.Warehouse))
		}
		switch {
		case c.SigV4:
			region, service := sigV4Defaults(c.URI, c.SigV4Region, c.SigV4Service)
			files, cfg, err := awsConfigs(ctx, region, "pass --catalog-sigv4-region, or set AWS_REGION, to the catalog's region")
			if err != nil {
				return nil, nil, ctx, err
			}
			ctx = utils.WithAwsConfig(ctx, &files)
			opts = append(opts, rest.WithSigV4RegionSvc(cfg.Region, service), rest.WithAwsConfig(cfg))
		case c.Credential != "":
			opts = append(opts, rest.WithCredential(c.Credential))
		case c.Token != "":
			opts = append(opts, rest.WithOAuthToken(c.Token))
		}
		cat, err := rest.NewCatalog(ctx, "bintrail", strings.TrimSpace(c.URI), opts...)
		if err != nil {
			return nil, nil, ctx, fmt.Errorf("open the REST catalog at %s: %w", RedactURI(c.URI), err)
		}
		return guard(cat), noop, ctx, nil

	default:
		cat, release, err := openHadoopWarehouse(ctx, location)
		if err != nil {
			return nil, nil, ctx, err
		}
		return guard(cat), release, ctx, nil
	}
}

// awsConfigs loads the AWS chain through storage.LoadAWSConfig, the one
// place bintrail builds S3 configs, so BINTRAIL_S3_ENDPOINT and the IMDS
// region fallback apply here as everywhere else. It returns two configs:
// files is what the S3 file IO reads (it keeps a BINTRAIL_S3_ENDPOINT, which
// points the data at an S3-compatible store), and catalog is the same
// without that endpoint, because Glue and the SigV4 REST endpoint are AWS
// services that an S3-compatible store does not serve. A config with no
// region is refused here: the SDK would only report it from deep inside the
// first request.
func awsConfigs(ctx context.Context, region, fix string) (files, catalog aws.Config, err error) {
	files, err = storage.LoadAWSConfig(ctx, region)
	if err != nil {
		return aws.Config{}, aws.Config{}, fmt.Errorf("load the AWS configuration: %w", err)
	}
	if files.Region == "" {
		return aws.Config{}, aws.Config{}, fmt.Errorf("no AWS region is configured: %s", fix)
	}
	catalog = files.Copy()
	catalog.BaseEndpoint = nil
	return files, catalog, nil
}

// RedactURI renders a catalog URI for a message or a report as
// scheme://host/path only. A URI can carry a secret in its userinfo
// (refused by CheckCatalog, but this also runs on values that were not
// checked) or in a query or fragment token, and a message is printed to the
// terminal, kept in logs and written into the JSON report. A value that does
// not parse is not echoed at all.
func RedactURI(s string) string {
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "(unparseable URI)"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}
