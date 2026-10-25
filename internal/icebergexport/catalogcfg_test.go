package icebergexport

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These pin how --catalog, --warehouse and the REST flags combine. Every
// refusal fires before a catalog, a bucket or the index is touched.

func TestCheckCatalog_refusals(t *testing.T) {
	cases := []struct {
		name string
		cat  CatalogConfig
		wh   string
		want string
	}{
		{"unknown kind", CatalogConfig{Kind: "hive"}, "/w", `unknown catalog "hive"`},
		{"hadoop with a URL warehouse", CatalogConfig{}, "s3://b/p", "object-store URL"},
		{"hadoop with a uri", CatalogConfig{URI: "https://x"}, "/w", "--catalog-uri only applies to --catalog rest"},
		{"hadoop with a catalog warehouse", CatalogConfig{Warehouse: "acct"}, "/w", "--catalog-warehouse only applies to --catalog rest"},
		{"hadoop with sigv4", CatalogConfig{SigV4: true}, "/w", "--catalog-sigv4 only applies to --catalog rest"},
		{"hadoop with a credential", CatalogConfig{Credential: "id:secret"}, "/w", "a catalog credential only applies to --catalog rest"},
		{"hadoop with a token", CatalogConfig{Token: "tok"}, "/w", "a catalog token only applies to --catalog rest"},

		{"glue without warehouse", CatalogConfig{Kind: CatalogGlue}, "", "--catalog glue needs --warehouse s3://bucket/prefix"},
		{"glue with blank warehouse", CatalogConfig{Kind: CatalogGlue}, "   ", "--catalog glue needs --warehouse s3://bucket/prefix"},
		{"glue with a local path", CatalogConfig{Kind: CatalogGlue}, "/data/iceberg", "must be an s3:// location"},
		{"glue with a relative path", CatalogConfig{Kind: CatalogGlue}, "iceberg", "must be an s3:// location"},
		{"glue with gs", CatalogConfig{Kind: CatalogGlue}, "gs://b/p", "must be an s3:// location"},
		{"glue with s3a", CatalogConfig{Kind: CatalogGlue}, "s3a://b/p", "must be an s3:// location"},
		{"glue with file", CatalogConfig{Kind: CatalogGlue}, "file:///tmp/x", "must be an s3:// location"},
		{"glue with no bucket", CatalogConfig{Kind: CatalogGlue}, "s3://", "names no bucket"},
		{"glue with an empty bucket", CatalogConfig{Kind: CatalogGlue}, "s3:///p", "names no bucket"},
		{"glue with a query", CatalogConfig{Kind: CatalogGlue}, "s3://b/p?x=1", "query"},
		{"glue with a fragment", CatalogConfig{Kind: CatalogGlue}, "s3://b/p#x", "query"},
		{"glue with dot-dot", CatalogConfig{Kind: CatalogGlue}, "s3://b/a/../c", `"." or ".."`},
		{"glue with dot", CatalogConfig{Kind: CatalogGlue}, "s3://b/./c", `"." or ".."`},
		{"glue with a uri", CatalogConfig{Kind: CatalogGlue, URI: "https://x"}, "s3://b/p", "--catalog-uri only applies to --catalog rest"},
		{"glue with a token", CatalogConfig{Kind: CatalogGlue, Token: "t"}, "s3://b/p", "a catalog token only applies to --catalog rest"},

		{"rest without uri", CatalogConfig{Kind: CatalogREST}, "", "--catalog rest needs --catalog-uri"},
		{"rest with a non-http uri", CatalogConfig{Kind: CatalogREST, URI: "glue.us-west-2.amazonaws.com/iceberg"}, "", "must be an http:// or https:// URL"},
		{"rest with a local warehouse", CatalogConfig{Kind: CatalogREST, URI: "https://c"}, "/data/iceberg", "must be an object-store location"},
		{"rest with a file warehouse", CatalogConfig{Kind: CatalogREST, URI: "https://c"}, "file:///data", "must be an object-store location"},
		{"rest with sigv4 and a token", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4: true, Token: "t"}, "", "pick one way to authenticate"},
		{"rest with a credential and a token", CatalogConfig{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Token: "t"}, "", "pick one way to authenticate"},
		{"rest with sigv4 and a credential", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4: true, Credential: "a:b"}, "", "pick one way to authenticate"},
		{"rest token over plain http", CatalogConfig{Kind: CatalogREST, URI: "http://catalog.example.com", Token: "t"}, "", "over plain http://"},
		{"rest credential over plain http", CatalogConfig{Kind: CatalogREST, URI: "http://10.0.0.5:8181", Credential: "a:b"}, "", "over plain http://"},
		{"rest sigv4 region without sigv4", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4Region: "us-west-2"}, "", "--catalog-sigv4-region needs --catalog-sigv4"},
		{"rest sigv4 service without sigv4", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4Service: "glue"}, "", "--catalog-sigv4-service needs --catalog-sigv4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CheckCatalog(tc.cat, tc.wh)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckCatalog(%+v, %q) = %v, want an error containing %q", tc.cat, tc.wh, err, tc.want)
			}
		})
	}
}

// TestCheckCatalog_secretsNeverEchoed: a refusal that involves a secret
// names the setting, never its value.
func TestCheckCatalog_secretsNeverEchoed(t *testing.T) {
	for _, c := range []CatalogConfig{
		{Credential: "client:S3CR3T"},
		{Kind: CatalogGlue, Credential: "client:S3CR3T"},
		{Kind: CatalogREST, URI: "https://c", Credential: "client:S3CR3T", Token: "S3CR3T"},
		{Kind: CatalogREST, URI: "https://c", SigV4: true, Token: "S3CR3T"},
	} {
		_, err := CheckCatalog(c, "s3://b/p")
		if err == nil {
			t.Fatalf("CheckCatalog(%+v) accepted", c)
		}
		if strings.Contains(err.Error(), "S3CR3T") {
			t.Fatalf("refusal echoes the secret: %v", err)
		}
	}
}

func TestCheckCatalog_accepts(t *testing.T) {
	cases := []struct {
		name string
		cat  CatalogConfig
		wh   string
		want string // the normalized location
	}{
		{"hadoop default", CatalogConfig{}, "/data/iceberg", "/data/iceberg"},
		{"hadoop explicit, value kept as given", CatalogConfig{Kind: CatalogHadoop}, "rel/dir/", "rel/dir/"},
		{"kind is case-insensitive", CatalogConfig{Kind: "GLUE"}, "s3://b/p", "s3://b/p"},
		{"glue bucket only", CatalogConfig{Kind: CatalogGlue}, "s3://b", "s3://b"},
		{"glue trailing slash", CatalogConfig{Kind: CatalogGlue}, "s3://b/p/", "s3://b/p"},
		{"glue bucket and slash", CatalogConfig{Kind: CatalogGlue}, "s3://b/", "s3://b"},
		{"glue double slashes", CatalogConfig{Kind: CatalogGlue}, "s3://b//a///c//", "s3://b/a/c"},
		{"glue scheme case", CatalogConfig{Kind: CatalogGlue}, "S3://b/p", "s3://b/p"},
		{"glue surrounding blanks", CatalogConfig{Kind: CatalogGlue}, "  s3://b/p \n", "s3://b/p"},
		{"rest without warehouse", CatalogConfig{Kind: CatalogREST, URI: "https://c"}, "", ""},
		{"rest with a warehouse prefix", CatalogConfig{Kind: CatalogREST, URI: "https://c", Warehouse: "123456789012"}, "s3://b//p/", "s3://b/p"},
		{"rest with gs prefix", CatalogConfig{Kind: CatalogREST, URI: "http://localhost:8181"}, "gs://b/p", "gs://b/p"},
		{"rest with sigv4", CatalogConfig{Kind: CatalogREST, URI: "https://glue.us-west-2.amazonaws.com/iceberg", SigV4: true}, "", ""},
		{"rest with a token", CatalogConfig{Kind: CatalogREST, URI: "https://c", Token: "t"}, "", ""},
		{"rest with a credential", CatalogConfig{Kind: CatalogREST, URI: "https://c", Credential: "a:b"}, "", ""},
		{"rest token over http to localhost", CatalogConfig{Kind: CatalogREST, URI: "http://localhost:8181", Token: "t"}, "", ""},
		{"rest token over http to 127.0.0.1", CatalogConfig{Kind: CatalogREST, URI: "http://127.0.0.1:8181", Token: "t"}, "", ""},
		{"rest token over http to ::1", CatalogConfig{Kind: CatalogREST, URI: "http://[::1]:8181", Token: "t"}, "", ""},
		{"rest without secrets over http", CatalogConfig{Kind: CatalogREST, URI: "http://catalog.example.com"}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CheckCatalog(tc.cat, tc.wh)
			if err != nil {
				t.Fatalf("CheckCatalog(%+v, %q) = %v, want accepted", tc.cat, tc.wh, err)
			}
			if got != tc.want {
				t.Fatalf("location = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTableLocation(t *testing.T) {
	cases := []struct{ base, schema, tbl, want string }{
		{"s3://b/p", "shop", "orders", "s3://b/p/shop/orders"},
		{"s3://b", "shop", "orders", "s3://b/shop/orders"},
		{"", "shop", "orders", ""}, // REST without a prefix: the server picks
	}
	for _, tc := range cases {
		if got := tableLocation(tc.base, tc.schema, tc.tbl); got != tc.want {
			t.Errorf("tableLocation(%q, %q, %q) = %q, want %q", tc.base, tc.schema, tc.tbl, got, tc.want)
		}
	}
}

// TestSigV4Defaults: the signing service and region come from the URI when
// it is an AWS endpoint bintrail knows, so the documented Glue and S3 Tables
// commands need no extra flag; anything else falls back to API Gateway's
// "execute-api" (iceberg-go's own default) and the AWS chain's region.
func TestSigV4Defaults(t *testing.T) {
	cases := []struct {
		uri, region, service string
		wantRegion, wantSvc  string
	}{
		{"https://glue.us-west-2.amazonaws.com/iceberg", "", "", "us-west-2", "glue"},
		{"https://GLUE.eu-central-1.amazonaws.com/iceberg/", "", "", "eu-central-1", "glue"},
		{"https://s3tables.us-east-1.amazonaws.com/iceberg", "", "", "us-east-1", "s3tables"},
		{"https://glue.us-west-2.amazonaws.com/iceberg", "us-east-1", "", "us-east-1", "glue"},
		{"https://glue.us-west-2.amazonaws.com/iceberg", "", "execute-api", "us-west-2", "execute-api"},
		{"https://abc.execute-api.us-west-2.amazonaws.com/prod", "", "", "", "execute-api"},
		{"https://catalog.example.com", "", "", "", "execute-api"},
		{"https://glue.us-west-2.amazonaws.com.evil.example/iceberg", "", "", "", "execute-api"},
	}
	for _, tc := range cases {
		r, s := sigV4Defaults(tc.uri, tc.region, tc.service)
		if r != tc.wantRegion || s != tc.wantSvc {
			t.Errorf("sigV4Defaults(%q, %q, %q) = (%q, %q), want (%q, %q)", tc.uri, tc.region, tc.service, r, s, tc.wantRegion, tc.wantSvc)
		}
	}
}

// TestCheckCatalogNames covers the per-entry name rules. Glue (and Athena
// over it) keep database and table names in lowercase letters, digits and
// underscores and fold anything else; bintrail does not rename, so a name
// that would be folded is refused and named. REST passes names through,
// but two source names that differ only in case would still meet in a
// catalog that compares without case, so those are refused on every
// non-hadoop catalog, naming both.
func TestCheckCatalogNames(t *testing.T) {
	type want map[string]string // entry -> substring of its refusal ("" = accepted)
	cases := []struct {
		name    string
		kind    CatalogKind
		entries []string
		want    want
	}{
		{"glue lowercase ok", CatalogGlue, []string{"shop.orders", "shop_2.line_items"}, want{"shop.orders": "", "shop_2.line_items": ""}},
		{"glue uppercase schema", CatalogGlue, []string{"Shop.orders"}, want{"Shop.orders": "Shop (schema)"}},
		{"glue uppercase table", CatalogGlue, []string{"shop.Orders"}, want{"shop.Orders": "Orders (table)"}},
		{"glue both named", CatalogGlue, []string{"My-Shop.Orders"}, want{"My-Shop.Orders": "My-Shop (schema), Orders (table)"}},
		{"glue hyphen", CatalogGlue, []string{"shop.line-items"}, want{"shop.line-items": "line-items (table)"}},
		{"glue dot in table", CatalogGlue, []string{"shop.a.b"}, want{"shop.a.b": "a.b (table)"}},
		{"glue non-ascii", CatalogGlue, []string{"tienda.niño"}, want{"tienda.niño": "niño (table)"}},
		{"glue space", CatalogGlue, []string{"shop.my table"}, want{"shop.my table": "my table (table)"}},
		{"rest passes mixed case through", CatalogREST, []string{"Shop.Orders", "x.line-items"}, want{"Shop.Orders": "", "x.line-items": ""}},
		{"rest schema case collision", CatalogREST, []string{"Shop.orders", "shop.customers"},
			want{"Shop.orders": "Shop and shop", "shop.customers": "Shop and shop"}},
		{"rest table case collision", CatalogREST, []string{"shop.Orders", "shop.orders", "shop.items"},
			want{"shop.Orders": "shop.Orders and shop.orders", "shop.orders": "shop.Orders and shop.orders", "shop.items": ""}},
		{"glue collision names both", CatalogGlue, []string{"Shop.orders", "shop.orders"},
			want{"Shop.orders": "Shop and shop", "shop.orders": "Shop and shop"}},
		{"exact duplicate is not a collision", CatalogREST, []string{"shop.orders", "shop.orders"}, want{"shop.orders": ""}},
		{"hadoop refuses case collisions too", CatalogHadoop, []string{"Shop.orders", "shop.orders", "x.Line-Items"},
			want{"Shop.orders": "Shop and shop", "shop.orders": "Shop and shop", "x.Line-Items": ""}},
		{"hadoop table case collision", CatalogHadoop, []string{"shop.Orders", "shop.orders"},
			want{"shop.Orders": "shop.Orders and shop.orders", "shop.orders": "shop.Orders and shop.orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkCatalogNames(tc.kind, tc.entries)
			for entry, sub := range tc.want {
				err := got[entry]
				switch {
				case sub == "" && err != nil:
					t.Errorf("%s: refused (%v), want accepted", entry, err)
				case sub != "" && (err == nil || !strings.Contains(err.Error(), sub)):
					t.Errorf("%s: err = %v, want containing %q", entry, err, sub)
				}
			}
			for entry, err := range got {
				if _, ok := tc.want[entry]; !ok && err != nil {
					t.Errorf("unexpected refusal for %s: %v", entry, err)
				}
			}
		})
	}
}

// TestCatalogRemedy: every refusal in this package that tells the operator
// to remove the table DIRECTORY is a Hadoop instruction. Under Glue or REST
// the table is a catalog entry, and the remedy says to drop it there. The
// phrases are read from the source, so a new spelling that the rewrite does
// not cover fails here instead of reaching an operator.
func TestCatalogRemedy(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var msgs []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for line := range strings.SplitSeq(string(b), "\n") {
			if strings.Contains(line, "table directory") && !strings.HasPrefix(strings.TrimSpace(line), "//") {
				msgs = append(msgs, line)
			}
		}
	}
	if len(msgs) < 5 {
		t.Fatalf("found %d refusal lines naming the table directory; the scan is broken", len(msgs))
	}
	for _, m := range msgs {
		for _, kind := range []CatalogKind{CatalogGlue, CatalogREST} {
			got := catalogRemedy(kind, m)
			if strings.Contains(got, "directory") {
				t.Errorf("%s: remedy still names a directory: %s", kind, strings.TrimSpace(got))
			}
			if !strings.Contains(strings.ToLower(got), "drop") {
				t.Errorf("%s: remedy does not say to drop the table: %s", kind, strings.TrimSpace(got))
			}
		}
		if got := catalogRemedy(CatalogHadoop, m); got != m {
			t.Errorf("hadoop remedy changed: %s", got)
		}
	}
}

// TestOpenCatalog_glueWithoutRegion: a missing region is a sentence, not an
// SDK error from deep in the first request.
func TestOpenCatalog_glueWithoutRegion(t *testing.T) {
	isolateAWSEnv(t)
	_, _, _, err := openCatalog(context.Background(), CatalogConfig{Kind: CatalogGlue}, "s3://b/p")
	if err == nil || !strings.Contains(err.Error(), "no AWS region") || !strings.Contains(err.Error(), "AWS_REGION") {
		t.Fatalf("err = %v, want the no-region refusal naming AWS_REGION", err)
	}
}

func TestOpenCatalog_restSigV4WithoutRegion(t *testing.T) {
	isolateAWSEnv(t)
	_, _, _, err := openCatalog(context.Background(), CatalogConfig{Kind: CatalogREST, URI: "https://catalog.example.com", SigV4: true}, "")
	if err == nil || !strings.Contains(err.Error(), "no AWS region") || !strings.Contains(err.Error(), "--catalog-sigv4-region") {
		t.Fatalf("err = %v, want the no-region refusal naming --catalog-sigv4-region", err)
	}
}

// isolateAWSEnv points the AWS SDK at empty config files and clears every
// region variable, so the default chain resolves no region.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", empty)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", empty)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// TestCheckCatalog_uriCarryingSecrets: a REST URI with user:password is
// refused, and no refusal or report line echoes a secret a URI may carry
// (userinfo, a query token), even when the URI does not parse.
func TestCheckCatalog_uriCarryingSecrets(t *testing.T) {
	for _, uri := range []string{
		"https://user:S3CR3T@catalog.example.com/iceberg",
		"https://S3CR3T@catalog.example.com/iceberg",
	} {
		_, err := CheckCatalog(CatalogConfig{Kind: CatalogREST, URI: uri}, "")
		if err == nil || !strings.Contains(err.Error(), "BINTRAIL_ICEBERG_CATALOG_CREDENTIAL") {
			t.Fatalf("CheckCatalog(uri with userinfo) = %v, want the refusal naming where credentials go", err)
		}
		if strings.Contains(err.Error(), "S3CR3T") {
			t.Fatalf("refusal echoes the secret: %v", err)
		}
	}
	for _, uri := range []string{
		"https://u:S3CR3T@[bad/x",
		"ftp://u:S3CR3T@host/x",
		"https://host/iceberg?token=S3CR3T",
		"https://host/iceberg#S3CR3T",
		"u:S3CR3T@host/x",
	} {
		_, err := CheckCatalog(CatalogConfig{Kind: CatalogREST, URI: uri}, "")
		if err == nil || strings.Contains(err.Error(), "S3CR3T") {
			t.Fatalf("CheckCatalog(%q) = %v, want a refusal without the secret", uri, err)
		}
	}
}

func TestRedactURI(t *testing.T) {
	cases := map[string]string{
		"https://glue.us-west-2.amazonaws.com/iceberg": "https://glue.us-west-2.amazonaws.com/iceberg",
		"https://u:S3CR3T@host/x":                      "https://host/x",
		"https://host/x?token=S3CR3T":                  "https://host/x",
		"https://host:8181/x#S3CR3T":                   "https://host:8181/x",
		"https://u:S3CR3T@[bad/x":                      "(unparseable URI)",
		"  http://localhost:8181  ":                    "http://localhost:8181",
	}
	for in, want := range cases {
		if got := RedactURI(in); got != want {
			t.Errorf("RedactURI(%q) = %q, want %q", in, got, want)
		}
	}
}
