package console

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestViewsAPI_namesTheS3Endpoint wires the producer to the artifact (#1453).
// The generator is tested with a populated Input; this pins that the console
// FILLS it. views.sql is the one artifact that leaves the machine, and a file
// missing ENDPOINT hands the recipient an s3:// path naming their own bucket
// and sends their DuckDB to AWS, with no error anywhere.
func TestViewsAPI_namesTheS3Endpoint(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "http://minio:9000")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// An S3 archive source: a local-only layout emits no S3 preamble at all,
	// so it could not show the endpoint either way.
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "ENDPOINT 'minio:9000'") {
		t.Errorf("the downloaded file does not name the store it reads:\n%s", sql)
	}
	if !strings.Contains(sql, "URL_STYLE 'path'") || !strings.Contains(sql, "USE_SSL false") {
		t.Errorf("the file names the store but not how to address it:\n%s", sql)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An invalid endpoint is an upstream fault the file must not paper over, on a
// layout that actually reads S3: a 502, not a file that quietly describes AWS.
func TestViewsAPI_invalidEndpointIs502(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // no scheme

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 502 {
		t.Fatalf("code = %d, body = %s; want 502", rec.Code, body)
	}
	if strings.Contains(string(body), "minio:9000") {
		t.Errorf("the 502 body echoes the rejected value: %s", body)
	}
}

// The same broken variable on a server whose data is entirely local is not
// this page's problem: nothing in the rendered file reads through httpfs, so
// refusing would break a working download over a setting it never consults.
func TestViewsAPI_localOnlyLayoutIgnoresEndpointTypo(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // the same rejected value

	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	srv := newViewsServer(t, dir, false)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s; want 200", rec.Code, body)
	}
	// Positive evidence that the endpoint was genuinely not needed, rather
	// than needed and silently dropped.
	if strings.Contains(string(body), "s3://") || strings.Contains(string(body), "httpfs") {
		t.Errorf("a layout treated as local reads S3 after all:\n%s", body)
	}
}

// TestViewsAPI_pinsTheDetectedRegion is the WIRING guard for #1462: the
// resolver is unit-tested on its own, which says nothing about whether
// buildViewsInput calls it. Before this the console pinned no region at all
// while the daemon's own reads pinned a detected one, so a downloaded file
// described a different read than the one this process performs.
func TestViewsAPI_pinsTheDetectedRegion(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	// Pre-seeded so the test needs no network: what is under test is that the
	// resolver's answer reaches the file, not how the answer is obtained.
	srv.bucketRegions = map[string]bucketRegionEntry{"bkt": detected("eu-central-1")}

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "REGION 'eu-central-1'") {
		t.Errorf("the downloaded file pins no region, so the reader resolves their own:\n%s", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The other half of the same rule, at the endpoint: a bucket whose region was
// only GUESSED (GetBucketLocation is outside bintrail's documented minimal IAM
// policy, so this is the common case) must leave the file unpinned. Pinning the
// daemon's ambient region would override the reader's own correct
// configuration on a machine this process cannot see.
func TestViewsAPI_pinsNothingForAGuessedRegion(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	srv.bucketRegions = map[string]bucketRegionEntry{"bkt": fellBack("us-east-1")}

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, "REGION '") || strings.Contains(line, "s3_region") {
			t.Errorf("an unverified region was pinned into a file that leaves this host: %s", line)
		}
	}
	// And it must not claim the buckets disagree, which would be a stated fact.
	if strings.Contains(string(body), "each detected in a DIFFERENT region") {
		t.Errorf("a bucket that could not be asked was reported as a disagreement:\n%s", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Wiring guard for #1575: the downloaded file carries the bucket stores this
// process reads with. The renderer is tested on its own, which says nothing
// about whether buildViewsInput hands it the table.
func TestViewsAPI_carriesTheBucketStores(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))
	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	st, err := storage.NewBucketStore("http://minio:9000", "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"bkt": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "SCOPE 's3://bkt/'") {
		t.Errorf("the downloaded file does not carry the bucket's store:\n%s", body)
	}
}
