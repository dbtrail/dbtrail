package reconstruct

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// httpS3 is an S3 store that only lists, over HTTP, for the tests that must
// see the REQUESTS a lookup makes and the pages S3 cuts a listing into
// (#1740): fakeS3Snapshots stands in for the client, so a page is not a
// thing it has. It answers ListObjectsV2 for one bucket the way S3 does:
// keys in byte order, 1,000 entries a page at most, common prefixes under a
// delimiter counted as entries, start-after and continuation-token honoured.
type httpS3 struct {
	mu       sync.Mutex
	keys     []string // full keys, sorted
	requests []string // the raw query of every listing asked
	// cutAfter, when positive, answers that many listings and fails the
	// rest with a 503: a listing that dies between two pages.
	cutAfter int
	srv      *httptest.Server
}

type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []listedObject `xml:"Contents"`
	CommonPrefixes        []listedPrefix `xml:"CommonPrefixes"`
}

type listedObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listedPrefix struct {
	Prefix string `xml:"Prefix"`
}

const httpS3Bucket = "b"

// newHTTPS3 starts the store holding keys under prefix, and points every S3
// client of this process at it for the length of the test.
func newHTTPS3(t *testing.T, prefix string, keys []string) *httpS3 {
	t.Helper()
	f := &httpS3{}
	for _, k := range keys {
		f.keys = append(f.keys, prefix+k)
	}
	slices.Sort(f.keys)
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)

	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, f.srv.URL)
	resetS3Inventories()
	t.Cleanup(resetS3Inventories)
	return f
}

func (f *httpS3) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket := strings.Trim(r.URL.Path, "/")
	if r.Method != http.MethodGet || bucket != httpS3Bucket || q.Get("list-type") != "2" {
		http.Error(w, "<Error><Code>NoSuchKey</Code><Message>this store only lists</Message></Error>", http.StatusNotFound)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.RawQuery)
	n, cut := len(f.requests), f.cutAfter
	keys := f.keys
	f.mu.Unlock()
	if cut > 0 && n > cut {
		http.Error(w, "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>", http.StatusServiceUnavailable)
		return
	}

	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = tok
	}
	maxKeys := 1000
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v > 0 && v < maxKeys {
		maxKeys = v
	}
	encode := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		encode = func(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "%2F", "/") }
	}

	out := listBucketResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: encode(prefix),
		MaxKeys: maxKeys, Delimiter: encode(delimiter), EncodingType: q.Get("encoding-type"),
	}
	var last, lastPrefix string
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k <= after {
			continue
		}
		// A token that is a common prefix stands for every key under it.
		if delimiter != "" && strings.HasSuffix(after, delimiter) && strings.HasPrefix(k, after) {
			continue
		}
		entry, isPrefix := k, false
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				entry, isPrefix = k[:len(prefix)+i+len(delimiter)], true
			}
		}
		if isPrefix && entry == lastPrefix {
			continue
		}
		if out.KeyCount == maxKeys {
			out.IsTruncated, out.NextContinuationToken = true, last
			break
		}
		if isPrefix {
			out.CommonPrefixes = append(out.CommonPrefixes, listedPrefix{encode(entry)})
			lastPrefix = entry
		} else {
			out.Contents = append(out.Contents, listedObject{
				Key: encode(k), LastModified: "2026-09-01T00:00:00.000Z", ETag: `"d41d8cd98f00b204e9800998ecf8427e"`, StorageClass: "STANDARD",
			})
		}
		last = entry
		out.KeyCount++
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

// asked returns how many listings were asked since the last call.
func (f *httpS3) asked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.requests)
	f.requests = nil
	return n
}
