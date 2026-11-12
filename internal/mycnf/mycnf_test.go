package mycnf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		groups []string
		want   Client
	}{
		{name: "empty file", in: "", want: Client{}},
		{name: "only comments", in: "# a\n; b\n\n", want: Client{}},
		{
			name: "plain client group",
			in:   "[client]\nhost=db1.example.com\nport=3307\nuser=dbtrail\npassword=s3cret\n",
			want: Client{Host: "db1.example.com", Port: "3307", User: "dbtrail", Password: "s3cret"},
		},
		{
			name: "spaces around names, equals and values",
			in:   "  [ client ]  \n  host   =   db1  \n\tuser\t=\tu \n",
			want: Client{Host: "db1", User: "u"},
		},
		{
			name: "group names are not case-sensitive",
			in:   "[CLIENT]\nhost=db1\n",
			want: Client{Host: "db1"},
		},
		{
			name: "other groups are not read",
			in:   "[mysqld]\nport=3306\nuser=mysql\n[client]\nuser=u\n[mysqldump]\nhost=other\n",
			want: Client{User: "u"},
		},
		{
			name: "options before any group are not read",
			in:   "host=stray\n[client]\nuser=u\n",
			want: Client{User: "u"},
		},
		{
			name: "a later group wins over an earlier one, option by option",
			in:   "[dbtrail]\nuser=dbtrail\npassword=p2\n[client]\nhost=db1\nuser=root\npassword=p1\n",
			want: Client{Host: "db1", User: "dbtrail", Password: "p2"},
		},
		{
			name: "the last of a repeated option wins",
			in:   "[client]\nhost=a\nhost=b\n",
			want: Client{Host: "b"},
		},
		{
			name: "a group that appears twice is read both times",
			in:   "[client]\nhost=a\n[mysqld]\nport=1\n[client]\nuser=u\n",
			want: Client{Host: "a", User: "u"},
		},
		{
			name: "hash inside an unquoted value starts a comment",
			in:   "[client]\npassword=abc#def\nhost=db1 # the primary\n",
			want: Client{Host: "db1", Password: "abc"},
		},
		{
			name: "quotes keep a hash, spaces and an equals sign",
			in:   "[client]\npassword=\"ab#c d=e\"\nuser='my user' # who\n",
			want: Client{User: "my user", Password: "ab#c d=e"},
		},
		{
			name: "escapes",
			in:   "[client]\npassword=\"a\\\"b\\\\c\\sd\"\n",
			want: Client{Password: `a"b\c d`},
		},
		{
			name: "a quote in the middle of a value is a character, as in mysql",
			in:   "[client]\npassword='it's'\nuser=ab\"cd\n",
			want: Client{User: `ab"cd`, Password: "it's"},
		},
		{
			name: "a hash inside quotes in the middle of a value is not a comment",
			in:   "[client]\npassword=ab\"c#d\"e\n",
			want: Client{Password: `ab"c#d"e`},
		},
		{
			name: "an escaped space at the end survives, a plain one does not",
			in:   "[client]\npassword=abc\\s\nuser=u   \n",
			want: Client{User: "u", Password: "abc "},
		},
		{
			name: "a quote that is never closed is part of the value",
			in:   "[client]\npassword=\"abc\n",
			want: Client{Password: `"abc`},
		},
		{
			name: "a lone quote and an empty pair",
			in:   "[client]\npassword=\"\nuser=\"\"\n",
			want: Client{Password: `"`},
		},
		{
			name: "a quote of the other kind inside quotes is a character",
			in:   "[client]\npassword=\"it's\"\n",
			want: Client{Password: "it's"},
		},
		{
			name: "underscores and dashes in a name are the same, loose- is dropped",
			in:   "[client]\nloose-host=db1\n",
			want: Client{Host: "db1"},
		},
		{
			name: "an option with no value is empty, not an error",
			in:   "[client]\npassword\nuser=u\nhost=\n",
			want: Client{User: "u"},
		},
		{
			name: "windows line endings and a byte order mark",
			in:   "\xef\xbb\xbf[client]\r\nhost=db1\r\nuser=u\r\n",
			want: Client{Host: "db1", User: "u"},
		},
		{
			name: "socket is reported",
			in:   "[client]\nsocket=/var/run/mysqld/mysqld.sock\nuser=u\n",
			want: Client{User: "u", Socket: "/var/run/mysqld/mysqld.sock"},
		},
		{
			name: "include lines are reported, not followed",
			in:   "!includedir /etc/mysql/conf.d/\n[client]\nuser=u\n!include /etc/extra.cnf\n",
			want: Client{User: "u", Includes: []string{"!includedir /etc/mysql/conf.d/", "!include /etc/extra.cnf"}},
		},
		{
			name:   "custom groups",
			in:     "[client]\nuser=root\n[reports]\nuser=r\n",
			groups: []string{"reports"},
			want:   Client{User: "r"},
		},
		{
			name: "unknown options are ignored",
			in:   "[client]\nssl-ca=/x.pem\ndefault-character-set=utf8mb4\nuser=u\n",
			want: Client{User: "u"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			groups := c.groups
			if groups == nil {
				groups = []string{"client", "dbtrail"}
			}
			got, err := Parse([]byte(c.in), groups...)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got.Host != c.want.Host || got.Port != c.want.Port || got.User != c.want.User ||
				got.Password != c.want.Password || got.Socket != c.want.Socket ||
				strings.Join(got.Includes, "|") != strings.Join(c.want.Includes, "|") {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"group never closed", "[client\nuser=u\n", "line 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.in), "client")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error naming %q", err, c.want)
			}
		})
	}
}

// An error names the line by number and never quotes it.
func TestParseErrorDoesNotLeakTheLine(t *testing.T) {
	_, err := Parse([]byte("[client]\n[hunter2\n"), "client")
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("error must exist and not carry the line: %v", err)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "my.cnf")
	if err := os.WriteFile(p, []byte("[client]\nuser=u\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(p, "client")
	if err != nil || got.User != "u" {
		t.Fatalf("Load = %+v, %v", got, err)
	}
	missing := filepath.Join(dir, "nope.cnf")
	if _, err := Load(missing, "client"); err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("a missing file must be an error naming it, got %v", err)
	}
	if _, err := Load(dir, "client"); err == nil {
		t.Fatal("a directory must be an error")
	}
}

func TestRead(t *testing.T) {
	got, err := Read(strings.NewReader("[client]\nuser=u\npassword=p\n"), "on stdin", "client")
	if err != nil || got.User != "u" || got.Password != "p" {
		t.Fatalf("Read = %+v, %v", got, err)
	}
	if _, err := Read(strings.NewReader("[client\n"), "on stdin", "client"); err == nil || !strings.Contains(err.Error(), "on stdin") {
		t.Fatalf("a broken stream must be an error naming it, got %v", err)
	}
	// Exactly at the bound is read; one byte over is refused, not cut: a cut
	// file would lose whatever group came last, without a word.
	pad := strings.Repeat("#", maxOptionFile-len("[client]\nuser=u\n")-1) + "\n"
	if got, err := Read(strings.NewReader(pad+"[client]\nuser=u\n"), "x", "client"); err != nil || got.User != "u" {
		t.Fatalf("at the bound: %+v, %v", got, err)
	}
	if _, err := Read(strings.NewReader("#"+pad+"[client]\nuser=u\n"), "x", "client"); err == nil {
		t.Fatal("one byte over the bound was read")
	}
}

func TestOptions(t *testing.T) {
	data := "\xef\xbb\xbfstray = 1\n!include /x.cnf\n[client]\nuser = u\n[ DBTrail ] # main\nCommit_Within = 2s # c\nloose-Batch_Size='5'\nultrafast # a switch\n= 3\n[other]\nschemas = a\n[dbtrail]\nschemas = \"a, b\"\r\n"
	got, err := Options([]byte(data), "dbtrail")
	if err != nil {
		t.Fatal(err)
	}
	want := []Option{
		{Name: "commit-within", HasValue: true, Value: "2s", CommentCut: true, Line: 6},
		{Name: "batch-size", Loose: true, HasValue: true, Value: "5", Line: 7},
		{Name: "ultrafast", Line: 8},
		{Name: "", HasValue: true, Value: "3", Line: 9},
		{Name: "schemas", HasValue: true, Value: "a, b", Line: 13},
	}
	if len(got.Options) != len(want) {
		t.Fatalf("got %+v", got.Options)
	}
	for i := range want {
		if got.Options[i] != want[i] {
			t.Errorf("option %d = %+v, want %+v", i, got.Options[i], want[i])
		}
	}
	if len(got.Ungrouped) != 1 || got.Ungrouped[0] != 1 {
		t.Errorf("ungrouped = %v", got.Ungrouped)
	}
	if len(got.Includes) != 1 || got.Includes[0] != 2 {
		t.Errorf("includes = %v", got.Includes)
	}
}

func TestOptions_brokenGroupNamesTheLineOnly(t *testing.T) {
	_, err := Options([]byte("[dbtrail]\n[client password=hunter2\n"), "dbtrail")
	if err == nil || err.Error() != "line 2: a group name is missing its closing ]" {
		t.Fatalf("err = %v", err)
	}
}
