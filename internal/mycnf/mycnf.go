// Package mycnf reads the connection options of a MySQL option file, the
// format of ~/.my.cnf: host, port, user and password under [client].
//
// It reads one file and nothing else. It does not look for the files the
// mysql client reads by itself (/etc/my.cnf, ~/.my.cnf), and it does not
// follow !include or !includedir: a caller names the file, so what was read
// is never a surprise. The include lines it met are reported in Client.Includes.
package mycnf

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

// Client is what an option file says about how to connect. A field is empty
// when the file does not set it.
type Client struct {
	Host     string
	Port     string
	User     string
	Password string
	// Socket is reported so a caller that cannot use one can say so, instead
	// of connecting somewhere the file did not mean.
	Socket string
	// Includes are the !include and !includedir lines of the file, as
	// written. They were not followed.
	Includes []string
}

// Load reads the file at path. See Parse for groups.
func Load(path string, groups ...string) (Client, error) {
	f, err := Open(path)
	if err != nil {
		return Client{}, err
	}
	return f.Client(groups...)
}

// File is an option file read once, whose groups are then read apart: a
// caller that needs two logins from one file (one per group) reads it once,
// which matters when it came on a stream that cannot be read again.
type File struct {
	name string
	data []byte
}

// Open reads the file at path.
func Open(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("read option file %s: %w", path, err)
	}
	return File{name: path, data: data}, nil
}

// ReadFile reads an option file from r. name is what errors call it.
func ReadFile(r io.Reader, name string) (File, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxOptionFile+1))
	if err != nil {
		return File{}, fmt.Errorf("read option file %s: %w", name, err)
	}
	if len(data) > maxOptionFile {
		return File{}, fmt.Errorf("option file %s is larger than 1 MB: that is not an option file", name)
	}
	return File{name: name, data: data}, nil
}

// Client reads the options of the named groups only. See Parse.
func (f File) Client(groups ...string) (Client, error) {
	return parseNamed(f.name, f.data, groups...)
}

// maxOptionFile bounds an option file read from a stream. A real one is a
// few hundred bytes.
const maxOptionFile = 1 << 20

// Read reads an option file from r, for a caller whose file is not on this
// machine's disk. name is what errors call it.
func Read(r io.Reader, name string, groups ...string) (Client, error) {
	f, err := ReadFile(r, name)
	if err != nil {
		return Client{}, err
	}
	return f.Client(groups...)
}

func parseNamed(path string, data []byte, groups ...string) (Client, error) {
	c, err := Parse(data, groups...)
	if err != nil {
		return Client{}, fmt.Errorf("option file %s: %w", path, err)
	}
	return c, nil
}

// Parse reads the options of the named groups. When two groups set the same
// option, the group named LATER in groups wins, wherever each sits in the
// file; inside one group the last line wins, as in MySQL. Group names are
// not case-sensitive.
//
// An error names the line by number and never quotes it: the line most likely
// to be malformed is the one that holds the password.
func Parse(data []byte, groups ...string) (Client, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	rank := func(group string) int {
		for i, g := range groups {
			if strings.EqualFold(g, group) {
				return i
			}
		}
		return -1
	}
	type setting struct {
		value string
		rank  int
		set   bool
	}
	opts := map[string]*setting{"host": {}, "port": {}, "user": {}, "password": {}, "socket": {}}

	var out Client
	current := -1
	for n, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '!' {
			out.Includes = append(out.Includes, line)
			continue
		}
		if line[0] == '[' {
			end := strings.IndexByte(line, ']')
			if end < 0 {
				return Client{}, fmt.Errorf("line %d: a group name is missing its closing ]", n+1)
			}
			current = rank(strings.TrimSpace(line[1:end]))
			continue
		}
		if current < 0 {
			continue
		}
		name, rest, _ := strings.Cut(line, "=")
		name = strings.ReplaceAll(strings.TrimSpace(name), "_", "-")
		// An option alone on its line ("password") may still carry a comment.
		if i := strings.IndexByte(name, '#'); i >= 0 {
			name = strings.TrimSpace(name[:i])
		}
		name = strings.TrimPrefix(name, "loose-")
		o, ok := opts[name]
		if !ok {
			continue
		}
		value := parseValue(rest)
		if o.set && current < o.rank {
			continue
		}
		o.value, o.rank, o.set = value, current, true
	}
	out.Host, out.Port, out.User = opts["host"].value, opts["port"].value, opts["user"].value
	out.Password, out.Socket = opts["password"].value, opts["socket"].value
	return out, nil
}

// parseValue reads what follows the = sign, the way the mysql client does:
// a # outside quotes starts a comment, the spaces around the value are
// dropped, then one pair of quotes is removed when the value both starts and
// ends with the same one, and only then are the escapes read. So a quote in
// the middle of a value is a character, an escaped space at the end
// survives, and a quote that is never closed is part of the value.
func parseValue(s string) string {
	s = strings.TrimLeft(s, " \t")
	end := len(s)
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '\\' && i+1 < len(s):
			i++
		case quote != 0:
			if ch == quote {
				quote = 0
			}
		case ch == '"' || ch == '\'':
			quote = ch
		case ch == '#':
			end = i
			i = len(s)
		}
	}
	s = strings.TrimRight(s[:end], " \t\r")
	if n := len(s); n >= 2 && (s[0] == '"' || s[0] == '\'') && s[n-1] == s[0] {
		s = s[1 : n-1]
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'b':
			b.WriteByte('\b')
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 's':
			b.WriteByte(' ')
		case '\\', '"', '\'':
			b.WriteByte(s[i])
		default: // not an escape MySQL knows: both characters stand
			b.WriteByte('\\')
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
