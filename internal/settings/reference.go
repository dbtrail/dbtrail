package settings

import (
	"fmt"
	"regexp"
	"strings"
)

// The two files generated from docs (#2671). Both are pure functions of the
// table and its words, so a test can say when a committed file is behind.

// OptionFileSamplePath and ReferencePath are where the generated files are
// kept, from the root of the repository.
const (
	OptionFileSamplePath = "build/packaging/bintrail-console/dbtrail.cnf"
	ReferencePath        = "docs/option-reference.md"
)

// variableName finds a BINTRAIL_ variable named in a text.
var variableName = regexp.MustCompile(`BINTRAIL_[A-Z0-9_]+`)

// asOptions writes every operator setting a text names by its variable
// (BINTRAIL_SSL_CERT) by its option name (ssl-cert), as the option file
// writes it. Other names are left as they are.
func asOptions(text string) string {
	return variableName.ReplaceAllStringFunc(text, func(v string) string {
		if e, ok := Lookup(v); ok && e.Kind == Setting {
			return e.Option()
		}
		return v
	})
}

// wrap breaks text into lines of at most width characters, each starting
// with prefix. A word longer than the line is put on a line of its own.
func wrap(text, prefix string, width int) []string {
	var lines []string
	line := ""
	for _, w := range strings.Fields(text) {
		if line != "" && len(prefix)+len(line)+1+len(w) > width {
			lines = append(lines, prefix+line)
			line = ""
		}
		if line == "" {
			line = w
		} else {
			line += " " + w
		}
	}
	if line != "" {
		lines = append(lines, prefix+line)
	}
	return lines
}

// optionValue is an example as a line of the option file writes it: in
// single quotes when the value has a character the file would read as
// something else (# or ; starts a comment, a quote, spaces at its ends), or
// no value at all.
func optionValue(v string) string {
	if v == "" || strings.ContainsAny(v, "#;'\"") || strings.TrimSpace(v) != v {
		return "'" + strings.ReplaceAll(v, "'", `\'`) + "'"
	}
	return v
}

// operatorSettings is every Setting of the table, in the table's order
// within each group, groups in Groups' order.
func operatorSettings() [][]Entry {
	byGroup := map[string][]Entry{}
	for _, e := range table {
		if e.Kind != Setting {
			continue
		}
		d := docs[e.Env]
		byGroup[d.Group] = append(byGroup[d.Group], e)
	}
	out := make([][]Entry, 0, len(Groups))
	for _, g := range Groups {
		out = append(out, byGroup[g.ID])
	}
	return out
}

// readByMCPOnly: bintrail-mcp does not read the option file, so a setting
// only it reads does nothing there.
func readByMCPOnly(d Doc) bool { return strings.TrimSpace(d.ReadBy) == "bintrail-mcp" }

// OptionFileSample is the option file the packages install at
// /etc/dbtrail/dbtrail.cnf: every option, explained, and commented out.
func OptionFileSample() string {
	var b strings.Builder
	p := func(s string) { b.WriteString(s + "\n") }
	for _, l := range []string{
		"DBTrail's option file. Every DBTrail program reads it when it starts, except bintrail-mcp, which takes its settings from the AI client that starts it.",
		"Every option below is commented out, so DBTrail uses its defaults. To set one, remove the # in front of it and write your value. A program reads the file when it starts: restart it after a change. A line DBTrail does not understand is ignored, with a warning; dbtrail config check lists them, and dbtrail config show lists what is in effect and where each value comes from.",
		"What comes first, when a setting is given in several places: a command-line flag, then the environment (BINTRAIL_ and the option's name in capitals, with _ for -), then .bintrail.env or ~/.config/bintrail/config.env, then this file. Some settings can also be saved in the web interface; where they can, the web interface says which value it is using and where it comes from.",
		"Write a value in single quotes when it has a # or a ; in it, which would otherwise start a comment. This file can hold passwords: keep it readable by DBTrail only (chmod 640, owned by root and DBTrail's group). A file every user can change is not used at all.",
		"Generated from DBTrail's list of settings (internal/settings); docs/option-reference.md has the same, as a page.",
	} {
		for _, w := range wrap(l, "# ", 78) {
			p(w)
		}
		p("#")
	}
	p("[dbtrail]")
	for i, group := range operatorSettings() {
		if len(group) == 0 {
			continue
		}
		p("")
		p("# " + strings.Repeat("=", 76))
		p("# " + Groups[i].Title)
		p("# " + strings.Repeat("=", 76))
		for _, e := range group {
			d := docs[e.Env]
			p("")
			for _, w := range wrap(asOptions(d.Text), "# ", 78) {
				p(w)
			}
			for _, w := range wrap("Default: "+asOptions(d.Default)+".", "# ", 78) {
				p(w)
			}
			readBy := "Read by: " + d.ReadBy + "."
			if strings.Contains(d.ReadBy, "bintrail-mcp") {
				readBy += " bintrail-mcp takes it from its own environment, never from this file."
			}
			if readByMCPOnly(d) {
				readBy = "Read by bintrail-mcp only, which does not read this file: set it as " + e.Env + " in the AI client's configuration."
			}
			for _, w := range wrap(readBy, "# ", 78) {
				p(w)
			}
			if e.Secret {
				p("# It holds a secret: keep this file readable by DBTrail only.")
			}
			p("#" + e.Option() + " = " + optionValue(d.Example))
		}
	}
	return b.String()
}

// mdText is text for the page: its own words, with < and > that a reader
// would take for an HTML tag written as they are seen.
func mdText(s string) string {
	return strings.NewReplacer("<", "&lt;", ">", "&gt;").Replace(s)
}

// Reference is docs/option-reference.md: every option, by group.
func Reference() string {
	var b strings.Builder
	p := func(f string, a ...any) { fmt.Fprintf(&b, f+"\n", a...) }
	p("# Every option of dbtrail.cnf")
	p("")
	p("<!-- Generated from internal/settings (docs.go). Do not edit by hand: change docs.go, then run")
	p("     go test ./internal/settings -run TestGeneratedFilesAreCurrent -update -->")
	p("")
	p("Every setting DBTrail reads, as a line of [`/etc/dbtrail/dbtrail.cnf`](option-file.md) writes it. The same setting can be given as an environment variable, named under each option. A command-line flag comes first, then the environment, then `.bintrail.env` or `~/.config/bintrail/config.env`, then the file. `dbtrail config show` lists what is in effect and where each value comes from.")
	p("")
	p("`bintrail-mcp` does not read the file: an option it alone reads is set in the environment the AI client starts it with.")
	for i, group := range operatorSettings() {
		if len(group) == 0 {
			continue
		}
		p("")
		p("## %s", Groups[i].Title)
		for _, e := range group {
			d := docs[e.Env]
			p("")
			p("### `%s`", e.Option())
			p("")
			p("%s", mdText(d.Text))
			p("")
			p("- Default: %s.", mdText(d.Default))
			p("- Example: `%s = %s`", e.Option(), optionValue(d.Example))
			p("- Environment variable: `%s`", e.Env)
			readBy := mdText(d.ReadBy) + "."
			if strings.Contains(d.ReadBy, "bintrail-mcp") {
				readBy += " `bintrail-mcp` takes it from its own environment, never from the file."
			}
			p("- Read by: %s", readBy)
			if e.Secret {
				p("- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.")
			}
		}
	}
	return b.String()
}
