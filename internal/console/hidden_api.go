package console

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/views"
)

// hiddenBootRefusal is why the command-line server has no hidden rules: it is
// never written to the registry, so there is nowhere to keep them (#2529).
const hiddenBootRefusal = "the command-line server cannot hide tables or columns; " +
	"add the server from the web interface (+ Add server) to set rules for it"

// hiddenDTO is GET and PUT /api/servers/{id}/hidden. The lists are never null
// on the wire, so a reader can iterate them without a check.
type hiddenDTO struct {
	Tables  []string `json:"tables"`
	Columns []string `json:"columns"`
	// Editable is false on the command-line server and on a registry this
	// build opened read-only; Refusal says why.
	Editable bool   `json:"editable"`
	Refusal  string `json:"refusal,omitempty"`
	// UnreadableKeys are rules a newer build saved that this build cannot
	// read. While there are any, the analytical surfaces refuse this
	// server's statements (hiderules.Load), so the lists alone do not tell
	// the whole story.
	UnreadableKeys []string `json:"unreadable_keys,omitempty"`
	// Refused is why the analytical surfaces refuse this server's
	// statements: the hiderules.Load error, set whenever the stored rules
	// cannot be enforced (keys a newer build wrote, or a hand edit that does
	// not parse). Empty when the rules load.
	Refused string `json:"refused,omitempty"`
	// Unmatched are the rules that match no table or column of the copy's
	// newest snapshot (#2529). Not proof of a typo: a table or column added
	// after that snapshot lives only in the change history, where the rule
	// still hides it. Present (possibly empty) only when the rules were checked
	// against the copy; a table whose columns are unknown counts as a
	// possible match, so an entry is never called unmatched on a guess.
	Unmatched *[]string `json:"unmatched,omitempty"`
	// Unchecked says why the rules were not checked against the copy (no
	// snapshot on this host, or one only in S3). Set only when there are
	// rules to check.
	Unchecked string `json:"unmatched_unchecked,omitempty"`
}

// hiddenListsDTO is the rules on the server list: both lists, never null,
// absent when the server hides nothing.
type hiddenListsDTO struct {
	Tables  []string `json:"tables"`
	Columns []string `json:"columns"`
	// Unreadable: the stored rules cannot be enforced (keys a newer build
	// wrote, or a hand edit that does not parse), so the analytical surfaces
	// refuse this server. Present even when both lists are empty: a server
	// whose rules are only a newer build's key still hides something.
	Unreadable bool `json:"unreadable,omitempty"`
}

// loadedHiddenLists is what the stored rules hide, read the way the
// surfaces enforce them (hiderules.Load): the canonical lists when they load
// (blank entries gone), the stored lists as written when they do not, with
// the load error.
func loadedHiddenLists(r *hiderules.Rules) (tables, columns []string, set *hiderules.Set, lerr error) {
	tables, columns = []string{}, []string{}
	set, lerr = hiderules.Load(r)
	switch {
	case lerr != nil:
		tables = append(tables, r.Tables...)
		columns = append(columns, r.Columns...)
	case set != nil:
		canon := set.Rules()
		tables = append(tables, canon.Tables...)
		columns = append(columns, canon.Columns...)
	}
	return tables, columns, set, lerr
}

func newHiddenListsDTO(r *hiderules.Rules) *hiddenListsDTO {
	if r == nil {
		return nil
	}
	tables, columns, _, lerr := loadedHiddenLists(r)
	if len(tables) == 0 && len(columns) == 0 && lerr == nil {
		return nil
	}
	return &hiddenListsDTO{Tables: tables, Columns: columns, Unreadable: lerr != nil}
}

// newHiddenDTO is GET and PUT's answer for stored rules r, with the set
// they load as (nil when they do not load).
func newHiddenDTO(r *hiderules.Rules) (hiddenDTO, *hiderules.Set) {
	d := hiddenDTO{Editable: true}
	var set *hiderules.Set
	var lerr error
	d.Tables, d.Columns, set, lerr = loadedHiddenLists(r)
	if lerr != nil {
		d.Refused = lerr.Error()
	}
	d.UnreadableKeys = r.UnknownKeys()
	return d, set
}

// hiddenCopyBundle is the bundle whose copy SQL on the copy reads for id,
// without opening an index connection: the one already open, the boot
// bundle when it serves this id, else one derived from the entry (reading
// snapshots needs no database).
func (s *Server) hiddenCopyBundle(id string) *bundle {
	s.cm.mu.Lock()
	b := s.cm.bundles[id]
	if b == nil && s.cm.boot != nil && s.cm.boot.serverID == id {
		b = s.cm.boot
	}
	s.cm.mu.Unlock()
	if b != nil {
		return b
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		return nil
	}
	return s.cm.newBundle(nil, "", e)
}

// checkHiddenAgainstCopy fills d.Unmatched, or d.Unchecked when the copy
// cannot be read here. Only a snapshot on this host is read: a GET must not
// wait on an S3 listing.
func (s *Server) checkHiddenAgainstCopy(ctx context.Context, id string, d *hiddenDTO, set *hiderules.Set) {
	if set.Empty() {
		return
	}
	const why = "could not check these rules against the copy: "
	b := s.hiddenCopyBundle(id)
	src := b.sqlCopySource()
	switch {
	case src == "":
		d.Unchecked = why + "this server has no snapshot location"
		return
	case strings.HasPrefix(src, "s3://"):
		d.Unchecked = why + "its snapshots are only in S3"
		return
	}
	in, err := s.buildViewsInputAll(ctx, b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true})
	if err != nil && !errors.Is(err, errNoViewSources) {
		slog.Warn("hidden rules: could not read the copy to check them", "server", id, "error", err)
		d.Unchecked = why + "this server's snapshot on this host could not be read: " + err.Error()
		return
	}
	if err != nil || len(in.Baselines) == 0 {
		d.Unchecked = why + "there is no snapshot of this server on this host yet"
		return
	}
	out := hiddenUnmatched(set, in.Baselines)
	d.Unmatched = &out
}

// hiddenUnmatched returns the canonical entries of set that match no table
// (or no column of a matching table) in tables. A table whose columns are
// not known counts as holding every column.
func hiddenUnmatched(set *hiderules.Set, tables []views.BaselineTable) []string {
	out := []string{}
	canon := set.Rules()
	for _, entry := range canon.Tables {
		one, err := hiderules.Parse(hiderules.Rules{Tables: []string{entry}})
		if err != nil {
			continue
		}
		if !slices.ContainsFunc(tables, func(t views.BaselineTable) bool { return one.TableHidden(t.Schema, t.Table) }) {
			out = append(out, entry)
		}
	}
	for _, entry := range canon.Columns {
		one, err := hiderules.Parse(hiderules.Rules{Columns: []string{entry}})
		if err != nil {
			continue
		}
		hit := slices.ContainsFunc(tables, func(t views.BaselineTable) bool {
			if len(one.HiddenColumns(t.Schema, t.Table)) == 0 {
				return false
			}
			if !t.SchemaKnown || len(t.Columns) == 0 {
				return true
			}
			return slices.ContainsFunc(t.Columns, func(c string) bool { return one.ColumnHidden(t.Schema, t.Table, c) })
		})
		if !hit {
			out = append(out, entry)
		}
	}
	return out
}

// handleHiddenGet serves GET /api/servers/{id}/hidden.
func (s *Server) handleHiddenGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		d, _ := newHiddenDTO(nil)
		d.Editable, d.Refusal = false, hiddenBootRefusal
		writeJSON(w, http.StatusOK, d)
		return
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	d, set := newHiddenDTO(e.Hidden)
	s.checkHiddenAgainstCopy(r.Context(), id, &d, set)
	if s.cm.reg.ReadOnly() {
		d.Editable, d.Refusal = false, ErrRegistryReadOnly.Error()
	}
	writeJSON(w, http.StatusOK, d)
}

// handleHiddenPut serves PUT /api/servers/{id}/hidden: the body replaces both
// lists. Empty lists remove the rules. A refused entry saves nothing, and the
// error names every refused entry.
func (s *Server) handleHiddenPut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, hiddenBootRefusal)
		return
	}
	if _, ok := s.cm.reg.Get(id); !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	// Strict on purpose: a typo'd key, a wrong nesting or a null would decode
	// to empty lists, and empty lists REMOVE every rule. Clearing must be
	// said with both lists present and empty.
	var body struct {
		Tables  *[]string `json:"tables"`
		Columns *[]string `json:"columns"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	if dec.More() {
		writeJSONError(w, http.StatusBadRequest, "the body holds more than one JSON value")
		return
	}
	if body.Tables == nil || body.Columns == nil {
		writeJSONError(w, http.StatusBadRequest, `send both "tables" and "columns"; empty lists remove the rules`)
		return
	}
	req := hiderules.Rules{Tables: *body.Tables, Columns: *body.Columns}
	set, err := hiderules.Parse(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	e, err := s.cm.reg.SetHidden(id, set)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	saved := set.Rules()
	slog.Info("console: hidden rules saved", "actor", consoleActor(r), "server", e.Name, "id", id,
		"tables", strings.Join(saved.Tables, ","), "columns", strings.Join(saved.Columns, ","))
	d, loaded := newHiddenDTO(e.Hidden)
	s.checkHiddenAgainstCopy(r.Context(), id, &d, loaded)
	writeJSON(w, http.StatusOK, d)
}
