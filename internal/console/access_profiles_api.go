package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/accessprofiles"
)

// The Access profiles page (#1445): the console authors the flags,
// profiles and rules that `--profile` and a session's data profile enforce.
// This is a write to the SELECTED SERVER'S INDEX DATABASE, a class of
// console write the registry file and the rotation override are not, and it
// is allowed under four rules, all pinned by tests:
//
//   - every mutation needs settings:write (the permission that already
//     governs console administration), reads need settings:read;
//   - the validation and the SQL are internal/accessprofiles, the SAME code
//     the CLI verbs run, so a profile authored here is the rows the CLI
//     would write and is refused with the CLI's words;
//   - the target is the selected server (X-Bintrail-Server), the boot entry
//     included: it is a real index;
//   - every mutation is audited (console/flag.add, ... access.remove).
//
// One more, which the permission alone does not give: while an
// access-control profile is active, the whole surface (the GET included) is
// refused, keyed on profileActiveFor: a NAMED startup profile even when it
// has no rules yet (rbacActiveFor, the cascade/verify floor, is false for
// a zero-rule profile, and a fresh index under `serve --profile` is
// exactly where the first rule would be authored), a startup profile with
// rules, and a session that carries a data profile or restrictions. A
// console started under --profile does not rewrite the rows that profile
// is built from, and a profiled session could lift its own redaction. The
// GET is refused too because the flagged tables and columns are exactly
// what such a profile withholds (GET /api/profiles hands out names only,
// at the same tier).

// accessProfilesDoc is the whole configuration in one document: what GET
// returns and what every mutation returns once it has applied, so the page
// always repaints from the server's own view and never from what it thinks
// it just did.
type accessProfilesDoc struct {
	Flags    []accessFlagDTO    `json:"flags"`
	Profiles []accessProfileDTO `json:"profiles"`
	Rules    []accessRuleDTO    `json:"rules"`
}

type accessFlagDTO struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Column    string `json:"column"`
	Flag      string `json:"flag"`
	CreatedAt string `json:"created_at"`
}

type accessProfileDTO struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
}

type accessRuleDTO struct {
	Profile    string `json:"profile"`
	Flag       string `json:"flag"`
	Permission string `json:"permission"`
	CreatedAt  string `json:"created_at"`
}

// Request bodies. Flag add/remove share one; so do profile and rule.
type accessFlagRequest struct {
	Flag   string `json:"flag"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Column string `json:"column"`
}

type accessProfileRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type accessRuleRequest struct {
	Profile    string `json:"profile"`
	Flag       string `json:"flag"`
	Permission string `json:"permission"`
}

// accessProfilesRefusal is the message a data-profile session gets on every
// route of this surface; accessProfilesStartupRefusal the one a console
// started under a profile gives (see the file comment).
const (
	accessProfilesRefusal        = "your session has a data profile, so it cannot view or change access profiles; sign in with an account that has none"
	accessProfilesStartupRefusal = "access profiles are not available while an access-control profile is active (CLI: --profile); the web interface does not edit the rows that profile is built from"
	// accessReadbackFailedPrefix opens the error a mutation answers with
	// when the write landed and only the readback failed, so the operator
	// does not repeat a change that is already in.
	accessReadbackFailedPrefix = "The change was saved but the page could not be re-read: "
)

// accessProfilesGate refuses the request while a profile is active (see the
// file comment) and reports whether the handler may go on. A session
// refusal is audited as profile.denied, like the other profile gates.
func (s *Server) accessProfilesGate(w http.ResponseWriter, r *http.Request) bool {
	if !s.profileActiveFor(r) {
		return true
	}
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "access_profiles")
		writeJSONError(w, http.StatusForbidden, accessProfilesRefusal)
		return false
	}
	writeJSONError(w, http.StatusForbidden, accessProfilesStartupRefusal)
	return false
}

// loadAccessProfilesDoc reads the three tables of the selected index.
func loadAccessProfilesDoc(ctx context.Context, db accessprofiles.DBExecer) (accessProfilesDoc, error) {
	doc := accessProfilesDoc{Flags: []accessFlagDTO{}, Profiles: []accessProfileDTO{}, Rules: []accessRuleDTO{}}
	flags, err := accessprofiles.ListFlags(ctx, db, "", "")
	if err != nil {
		return doc, err
	}
	for _, f := range flags {
		doc.Flags = append(doc.Flags, accessFlagDTO{Schema: f.Schema, Table: f.Table, Column: f.Column, Flag: f.Name,
			CreatedAt: f.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	profiles, err := accessprofiles.ListProfiles(ctx, db)
	if err != nil {
		return doc, err
	}
	for _, p := range profiles {
		doc.Profiles = append(doc.Profiles, accessProfileDTO{Name: p.Name, Description: p.Description,
			CreatedAt: p.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	rules, err := accessprofiles.ListRules(ctx, db, "")
	if err != nil {
		return doc, err
	}
	for _, r := range rules {
		doc.Rules = append(doc.Rules, accessRuleDTO{Profile: r.Profile, Flag: r.Flag, Permission: r.Permission,
			CreatedAt: r.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	return doc, nil
}

// writeAccessProfilesError maps a shared-package refusal onto a status: bad
// input is 400, a row that is not there is 404, a profile or flag that
// exists under another spelling is 409, an index without the three tables is 422 (it
// predates them; nothing here can create them), anything else is the
// database's own error at 500.
func writeAccessProfilesError(w http.ResponseWriter, err error) {
	var myErr *mysql.MySQLError
	switch {
	case accessprofiles.IsNotFound(err):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case accessprofiles.IsConflict(err):
		writeJSONError(w, http.StatusConflict, err.Error())
	case accessprofiles.IsRefusal(err):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.As(err, &myErr) && myErr.Number == 1146:
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index has no access profile tables; it was created before they existed, so access profiles cannot be stored on it")
	default:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleAccessProfilesGet serves GET /api/access-profiles for the selected
// server. Configuration, not row data: flag names, table and column names,
// profile names and their rules.
func (s *Server) handleAccessProfilesGet(w http.ResponseWriter, r *http.Request) {
	if !s.accessProfilesGate(w, r) {
		return
	}
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	doc, err := loadAccessProfilesDoc(r.Context(), b.db)
	if err != nil {
		writeAccessProfilesError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

// accessMutation is one verb's body: apply it to the selected index and
// report what to audit (the schema and table the change names, when it
// names one, plus the fields that identify it).
type accessMutation func(ctx context.Context, db accessprofiles.DBExecer) (schema, table string, detail map[string]string, err error)

// mutateAccessProfiles is the shared shape of the six verbs: refuse while a
// profile is active, resolve the server, run the shared code, drop the
// cached profile rules for that server (a profiled session's next request
// must see the change, not the 30-second cache), audit, and answer with the
// fresh document.
func (s *Server) mutateAccessProfiles(w http.ResponseWriter, r *http.Request, action string, run accessMutation) {
	if !s.accessProfilesGate(w, r) {
		return
	}
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	schema, table, detail, err := run(r.Context(), b.db)
	if err != nil {
		writeAccessProfilesError(w, err)
		return
	}
	target := s.selectedID(r)
	s.sessionProfiles.invalidate(target)
	if detail == nil {
		detail = map[string]string{}
	}
	// Always name the index the change landed on: recordConsoleAccess
	// records the header only when one was sent, and a change made under
	// the default selection is a change to a real index all the same.
	detail["server"] = target
	recordConsoleAccess(r, action, schema, table, detail)
	doc, err := loadAccessProfilesDoc(r.Context(), b.db)
	if err != nil {
		// The change is in and audited; only the readback failed. The body
		// says so, so the operator reloads rather than repeats the change.
		writeJSONError(w, http.StatusInternalServerError, accessReadbackFailedPrefix+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func decodeAccessBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeBodyDecodeError(w, err)
		return false
	}
	return true
}

func flagDetail(f accessprofiles.Flag) map[string]string {
	return map[string]string{"flag": f.Name, "column": f.Column}
}

// handleAccessFlagAdd serves POST /api/access-profiles/flags.
func (s *Server) handleAccessFlagAdd(w http.ResponseWriter, r *http.Request) {
	var req accessFlagRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	// Trimmed here as well as in the writer, so the audit detail and the
	// Schema/Table on the event name the row as stored.
	f := accessprofiles.Flag{Schema: req.Schema, Table: req.Table, Column: req.Column, Name: req.Flag}.Trimmed()
	s.mutateAccessProfiles(w, r, "flag.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return f.Schema, f.Table, flagDetail(f), accessprofiles.AddFlag(ctx, db, f)
	})
}

// handleAccessFlagRemove serves POST /api/access-profiles/flags/remove. A
// POST with a body rather than a DELETE: a flag's key is four values, which
// have no place in a path.
func (s *Server) handleAccessFlagRemove(w http.ResponseWriter, r *http.Request) {
	var req accessFlagRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	f := accessprofiles.Flag{Schema: req.Schema, Table: req.Table, Column: req.Column, Name: req.Flag}.Trimmed()
	s.mutateAccessProfiles(w, r, "flag.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return f.Schema, f.Table, flagDetail(f), accessprofiles.RemoveFlag(ctx, db, f)
	})
}

// handleAccessProfileAdd serves POST /api/access-profiles/profiles.
func (s *Server) handleAccessProfileAdd(w http.ResponseWriter, r *http.Request) {
	var req accessProfileRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	p := accessprofiles.Profile{Name: req.Name, Description: req.Description}.Trimmed()
	s.mutateAccessProfiles(w, r, "profile.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": p.Name}, accessprofiles.AddProfile(ctx, db, p)
	})
}

// handleAccessProfileRemove serves POST /api/access-profiles/profiles/remove.
// The profile's rules go with it (the table's foreign key cascades), which
// the page says before asking.
func (s *Server) handleAccessProfileRemove(w http.ResponseWriter, r *http.Request) {
	var req accessProfileRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	name := strings.TrimSpace(req.Name)
	s.mutateAccessProfiles(w, r, "profile.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": name}, accessprofiles.RemoveProfile(ctx, db, name)
	})
}

// handleAccessRuleAdd serves POST /api/access-profiles/rules. Adding a rule
// for a pair that has one replaces its permission, as the CLI does.
func (s *Server) handleAccessRuleAdd(w http.ResponseWriter, r *http.Request) {
	var req accessRuleRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	rule := accessprofiles.Rule{Profile: req.Profile, Flag: req.Flag, Permission: req.Permission}.Trimmed()
	s.mutateAccessProfiles(w, r, "access.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": rule.Profile, "flag": rule.Flag, "permission": rule.Permission},
			accessprofiles.AddRule(ctx, db, rule)
	})
}

// handleAccessRuleRemove serves POST /api/access-profiles/rules/remove.
func (s *Server) handleAccessRuleRemove(w http.ResponseWriter, r *http.Request) {
	var req accessRuleRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	profile, flag := strings.TrimSpace(req.Profile), strings.TrimSpace(req.Flag)
	s.mutateAccessProfiles(w, r, "access.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": profile, "flag": flag},
			accessprofiles.RemoveRule(ctx, db, profile, flag)
	})
}
