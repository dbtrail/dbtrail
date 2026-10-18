package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// MetaKeyPGRelation records, on a PostgreSQL-source baseline, the table's OID
// and its column list with types, read from the catalog inside the snapshot
// transaction (#1596 slice 1). The value is PGRelation as JSON.
//
// The column list is the one the file stores (live, not dropped, not
// generated, attnum order). On PostgreSQL 14-17 that is also the column list a
// pgoutput Relation message carries, so a later shape can be compared against
// it by (name, type_oid, typmod), and the OID survives a rename (#2441). On
// PostgreSQL 18 a publication created WITH (publish_generated_columns =
// stored) also streams STORED generated columns, which this list leaves out:
// a comparison must account for that before it reads the difference as DDL.
//
// Absent on MySQL/MariaDB baselines and on PostgreSQL baselines written before
// this key existed.
const MetaKeyPGRelation = "bintrail.pg_relation"

// PGRelation is the value of MetaKeyPGRelation.
//
// PrimaryKey and Generated, and the NotNull, Default, Identity, Sequence and
// Ordinal of each column, are what a CREATE TABLE needs beyond the column
// list (#1596 slice 2), read in the same transaction. Absent on snapshots
// written before they were recorded.
type PGRelation struct {
	OID     uint32     `json:"oid"`
	Columns []PGColumn `json:"columns"`
	// PrimaryKey names the primary key's columns in key order; empty when
	// the table has none or it was not recorded.
	PrimaryKey []string `json:"primary_key,omitempty"`
	// Generated are the STORED generated columns, which the file does not
	// hold (PostgreSQL computes them again on load).
	Generated []PGGeneratedColumn `json:"generated,omitempty"`
}

// PGGeneratedColumn is one STORED generated column: its type as format_type
// spells it and its generation expression as pg_get_expr spells it.
type PGGeneratedColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Expr    string `json:"expr"`
	NotNull bool   `json:"not_null,omitempty"`
	Ordinal int    `json:"ordinal,omitempty"`
}

// PGColumn is one column of a PGRelation. TypeOID and TypeMod are
// pg_attribute.atttypid and atttypmod (-1 = no modifier), the same numbers a
// pgoutput Relation message carries; Type is format_type() of the two, the
// human-readable spelling a CREATE TABLE needs.
type PGColumn struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
	TypeMod int32  `json:"typmod"`
	Type    string `json:"type,omitempty"`
	// NotNull is attnotnull.
	NotNull bool `json:"not_null,omitempty"`
	// Default is the default expression as pg_get_expr spells it; "" none.
	Default string `json:"default,omitempty"`
	// Identity is attidentity: "a" GENERATED ALWAYS, "d" BY DEFAULT, ""
	// not an identity column.
	Identity string `json:"identity,omitempty"`
	// Sequence is the sequence the column owns (a serial or identity
	// column), schema-qualified and quoted as pg_get_serial_sequence
	// returns it; "" none.
	Sequence string `json:"sequence,omitempty"`
	// Ordinal is attnum: where the column sits among all of the table's,
	// generated ones included. 0 = not recorded.
	Ordinal int `json:"ordinal,omitempty"`
}

// Encode returns the footer value.
func (r PGRelation) Encode() (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encode %s: %w", MetaKeyPGRelation, err)
	}
	return string(b), nil
}

// ParsePGRelation decodes a MetaKeyPGRelation value. ok is false, with no
// error, when the value is empty (the key is absent). A value that is present
// but does not describe a table (bad JSON, zero OID, no columns, an empty or
// repeated column name, a zero type OID) is an error, never a partial shape:
// a consumer comparing shapes must not compare against a wrong one.
func ParsePGRelation(s string) (r PGRelation, ok bool, err error) {
	if strings.TrimSpace(s) == "" {
		return PGRelation{}, false, nil
	}
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return PGRelation{}, false, fmt.Errorf("parse %s: %w", MetaKeyPGRelation, err)
	}
	if r.OID == 0 {
		return PGRelation{}, false, errors.New("parse " + MetaKeyPGRelation + ": oid is zero")
	}
	if len(r.Columns) == 0 {
		return PGRelation{}, false, errors.New("parse " + MetaKeyPGRelation + ": no columns")
	}
	seen := make(map[string]bool, len(r.Columns))
	for i, c := range r.Columns {
		if c.Name == "" {
			return PGRelation{}, false, fmt.Errorf("parse %s: column %d has no name", MetaKeyPGRelation, i+1)
		}
		if c.TypeOID == 0 {
			return PGRelation{}, false, fmt.Errorf("parse %s: column %q has no type oid", MetaKeyPGRelation, c.Name)
		}
		// PostgreSQL identifiers are case-sensitive once stored, so the
		// comparison is exact.
		if seen[c.Name] {
			return PGRelation{}, false, fmt.Errorf("parse %s: column %q appears twice", MetaKeyPGRelation, c.Name)
		}
		seen[c.Name] = true
		if c.Identity != "" && c.Identity != "a" && c.Identity != "d" {
			return PGRelation{}, false, fmt.Errorf("parse %s: column %q has identity %q", MetaKeyPGRelation, c.Name, c.Identity)
		}
	}
	for i, g := range r.Generated {
		switch {
		case g.Name == "":
			return PGRelation{}, false, fmt.Errorf("parse %s: generated column %d has no name", MetaKeyPGRelation, i+1)
		case seen[g.Name]:
			return PGRelation{}, false, fmt.Errorf("parse %s: column %q appears twice", MetaKeyPGRelation, g.Name)
		case g.Type == "" || g.Expr == "":
			return PGRelation{}, false, fmt.Errorf("parse %s: generated column %q has no type or expression", MetaKeyPGRelation, g.Name)
		}
		seen[g.Name] = true
	}
	inKey := make(map[string]bool, len(r.PrimaryKey))
	for _, k := range r.PrimaryKey {
		if inKey[k] {
			return PGRelation{}, false, fmt.Errorf("parse %s: primary key names %q twice", MetaKeyPGRelation, k)
		}
		inKey[k] = true
		if !slices.ContainsFunc(r.Columns, func(c PGColumn) bool { return c.Name == k }) {
			return PGRelation{}, false, fmt.Errorf("parse %s: primary key column %q is not a stored column", MetaKeyPGRelation, k)
		}
	}
	return r, true, nil
}
