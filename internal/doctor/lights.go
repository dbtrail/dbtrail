package doctor

// The lights of the Connect screen's step 3 (#1953): each check belongs to one
// of a few plain questions, drawn as a vertical list, in this order. A check
// has a display name, not an id, so the grouping is keyed on the name here,
// beside the checks, and TestLightsCoverEveryCheck reads this package's code
// and fails on a check with no light: a new check is placed on purpose.
const (
	// LightReach: DBTrail reaches the server. The source connection check,
	// unless it failed on the password (then it is LightLogin's).
	LightReach = "reach"
	// LightLogin: the user logs in.
	LightLogin = "login"
	// LightRows: the change log keeps what capture needs (binary log on,
	// ROW, FULL row images, retention, crash safety).
	LightRows = "rows"
	// LightPermissions: the user may read what capture reads.
	LightPermissions = "permissions"
	// LightKeys: every table can be captured (a primary key, InnoDB).
	LightKeys = "keys"
	// LightOther: everything else, including checks an extension adds.
	LightOther = "other"
)

// Lights lists the lights in the order the screen draws them.
func Lights() []string {
	return []string{LightReach, LightLogin, LightRows, LightPermissions, LightKeys, LightOther}
}

var checkLights = map[string]string{
	SourceConnectionCheckName:                      LightReach,
	MariaDBVersionCheckName:                        LightReach,
	"log_bin enabled":                              LightRows,
	"binlog_format=ROW":                            LightRows,
	"binlog_row_image=FULL":                        LightRows,
	"binlog_row_value_options (JSON capture)":      LightRows,
	"Binlog retention >= 2 days":                   LightRows,
	"Source sync_binlog=1 (crash-safety)":          LightRows,
	"Statement capture (query_text)":               LightRows,
	"Schema-drift detection (binlog_row_metadata)": LightRows,
	ReplicationGrantsCheckName:                     LightPermissions,
	"Schema visibility":                            LightPermissions,
	SchemaAccessCheckName:                          LightPermissions,
	PrimaryKeyCheckName:                            LightKeys,
	InnoDBCheckName:                                LightKeys,
	"No FK CASCADE constraints":                    LightOther,
	"Replication server-id collision":              LightOther,
	// Index and storage checks: not run for a server being connected
	// (ForUnsavedServer), placed so a report that carries one still draws.
	IndexConnectionCheckName:       LightOther,
	"Index co-located with source": LightOther,
	IndexWriteAccessCheckName:      LightOther,
	"Index DSN parse":              LightOther,
	"Index DSN database name":      LightOther,
	"Index database":               LightOther,
	CapacityCheckName:              LightOther,
	SnapshotExpiryCheckName:        LightOther,
	ObjectLockCheckName:            LightOther,
	proxySQLRulesCheckName:         LightOther,
	ExtensionPanicCheckName:        LightOther,
	// PostgreSQL has its own report; its connection is still a connection.
	PGSourceConnectionCheckName: LightReach,
}

// LightFor places a check by its name and kind. A connection that failed on
// the user or password reached the server, so it belongs to LightLogin. A
// name this package does not know (an extension's check) is LightOther.
func LightFor(name, kind string) string {
	if kind == KindAccessDenied {
		return LightLogin
	}
	if l, ok := checkLights[name]; ok {
		return l
	}
	return LightOther
}
