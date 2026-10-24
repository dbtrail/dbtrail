package sqlsandbox

// The wire: what the parent and a worker process say to each other. This
// file is all of it.
//
// It is JSON values on the worker's stdin and stdout, one after another, with
// no other framing:
//
//	parent -> worker   wireJob                 the statement and its limits
//	worker -> parent   wireResult{Ask}         only when the job has AskViews:
//	                                           the relations the statement names
//	parent -> worker   wireViews               the views script for those, or Refused
//	worker -> parent   wireResult              the rows, or a wireError
//
// A worker that was told More waits for the next wireJob on the same pipe;
// any other worker exits after its result.
//
// Nothing outside this package sees these types. What the rest of the
// program holds is a Runner and its Slot (Run(ctx, Job) (Result, error)),
// and the console and the MySQL port reach those through interfaces of their
// own (internal/console sqlRunner and sqlSlot, internal/shim FreeSQL). So
// another way of reaching a DuckDB (a DuckDB server spoken to over its own
// protocol, for one) replaces what is behind Run, and this file with it,
// without the callers changing.

// wireJob is what the parent writes to the child's stdin.
type wireJob struct {
	CopyDirs []string `json:"copy_dirs"`
	ViewsSQL string   `json:"views_sql"`
	// AskViews: ViewsSQL is empty, and the worker asks for the script once
	// it has parsed the statement (Job.ViewsFor).
	AskViews bool   `json:"ask_views,omitempty"`
	SQL      string `json:"sql"`
	// NoFileReaders: see Job.NoFileReaders.
	NoFileReaders bool   `json:"no_file_readers,omitempty"`
	Schema        string `json:"schema,omitempty"`
	TimeZone      string `json:"time_zone,omitempty"`
	SelectLimit   int    `json:"select_limit,omitempty"`
	Threads       int    `json:"threads"`
	MemoryLimit   string `json:"memory_limit"`
	MaxRows       int    `json:"max_rows"`
	// MaxResultBytes is counted by the worker as it collects rows, so it
	// stops before building a result the parent would refuse anyway.
	MaxResultBytes int64 `json:"max_result_bytes"`
	// SpillDir is the private directory the parent made for this statement,
	// and SpillMaxBytes what it may hold (#2210). Empty: no spill.
	SpillDir      string `json:"spill_dir,omitempty"`
	SpillMaxBytes int64  `json:"spill_max_bytes,omitempty"`
	// TimeoutNS is the parent's wall clock, so the child can stop on its own
	// shortly after it if the parent is no longer there to kill it.
	TimeoutNS int64 `json:"timeout_ns"`
	// More: another job may follow this one on the same pipe (#2084), so the
	// worker does not exit after its result. Unset, the worker serves this
	// job and exits.
	More bool `json:"more,omitempty"`
}

// wireViews is the parent's answer to an Ask: the views script to install.
type wireViews struct {
	ViewsSQL string `json:"views_sql"`
	// Refused: there is no script, the parent refused the statement while
	// building it. Sent only to a worker that stays for another job, so it
	// ends this one instead of waiting; a worker that serves one job is
	// stopped.
	Refused bool `json:"refused,omitempty"`
}

// wireResult is what the child writes to stdout: a result, or a structured
// error. The child exits 0 in both cases; a non-zero exit is a protocol
// failure (it could not even read the job or encode its answer).
type wireResult struct {
	// Ask is the worker's question, not a result: the relations the
	// statement names, sent ahead of the result when the job has AskViews.
	Ask            *Refs    `json:"ask,omitempty"`
	Columns        []Column `json:"columns,omitempty"`
	Rows           [][]any  `json:"rows,omitempty"`
	Truncated      bool     `json:"truncated"`
	TruncatedCells int      `json:"truncated_cells"`
	ElapsedNS      int64    `json:"elapsed_ns"`
	// Child-side phase durations, ns (see Phases).
	OpenNS     int64      `json:"open_ns,omitempty"`
	LockdownNS int64      `json:"lockdown_ns,omitempty"`
	ViewsNS    int64      `json:"views_ns,omitempty"`
	Error      *wireError `json:"error,omitempty"`
	// ResidentBytes is the memory the worker holds after the statement, its
	// DuckDB closed; 0 where the platform does not say. Sent by a worker that
	// stays for another job (wireJob.More).
	ResidentBytes int64 `json:"resident_bytes,omitempty"`
}

type wireError struct {
	// Kind is one of errRefused, errQuery, errSession.
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

const (
	errRefused  = "refused"   // the statement's shape; nothing ran
	errQuery    = "query"     // DuckDB failed the statement
	errSession  = "session"   // the worker could not prepare its locked session
	errTooLarge = "too_large" // the result passed MaxResultBytes while collecting
)
