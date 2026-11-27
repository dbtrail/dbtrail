//go:build integration

package consoleapp

import (
	"maps"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// pgAnsweredByCopy runs one statement (run) and reports its answer and
// whether the copy gave it, by the route reasons the port's status counts
// (reasons): answers_the_same once more than before. A test in fresh mode
// that waits for the copy to show a change has to ask this, not only the
// answer: since #3116 a light statement whose recent changes are not ready
// is answered by the source, which already holds the change, so the answer
// alone passes before the copy has it.
func pgAnsweredByCopy(reasons func() map[string]uint64, run func() string) (string, bool) {
	before := maps.Clone(reasons())
	got := run()
	total := func(m map[string]uint64) (n uint64) {
		for _, v := range m {
			n += v
		}
		return n
	}
	for range 100 {
		after := reasons()
		if after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame] {
			return got, true
		}
		if total(after) > total(before) {
			return got, false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return got, false
}
