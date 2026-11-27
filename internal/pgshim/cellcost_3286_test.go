package pgshim

import (
	"bytes"
	"encoding/json"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgtext"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What the copy's answer takes in the port's memory while it is read and
// written for the wire (#3286), held to the model the PostgreSQL router
// sizes its memory sum with: CellMemoryBytes a cell beside the text, and the
// text TextCopiesInMemory times. Everything alive at once at the end of
// runFree is measured: the worker's output line, the JSON decoder (which
// buffers the whole line), the decoded sqlsandbox.Result (json.Number
// cells, as the parent reads it) and the [][]byte rows for the wire. Over
// 1,000,000 small numeric cells, the most cells per byte of text.
func TestCellMemoryCost_3286(t *testing.T) {
	// The most cells per byte of text, and the most text per cell.
	t.Run("small numbers", func(t *testing.T) { measureAnswer(t, 100_000, 10, "INTEGER", func(i, j int) string { return strconv.Itoa((i*10 + j) % 100000) }) })
	long := strconv.Quote(strings.Repeat("x", 3000))
	t.Run("long text", func(t *testing.T) { measureAnswer(t, 20_000, 1, "VARCHAR", func(int, int) string { return long }) })
}

func measureAnswer(t *testing.T, rows, cols int, typ string, cell func(i, j int) string) {
	var line bytes.Buffer
	line.WriteString(`{"columns":[`)
	for j := range cols {
		if j > 0 {
			line.WriteByte(',')
		}
		line.WriteString(`{"name":"c` + strconv.Itoa(j) + `","type":"` + typ + `"}`)
	}
	line.WriteString(`],"rows":[`)
	for i := range rows {
		if i > 0 {
			line.WriteByte(',')
		}
		line.WriteByte('[')
		for j := range cols {
			if j > 0 {
				line.WriteByte(',')
			}
			line.WriteString(cell(i, j))
		}
		line.WriteByte(']')
	}
	line.WriteString(`]}`)
	text := line.String()
	line.Reset()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	// The worker's output, made after the snapshot: it is alive in the real
	// path too.
	raw := []byte(text)

	var res sqlsandbox.Result
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&res); err != nil {
		t.Fatal(err)
	}
	wire := make([][][]byte, len(res.Rows))
	for i, r := range res.Rows {
		cells := make([][]byte, len(r))
		for j, v := range r {
			if text := pgtext.CellIn(res.Columns[j].Type, v, time.UTC); text != nil {
				cells[j] = append([]byte{}, *text...)
			}
		}
		wire[i] = cells
	}
	runtime.ReadMemStats(&after)
	cells := rows * cols
	live := float64(after.HeapAlloc) - float64(before.HeapAlloc)
	model := float64(cells*CellMemoryBytes + TextCopiesInMemory*len(raw))
	t.Logf("%d cells, %d bytes of JSON: %.0f bytes alive (%.0f a cell), %.0f allocated; the model says %.0f",
		cells, len(raw), live, live/float64(cells), float64(after.TotalAlloc-before.TotalAlloc), model)
	runtime.KeepAlive(raw)
	runtime.KeepAlive(dec)
	runtime.KeepAlive(res)
	runtime.KeepAlive(wire)
	if live > model {
		t.Errorf("%.0f bytes alive, over the %.0f of CellMemoryBytes and TextCopiesInMemory: raise them", live, model)
	}
}
