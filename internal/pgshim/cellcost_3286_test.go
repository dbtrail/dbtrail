package pgshim

import (
	"bytes"
	"encoding/json"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgtext"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What one cell of the copy's answer takes in the port's memory while it is
// decoded and written for the wire (#3286): the worker's JSON line, its
// decoding into a sqlsandbox.Result (json.Number cells, as the parent reads
// it), and the [][]byte rows runFree builds, all alive at once as they are
// at the end of runFree. Measured over 1,000,000 small numeric cells, the
// cheapest text and the most cells per byte. consoleapp's pgCopyCellBytes
// must stay at or above it (TestPGCopyCellBytesCoversTheMeasure_3286 there
// pins the number); this test fails when the cost grows past it.
const measuredCellCostCeiling = 160

func TestCellMemoryCost_3286(t *testing.T) {
	const rows, cols = 100_000, 10
	var line bytes.Buffer
	line.WriteString(`{"columns":[`)
	for j := range cols {
		if j > 0 {
			line.WriteByte(',')
		}
		line.WriteString(`{"name":"c` + strconv.Itoa(j) + `","type":"INTEGER"}`)
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
			line.WriteString(strconv.Itoa((i*cols + j) % 100000))
		}
		line.WriteByte(']')
	}
	line.WriteString(`]}`)
	raw := line.Bytes()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

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
	perCell := float64(after.TotalAlloc-before.TotalAlloc) / float64(cells)
	live := float64(after.HeapAlloc) - float64(before.HeapAlloc)
	t.Logf("%d cells: %.0f bytes allocated per cell, %.0f live per cell; the JSON line itself %.1f per cell",
		cells, perCell, live/float64(cells), float64(len(raw))/float64(cells))
	runtime.KeepAlive(raw)
	runtime.KeepAlive(res)
	runtime.KeepAlive(wire)
	if live/float64(cells) > measuredCellCostCeiling {
		t.Errorf("%.0f live bytes per cell, over the %d pgCopyCellBytes is sized with", live/float64(cells), measuredCellCostCeiling)
	}
}
