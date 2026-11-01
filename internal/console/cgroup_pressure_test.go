package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stagePressureRoot writes a fake / with /proc/self/cgroup and the given
// files under sys/fs/cgroup, keyed by their path below it.
func stagePressureRoot(t *testing.T, selfCgroup string, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if selfCgroup != "" {
		writeUnder(t, root, "proc/self/cgroup", selfCgroup)
	}
	for rel, body := range files {
		writeUnder(t, root, filepath.Join("sys/fs/cgroup", rel), body)
	}
	return root
}

func writeUnder(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func psi(someTotal, fullTotal string) string {
	s := "some avg10=1.00 avg60=0.50 avg300=0.10 total=" + someTotal + "\n"
	if fullTotal != "" {
		s += "full avg10=0.00 avg60=0.00 avg300=0.00 total=" + fullTotal + "\n"
	}
	return s
}

func TestReadPressure(t *testing.T) {
	const svc = "system.slice/bintrail.service"
	cases := []struct {
		name       string
		self       string
		files      map[string]string
		wantCPU    time.Duration
		wantIO     time.Duration
		wantCPUErr string // substring; "" means read
		wantIOErr  string
	}{
		{
			name:    "own cgroup under a unit",
			self:    "0::/" + svc + "\n",
			files:   map[string]string{svc + "/cpu.pressure": psi("1500000", "900"), svc + "/io.pressure": psi("250", "100")},
			wantCPU: 1500 * time.Millisecond, wantIO: 250 * time.Microsecond,
		},
		{
			// A container with its own cgroup namespace: the path is "/" and
			// the mount root IS the container's cgroup.
			name:    "container root",
			self:    "0::/\n",
			files:   map[string]string{"cpu.pressure": psi("7", ""), "io.pressure": psi("9", "")},
			wantCPU: 7 * time.Microsecond, wantIO: 9 * time.Microsecond,
		},
		{
			// Sharing the host's namespace: the named path is not mounted, the
			// container's own cgroup is at the root (as cpuPriorityOf reads it).
			name:    "named path missing, root holds it",
			self:    "0::/docker/abc\n",
			files:   map[string]string{"cpu.pressure": psi("3", ""), "io.pressure": psi("4", "")},
			wantCPU: 3 * time.Microsecond, wantIO: 4 * time.Microsecond,
		},
		{
			// A kernel without "full" for cpu (before 5.13) is still a reading.
			name:    "cpu without a full line",
			self:    "0::/" + svc + "\n",
			files:   map[string]string{svc + "/cpu.pressure": psi("10", ""), svc + "/io.pressure": psi("20", "5")},
			wantCPU: 10 * time.Microsecond, wantIO: 20 * time.Microsecond,
		},
		{
			name:       "hybrid layout reads the v2 line",
			self:       "12:cpu,cpuacct:/" + svc + "\n0::/" + svc + "\n",
			files:      map[string]string{svc + "/cpu.pressure": psi("11", ""), "cpu,cpuacct/" + svc + "/cpu.shares": "1024"},
			wantCPU:    11 * time.Microsecond,
			wantIOErr:  "io.pressure could not be read",
			wantCPUErr: "",
		},
		{
			name:       "no /proc/self/cgroup (not Linux)",
			wantCPUErr: "/proc/self/cgroup could not be read", wantIOErr: "/proc/self/cgroup could not be read",
		},
		{
			name:       "cgroup v1 only",
			self:       "12:cpu,cpuacct:/" + svc + "\n4:memory:/" + svc + "\n",
			wantCPUErr: "needs cgroup v2", wantIOErr: "needs cgroup v2",
		},
		{
			// PSI compiled out or off (psi=0): the file is not there.
			name:       "pressure files missing",
			self:       "0::/" + svc + "\n",
			files:      map[string]string{svc + "/cpu.weight": "100"},
			wantCPUErr: "cpu.pressure could not be read", wantIOErr: "io.pressure could not be read",
		},
		{
			name:       "garbled",
			self:       "0::/\n",
			files:      map[string]string{"cpu.pressure": "full avg10=0 total=5\n", "io.pressure": "some avg10=0 total=abc\n"},
			wantCPUErr: "holds no \"some\" total", wantIOErr: "holds no \"some\" total",
		},
		{
			name:       "empty file",
			self:       "0::/\n",
			files:      map[string]string{"cpu.pressure": "", "io.pressure": "  \n"},
			wantCPUErr: "holds no \"some\" total", wantIOErr: "holds no \"some\" total",
		},
		{
			name:    "blank lines and extra spaces",
			self:    "\n0::/" + svc + "  \n\n",
			files:   map[string]string{svc + "/cpu.pressure": "\n" + psi("42", "1") + "\n", svc + "/io.pressure": "some  avg10=0.00   total=43  \n"},
			wantCPU: 42 * time.Microsecond, wantIO: 43 * time.Microsecond,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := stagePressureRoot(t, tc.self, tc.files)
			got := readPressure(root)
			check := func(what string, d time.Duration, errText string, wantD time.Duration, wantErr string) {
				t.Helper()
				if wantErr == "" {
					if errText != "" || d != wantD {
						t.Fatalf("%s = %v (%q), want %v read", what, d, errText, wantD)
					}
					return
				}
				if !strings.Contains(errText, wantErr) {
					t.Fatalf("%s error = %q, want it to say %q", what, errText, wantErr)
				}
				if strings.Contains(errText, root) {
					t.Fatalf("%s error %q leaks the test root", what, errText)
				}
			}
			check("cpu", got.CPU, got.CPUErr, tc.wantCPU, tc.wantCPUErr)
			check("io", got.IO, got.IOErr, tc.wantIO, tc.wantIOErr)
		})
	}
}

func TestStallWords(t *testing.T) {
	read := func(d time.Duration) PressureSample { return PressureSample{CPU: d, IO: d} }
	cases := []struct {
		name          string
		before, after PressureSample
		took          time.Duration
		want          string
	}{
		{"a share of a long run", read(time.Minute), read(time.Minute + 7*time.Minute + 26*time.Second), 24 * time.Minute, "31% of the run (7m26s of 24m0s)"},
		{"none", read(5 * time.Second), read(5 * time.Second), 10 * time.Minute, "0% of the run (0s of 10m0s)"},
		{"a short run keeps milliseconds", read(0), read(450 * time.Millisecond), 1500 * time.Millisecond, "30% of the run (450ms of 1.5s)"},
		// "some" is wall time with at least one task stalled, so it cannot
		// pass the run; a reading that does is sampling skew, and says 100.
		{"capped at the whole run", read(0), read(11 * time.Second), 10 * time.Second, "100% of the run (10s of 10s)"},
		{"backwards", read(9 * time.Second), read(time.Second), time.Minute, "unknown: the counter went backwards"},
		{"zero run", read(0), read(0), 0, "unknown: the run took no measurable time"},
		{"unreadable before", PressureSample{CPUErr: "unknown: cpu.pressure could not be read", IOErr: "x"}, read(time.Second), time.Minute, "unknown: cpu.pressure could not be read"},
		{"unreadable after", read(0), PressureSample{CPUErr: "unknown: /sys/fs/cgroup/cpu.pressure holds no \"some\" total", IOErr: "x"}, time.Minute, "unknown: /sys/fs/cgroup/cpu.pressure holds no \"some\" total"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cpu, _ := StallWords(tc.before, tc.after, tc.took)
			if cpu != tc.want {
				t.Fatalf("cpu = %q, want %q", cpu, tc.want)
			}
		})
	}
	// The two counters are worded independently: one unreadable file does
	// not hide the other's reading.
	cpu, io := StallWords(PressureSample{CPU: 0, IOErr: "unknown: io.pressure could not be read"},
		PressureSample{CPU: 30 * time.Second, IO: time.Hour}, time.Minute)
	if cpu != "50% of the run (30s of 1m0s)" || io != "unknown: io.pressure could not be read" {
		t.Fatalf("cpu, io = %q, %q", cpu, io)
	}
}
