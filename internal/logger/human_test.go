package logger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

var testClock = time.Date(2026, 9, 10, 10, 42, 9, 0, time.UTC)

// consoleLogger renders through humanWriter into out at min, in UTC, the way
// New wires the console.
func consoleLogger(out *bytes.Buffer, prefix string, color bool, min zerolog.Level) zerolog.Logger {
	w := levelWriter{w: &humanWriter{out: out, prefix: prefix, color: color, loc: time.UTC}, min: min}
	return zerolog.New(zerolog.MultiLevelWriter(w)).Level(zerolog.TraceLevel)
}

// The console lines for a success, work in progress, a warning, an error and
// a debug line, as `journalctl -u decypharr_beta -f -o cat` shows them with
// colour off.
func Example_consoleLines() {
	var out bytes.Buffer
	log := consoleLogger(&out, "repair", false, zerolog.DebugLevel)
	at := func(sec int) time.Time { return testClock.Add(time.Duration(sec) * time.Second) }
	const marlowes = "The.Marlowes.S01E03.Winter.Frost.Beginnings.1080p.AMZN.WEB-DL.DDP2.0.H.264-WADU.mkv"

	log.Info().Time("time", at(0)).Str(FieldStatus, StatusStart).Str(FieldSubject, marlowes).
		Msg("checking")
	log.Info().Time("time", at(9)).Str(FieldStatus, StatusOK).Str(FieldSubject, marlowes).
		Int64(FieldSize, 2_100_000_000).Float64(FieldRate, 16.3).Dur(FieldTook, 9200*time.Millisecond).
		Str("run_id", "5a97a47a-8349-4546-b605-6973c11ddc0c").
		Msg("ffprobe passed")
	log.Warn().Time("time", at(15)).
		Str(FieldSubject, "Northernlight.S01E04.The.Long.Road.of.Rye.1080p.ATVP.WEB-DL.DDP5.1.H.264-NTb.mkv").
		Str(FieldNote, "partial coverage").Str("reason", "read budget spent before a verdict").
		Msg("Repair: decode check inconclusive")
	log.Error().Time("time", at(18)).Str(FieldStatus, StatusFail).
		Str("entry", "The.Rolling.Hill.Maxim.Brook.S02E01.1080p.AMC.WEB-DL.DDP5.1.H.264-NTb").
		Err(errors.New("ffprobe_decode_error: element overruns its container")).
		Msg("broken")
	log.Debug().Time("time", at(19)).Str("entry", "The.Orb.2018.1080p.BluRay.REMUX.AVC.Atmos-EPSiLON").
		Int64("file_bytes", 19089929491).Int("frames", 1440).Str("phase", "spread").
		Msg("Repair: ffprobe decode check passed")
	fmt.Print(out.String())

	// Output:
	// 10:42:09 REPAIR   → checking The Marlowes S01E03 Winter Frost Beginnings
	// 10:42:18 REPAIR   ✓ ffprobe passed The Marlowes S01E03 Winter Frost Beginnings [2.1 GB • 16.3 MiB/s • 9.2s]
	// 10:42:24 REPAIR   ⚠ decode check inconclusive Northernlight S01E04 The Long Road of Rye [partial coverage] (read budget spent before a verdict)
	// 10:42:27 REPAIR   ✗ broken The Rolling Hill Maxim Brook S02E01 (ffprobe_decode_error: element overruns its container)
	// 10:42:28 REPAIR   · ffprobe decode check passed The Orb 2018 file_bytes=19089929491 frames=1440 phase=spread
}

func TestHumanWriter_InfoKeepsMachineFieldsOff(t *testing.T) {
	var out bytes.Buffer
	log := consoleLogger(&out, "usenet", false, zerolog.InfoLevel)
	log.Info().Time("time", testClock).
		Str("nzb_id", "776a2538-b4e3-4805-bb43-6217b5ea05eb").Dur("timeout", time.Minute).
		Int64("file_bytes", 4254545059).Str("name", "Northernlight.S01E04.The.Long.Road.of.Rye.1080p.ATVP.WEB-DL.DDP5.1.H.264-NTb").
		Msg("Successfully parsed NZB file")
	line := out.String()
	for _, hidden := range []string{"nzb_id", "776a2538", "timeout", "4254545059", "="} {
		if strings.Contains(line, hidden) {
			t.Fatalf("info line %q shows %q", line, hidden)
		}
	}
	if want := "10:42:09 USENET     Successfully parsed NZB file Northernlight S01E04 The Long Road of Rye\n"; line != want {
		t.Fatalf("got  %q\nwant %q", line, want)
	}
}

func TestHumanWriter_ColourOnlyWhenEnabled(t *testing.T) {
	for _, color := range []bool{false, true} {
		var out bytes.Buffer
		log := consoleLogger(&out, "webdav", color, zerolog.InfoLevel)
		log.Error().Time("time", testClock).
			Err(errors.New("connection reset")).Str(FieldSubject, "The.Orb.2018.1080p.mkv").Msg("stream failed")
		if got := strings.Contains(out.String(), "\x1b["); got != color {
			t.Fatalf("color=%v: line %q has ANSI=%v", color, out.String(), got)
		}
	}
}

func TestHumanWriter_ComponentOverridesPrefix(t *testing.T) {
	var out bytes.Buffer
	log := consoleLogger(&out, "usenet", false, zerolog.InfoLevel).With().Str("component", "parser").Logger()
	log.Warn().Time("time", testClock).Msg("RAR file is shorter than its archive header says")
	if !strings.HasPrefix(out.String(), "10:42:09 PARSER   ⚠ ") {
		t.Fatalf("got %q, want the PARSER category", out.String())
	}
}

func TestHumanWriter_UnknownPrefixIsUppercased(t *testing.T) {
	var out bytes.Buffer
	log := consoleLogger(&out, "somethingnew", false, zerolog.InfoLevel)
	log.Info().Time("time", testClock).Msg("hello")
	if !strings.HasPrefix(out.String(), "10:42:09 SOMETHI… ") {
		t.Fatalf("got %q", out.String())
	}
}

func TestHumanWriter_OneLinePerEvent(t *testing.T) {
	var out bytes.Buffer
	log := consoleLogger(&out, "repair", false, zerolog.InfoLevel)
	log.Warn().Time("time", testClock).Str("reason", "first\nsecond").Msg("multi\nline")
	if n := strings.Count(out.String(), "\n"); n != 1 {
		t.Fatalf("got %d newlines in %q, want exactly one", n, out.String())
	}
}

func TestHumanWriter_PassesThroughNonJSON(t *testing.T) {
	var out bytes.Buffer
	w := &humanWriter{out: &out, prefix: "x"}
	if _, err := w.Write([]byte("plain text\n")); err != nil || out.String() != "plain text\n" {
		t.Fatalf("got %q, %v", out.String(), err)
	}
}

// The console at info and the file at debug: a debug event reaches only the
// file, an info event both.
func TestLevelWriter_SplitsConsoleAndFile(t *testing.T) {
	var console, file bytes.Buffer
	log := zerolog.New(zerolog.MultiLevelWriter(
		levelWriter{w: &humanWriter{out: &console, prefix: "repair", loc: time.UTC}, min: zerolog.InfoLevel},
		levelWriter{w: newFileWriter(&file, "repair"), min: zerolog.DebugLevel},
	)).Level(zerolog.DebugLevel)

	log.Debug().Time("time", testClock).Msg("detail only")
	log.Info().Time("time", testClock).Msg("for everyone")

	if strings.Contains(console.String(), "detail only") || !strings.Contains(console.String(), "for everyone") {
		t.Fatalf("console got %q", console.String())
	}
	if !strings.Contains(file.String(), "detail only") || !strings.Contains(file.String(), "for everyone") {
		t.Fatalf("file got %q", file.String())
	}
}

// The file keeps the line shape the analysis scripts parse, with no colour.
func TestFileWriter_KeepsTheGreppableFormat(t *testing.T) {
	var file bytes.Buffer
	log := zerolog.New(newFileWriter(&file, "repair"))
	log.Info().Time("time", testClock).
		Str("run_id", "5a97a47a").Str(FieldStatus, StatusOK).Msg("Repair sweep started")
	line := strings.TrimRight(file.String(), "\n")
	pattern := regexp.MustCompile(`^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) \| ([A-Z]+) *\| \[([^\]]+)\] (.*)$`)
	m := pattern.FindStringSubmatch(line)
	if m == nil || m[2] != "INFO" || m[3] != "repair" || !strings.HasPrefix(m[4], "Repair sweep started") {
		t.Fatalf("file line %q does not match the analysis format", line)
	}
	if strings.Contains(line, "\x1b") || !strings.Contains(line, "run_id=5a97a47a") || !strings.Contains(line, "status=ok") {
		t.Fatalf("file line %q must keep every field and no colour", line)
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]zerolog.Level{
		"debug": zerolog.DebugLevel, "INFO": zerolog.InfoLevel, "warning": zerolog.WarnLevel,
		" trace ": zerolog.TraceLevel, "error": zerolog.ErrorLevel, "": zerolog.WarnLevel, "loud": zerolog.WarnLevel,
	}
	for name, want := range cases {
		if got := parseLevel(name, zerolog.WarnLevel); got != want {
			t.Fatalf("parseLevel(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestUseColor(t *testing.T) {
	env := func(vars map[string]string) func(string) string {
		return func(k string) string { return vars[k] }
	}
	regular, err := os.Create(filepath.Join(t.TempDir(), "out.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()

	cases := []struct {
		name    string
		setting string
		vars    map[string]string
		stdout  *os.File
		want    bool
	}{
		{"always wins over NO_COLOR", "always", map[string]string{"NO_COLOR": "1"}, regular, true},
		{"never wins over journald", "never", map[string]string{"JOURNAL_STREAM": "8:1"}, nil, false},
		{"NO_COLOR turns auto off", "", map[string]string{"NO_COLOR": "1", "JOURNAL_STREAM": "8:1"}, nil, false},
		{"journald", "auto", map[string]string{"JOURNAL_STREAM": "8:1"}, regular, true},
		{"redirected to a file", "", nil, regular, false},
		{"no stdout", "", nil, nil, false},
	}
	for _, tc := range cases {
		if got := useColor(tc.setting, env(tc.vars), tc.stdout); got != tc.want {
			t.Fatalf("%s: useColor = %v, want %v", tc.name, got, tc.want)
		}
	}
}
