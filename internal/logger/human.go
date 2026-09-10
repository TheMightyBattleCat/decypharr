package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Fields the console line understands. Call sites set them on the events a
// person following the journal needs; like every other field they are also
// written to the log file.
const (
	// FieldStatus is one of the Status values; without it the line's symbol
	// follows its level.
	FieldStatus = "status"
	// FieldSubject names what the line is about - an entry or a file. Release
	// names are shortened for display (DisplayName). Without it the line
	// falls back to an entry, file or name field.
	FieldSubject = "subject"
	// FieldSize (bytes), FieldRate (MiB/s) and FieldTook (a zerolog Dur) fill
	// the line's [2.1 GB • 16.3 MiB/s • 9.2s] summary.
	FieldSize = "size_bytes"
	FieldRate = "rate_mib_s"
	FieldTook = "took"
	// FieldNote is one short free-text summary item, e.g. "partial coverage".
	FieldNote = "note"
)

// Status values for FieldStatus.
const (
	StatusOK    = "ok"    // ✓ finished well
	StatusStart = "start" // → started, in progress
	StatusWarn  = "warn"  // ⚠ finished with a problem worth a look
	StatusFail  = "fail"  // ✗ failed
)

const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
)

// categories maps a logger prefix (logger.New) to the short category shown
// on the console.
var categories = map[string]string{
	"decypharr":        "APP",
	"stats":            "STATS",
	"repair":           "REPAIR",
	"test-repair":      "REPAIR",
	"par2-repair":      "PAR2",
	"test-par2-repair": "PAR2",
	"precache":         "CACHE",
	"dfs":              "MOUNT",
	"vfs":              "MOUNT",
	"cgofuse":          "MOUNT",
	"hanwen-backend":   "MOUNT",
	"rclone":           "MOUNT",
	"webdav":           "STREAM",
	"usenet":           "USENET",
	"usenet-overlay":   "USENET",
	"nzb-storage":      "USENET",
	"nntp-client":      "NNTP",
	"manager":          "MANAGER",
	"link":             "MANAGER",
	"queue":            "QUEUE",
	"jobqueue":         "QUEUE",
	"store":            "STORE",
	"storage":          "STORE",
	"migrator":         "STORE",
	"arr":              "ARR",
	"qbit":             "QBIT",
	"sabnzbd":          "SABNZBD",
	"plex-sessions":    "PLEX",
	"external":         "EXTERNAL",
	"http":             "SERVER",
	"request":          "SERVER",
}

// componentCategories maps a "component" field, which sub-loggers add, to a
// category that overrides the prefix's.
var componentCategories = map[string]string{
	"parser":              "PARSER",
	"rar_parser":          "PARSER",
	"rar_parser_embedded": "PARSER",
	"7z_parser":           "PARSER",
	"zip_parser":          "PARSER",
	"par2-backfill":       "PARSER",
	"fetcher":             "FETCH",
	"downloader":          "DOWNLOAD",
	"cache":               "CACHE",
	"notifications":       "NOTIFY",
}

// messagePrefixes are stripped from console messages: the category already
// says what they say. The log file keeps messages verbatim.
var messagePrefixes = []string{"[repair] ", "Repair: ", "Import: ", "Usenet: ", "PAR2: ", "Precache: "}

// ffmpegAddress matches the " @ 0x55e40f1cda80" instance address ffmpeg puts
// in its log prefixes: noise on a console line, kept in the log file.
var ffmpegAddress = regexp.MustCompile(` @ 0x[0-9A-Fa-f]+`)

// subjectFields are tried in order for a line's subject.
var subjectFields = []string{FieldSubject, "entry", "file", "name", "entry_name"}

const categoryWidth = 8

// humanWriter renders zerolog JSON events as one short line each, for a
// person following the journal:
//
//	10:42:18 REPAIR   ✓ ffprobe passed The Marlowes S01E03 [2.1 GB • 16.3 MiB/s • 9.2s]
//
// The line carries the time, category, status symbol, message, subject, a
// unit summary and any reason or error. Other fields - IDs, raw byte counts,
// timeouts - stay off info, warning and error lines; they remain on the event
// and so in the log file. Debug and trace lines, which only reach the console
// when log_level asks for them, append the remaining fields dimmed.
//
// Colour, when on, is only ever in the rendered line: the time and summary
// dimmed, the symbol coloured, and errors and failures in bold.
type humanWriter struct {
	out    io.Writer
	prefix string
	color  bool
	loc    *time.Location // nil: time.Local
}

func newHumanWriter(out io.Writer, prefix string, color bool) *humanWriter {
	return &humanWriter{out: out, prefix: prefix, color: color}
}

func (h *humanWriter) Write(p []byte) (int, error) {
	evt := map[string]any{}
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	if err := d.Decode(&evt); err != nil {
		// Not a zerolog event: pass it through untouched.
		return h.out.Write(p)
	}
	var b bytes.Buffer
	h.render(&b, evt)
	if _, err := h.out.Write(b.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (h *humanWriter) render(b *bytes.Buffer, evt map[string]any) {
	level, err := zerolog.ParseLevel(text(evt[zerolog.LevelFieldName]))
	if err != nil || level == zerolog.NoLevel {
		level = zerolog.InfoLevel
	}
	status := text(evt[FieldStatus])
	used := map[string]bool{
		zerolog.TimestampFieldName: true, zerolog.LevelFieldName: true, zerolog.MessageFieldName: true,
		FieldStatus: true, "component": true,
	}

	h.paint(b, ansiDim, h.clock(evt[zerolog.TimestampFieldName]))
	b.WriteByte(' ')
	b.WriteString(fmt.Sprintf("%-*s", categoryWidth, h.category(text(evt["component"]))))
	b.WriteByte(' ')
	symbol, symbolColor := statusSymbol(status, level)
	h.paint(b, symbolColor, symbol)
	b.WriteByte(' ')

	msg := oneLine(text(evt[zerolog.MessageFieldName]))
	for _, p := range messagePrefixes {
		msg = strings.TrimPrefix(msg, p)
	}
	if level >= zerolog.ErrorLevel || status == StatusFail {
		h.paint(b, ansiBold, msg)
	} else {
		b.WriteString(msg)
	}

	for _, key := range subjectFields {
		if name := text(evt[key]); name != "" {
			used[key] = true
			b.WriteByte(' ')
			b.WriteString(DisplayName(oneLine(name)))
			break
		}
	}

	var summary []string
	if n, ok := integer(evt[FieldSize]); ok {
		summary = append(summary, FormatBytes(n))
		used[FieldSize] = true
	}
	if f, ok := number(evt[FieldRate]); ok {
		summary = append(summary, FormatRate(f))
		used[FieldRate] = true
	}
	if f, ok := number(evt[FieldTook]); ok {
		summary = append(summary, FormatDuration(time.Duration(f*float64(zerolog.DurationFieldUnit))))
		used[FieldTook] = true
	}
	if note := oneLine(text(evt[FieldNote])); note != "" {
		summary = append(summary, note)
		used[FieldNote] = true
	}
	if len(summary) > 0 {
		b.WriteByte(' ')
		h.paint(b, ansiDim, "["+strings.Join(summary, " • ")+"]")
	}

	var why []string
	for _, key := range []string{"reason", zerolog.ErrorFieldName} {
		if v := ffmpegAddress.ReplaceAllString(oneLine(text(evt[key])), ""); v != "" {
			why = append(why, truncate(v, 160))
			used[key] = true
		}
	}
	if len(why) > 0 {
		b.WriteString(" (" + strings.Join(why, ": ") + ")")
	}

	if level <= zerolog.DebugLevel {
		keys := make([]string, 0, len(evt))
		for k := range evt {
			if !used[k] {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			extras := make([]string, len(keys))
			for i, k := range keys {
				extras[i] = k + "=" + fieldValue(evt[k])
			}
			b.WriteByte(' ')
			h.paint(b, ansiDim, strings.Join(extras, " "))
		}
	}
	b.WriteByte('\n')
}

func (h *humanWriter) category(component string) string {
	if c, ok := componentCategories[component]; ok {
		return c
	}
	if c, ok := categories[h.prefix]; ok {
		return c
	}
	return truncate(strings.ToUpper(h.prefix), categoryWidth)
}

func (h *humanWriter) clock(v any) string {
	t, err := time.Parse(zerolog.TimeFieldFormat, text(v))
	if err != nil {
		t = time.Now()
	}
	if h.loc != nil {
		t = t.In(h.loc)
	} else {
		t = t.Local()
	}
	return t.Format("15:04:05")
}

func (h *humanWriter) paint(b *bytes.Buffer, code, s string) {
	if !h.color || code == "" || s == "" {
		b.WriteString(s)
		return
	}
	b.WriteString(code)
	b.WriteString(s)
	b.WriteString(ansiReset)
}

func statusSymbol(status string, level zerolog.Level) (string, string) {
	switch status {
	case StatusOK:
		return "✓", ansiGreen
	case StatusStart:
		return "→", ansiCyan
	case StatusWarn:
		return "⚠", ansiYellow
	case StatusFail:
		return "✗", ansiRed
	}
	switch {
	case level >= zerolog.ErrorLevel:
		return "✗", ansiRed
	case level == zerolog.WarnLevel:
		return "⚠", ansiYellow
	case level <= zerolog.DebugLevel:
		return "·", ansiDim
	}
	return " ", ""
}

// useColor decides whether console lines carry ANSI colour. setting is the
// log_color config: "always" or "never" win; otherwise ("auto") a non-empty
// NO_COLOR turns colour off, and it is on for journald (JOURNAL_STREAM set:
// journalctl hands the codes to the terminal) or a terminal, and off when
// stdout is redirected to a file or pipe.
func useColor(setting string, getenv func(string) string, stdout *os.File) bool {
	switch strings.ToLower(strings.TrimSpace(setting)) {
	case "always", "on", "true":
		return true
	case "never", "off", "false":
		return false
	}
	if getenv("NO_COLOR") != "" {
		return false
	}
	if getenv("JOURNAL_STREAM") != "" {
		return true
	}
	if stdout == nil {
		return false
	}
	fi, err := stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func text(v any) string {
	switch s := v.(type) {
	case nil:
		return ""
	case string:
		return s
	case json.Number:
		return s.String()
	default:
		return fmt.Sprint(s)
	}
}

func oneLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.Join(strings.Fields(strings.NewReplacer("\r", " ", "\n", " | ").Replace(s)), " ")
}

func number(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	}
	return 0, false
}

func integer(v any) (int64, bool) {
	if n, ok := v.(json.Number); ok {
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	}
	f, ok := number(v)
	return int64(f), ok
}

func fieldValue(v any) string {
	switch x := v.(type) {
	case string:
		if x == "" || strings.ContainsAny(x, " \t\"=") {
			return fmt.Sprintf("%q", x)
		}
		return x
	case json.Number:
		return x.String()
	case map[string]any, []any:
		if raw, err := json.Marshal(x); err == nil {
			return string(raw)
		}
	}
	return fmt.Sprint(v)
}
