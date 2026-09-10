package logger

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// FormatBytes renders a byte count in decimal units: "512 B", "4.0 KB",
// "812 MB", "2.1 GB".
func FormatBytes(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	v := float64(n) / float64(div)
	if v >= 100 {
		return fmt.Sprintf("%.0f %cB", v, "KMGTP"[exp])
	}
	return fmt.Sprintf("%.1f %cB", v, "KMGTP"[exp])
}

// Count renders n with its noun, plural unless n is 1: "1 file", "3 files".
func Count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// FormatRate renders a throughput in MiB/s: "16.3 MiB/s", "112 MiB/s".
func FormatRate(mibPerSec float64) string {
	if mibPerSec >= 100 {
		return fmt.Sprintf("%.0f MiB/s", mibPerSec)
	}
	return fmt.Sprintf("%.1f MiB/s", mibPerSec)
}

// FormatDuration renders a duration the way a person says it: "850ms",
// "9.2s", "31s", "3m12s", "1h04m".
func FormatDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < 10*time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

var (
	// releaseTag matches the first quality, source or codec tag of a release
	// name, which is where the part a person reads ends. It only matches after
	// a separator, so a title that starts with such a word keeps it. A bare
	// "web" is left out: it is a word in titles (Madeleine's Web).
	releaseTag = regexp.MustCompile(`(?i)[ ._\-\[(](?:2160p|1080p|1080i|720p|576p|480p|4k|uhd|bluray|blu-ray|bdrip|brrip|web-?dl|webrip|hdtv|dvdrip|remux|repack|proper|internal|amzn|hmax|atvp|dsnp|hulu|pcok|x26[45]|h\.?26[45]|hevc|ddp?[0-9.]*|dts|truehd|flac)(?:[ ._\-\])]|$)`)
	mediaExt   = regexp.MustCompile(`(?i)\.(mkv|mp4|m4v|avi|ts|m2ts|wmv|mov|webm|iso|nzb|rar)$`)
)

// maxSubjectRunes bounds a displayed name.
const maxSubjectRunes = 80

// DisplayName turns a release or file name into the words a person reads:
// "The.Marlowes.S01E03.Winter.Frost.Beginnings.1080p.AMZN.WEB-DL.DDP2.0.H.264-WADU.mkv"
// becomes "The Marlowes S01E03 Winter Frost Beginnings". A name with no tags
// to trim only has its separators replaced; if nothing is left the input is
// returned unchanged.
func DisplayName(name string) string {
	s := strings.TrimSpace(name)
	if i := strings.LastIndexAny(s, `/\`); i >= 0 {
		s = s[i+1:]
	}
	s = mediaExt.ReplaceAllString(s, "")
	if loc := releaseTag.FindStringIndex(s); loc != nil && loc[0] > 0 {
		s = s[:loc[0]]
	}
	s = strings.NewReplacer(".", " ", "_", " ").Replace(s)
	s = strings.TrimRight(strings.Join(strings.Fields(s), " "), " -")
	if s == "" {
		return name
	}
	return truncate(s, maxSubjectRunes)
}

// truncate shortens s to at most n runes, marking the cut with an ellipsis.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
