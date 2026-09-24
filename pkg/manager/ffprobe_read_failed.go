package manager

import "strings"

// ffprobeReadFailedTag marks an ffprobe_unreadable reason whose stderr shows
// the read itself failing rather than the container failing to parse.
const ffprobeReadFailedTag = " (read failed)"

// ffprobeReadFailedMarkers are what ffmpeg prints when the bytes stop coming
// (a short HTTP body, a dropped connection), as opposed to a parse error.
var ffprobeReadFailedMarkers = []string{
	"Stream ends prematurely",
	"Input/output error",
	"I/O error",
	"Connection reset",
	"Connection refused",
	"Connection timed out",
	"HTTP error",
	"Server returned 5",
	"Broken pipe",
}

// ffprobeReadFailed reports whether ffprobe's stderr shows a failed read.
func ffprobeReadFailed(stderr string) bool {
	for _, m := range ffprobeReadFailedMarkers {
		if strings.Contains(stderr, m) {
			return true
		}
	}
	return false
}
