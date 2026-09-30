package manager

import (
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// importChecksFile reports whether the import gates (ffprobe, availability,
// padding) check file. Only video files are checked. One of at least
// ffprobeImportMinSize always is. A smaller one is checked when it is at
// least 1/importSmallFileShare of largest, the entry's largest video: an SD
// episode, a short film, or a pack of small episodes is the release's
// content, while a clip bundled with a film (a sample the parser's name
// filter missed, a featurette) is not, and failing it would reject a good
// grab. The repair sweep checks every library file whatever its size.
func importChecksFile(file *storage.File, largest int64) bool {
	if file == nil || !config.IsVideoFile(file.Name) {
		return false
	}
	if file.Size >= ffprobeImportMinSize {
		return true
	}
	return file.Size > 0 && file.Size*importSmallFileShare >= largest
}

// largestVideoSize is the size of entry's largest active video file.
func largestVideoSize(entry *storage.Entry) int64 {
	var largest int64
	for _, f := range entry.GetActiveFiles() {
		if f != nil && config.IsVideoFile(f.Name) && f.Size > largest {
			largest = f.Size
		}
	}
	return largest
}
