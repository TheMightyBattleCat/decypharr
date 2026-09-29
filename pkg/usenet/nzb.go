package usenet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// ErrInvalidNZB marks a parse that failed on the NZB itself (empty, or not
// NZB XML). Parsing the same content again cannot help, unlike a provider or
// connection error during the parse.
var ErrInvalidNZB = errors.New("invalid NZB content")

// ValidateNZB performs basic validation on NZB content
func validateNZB(content []byte) error {
	if len(content) == 0 {
		return fmt.Errorf("empty NZB content")
	}

	// Check for basic XML structure (bytes.Contains avoids copying the
	// multi-MB NZB into a string twice)
	if !bytes.Contains(content, []byte("<nzb")) {
		return fmt.Errorf("invalid NZB format: missing <nzb> tag")
	}

	if !bytes.Contains(content, []byte("<file")) {
		return fmt.Errorf("invalid NZB format: no files found")
	}

	return nil
}

func GetFileVolumes(nf *storage.NZBFile) []*types.Volume {
	// Use the file's known size directly
	// EndOffset calculation only works for contiguous segments, not sliced RAR segments
	size := nf.Size
	if size <= 0 && len(nf.Segments) > 0 {
		// Fallback: calculate from segments if Size not set
		var maxEnd int64
		for _, seg := range nf.Segments {
			if seg.EndOffset+1 > maxEnd {
				maxEnd = seg.EndOffset + 1
			}
		}
		size = maxEnd
	}
	return []*types.Volume{
		{
			Name:          nf.Name,
			Size:          size,
			Segments:      nf.Segments,
			IsEncrypted:   nf.IsEncrypted,
			EncryptionKey: nf.EncryptionKey,
			EncryptionIV:  nf.EncryptionIV,
		},
	}
}
