package webdav

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (h *Handler) StreamResponse(entry *storage.Entry, info *manager.FileInfo, w http.ResponseWriter, r *http.Request) error {
	return h.streamResponse(entry, info.Name(), info.Size(), info.ByteRange(), w, r)
}

// streamResponse serves one file of an entry by name. byteRange is the file's
// slice of its backing download (nil when it is the whole download); the
// request's Range header is mapped into it.
func (h *Handler) streamResponse(entry *storage.Entry, name string, size int64, byteRange *[2]int64, w http.ResponseWriter, r *http.Request) error {
	start, end := resolveRange(r.Header.Get("Range"), size, byteRange)

	// Extract client identifier from User-Agent header
	client := r.UserAgent()
	if client == "" {
		client = "Unknown"
	}

	streamID := h.manager.TrackStream(entry, name, client)
	if streamID != "" {
		defer h.manager.UntrackStream(streamID)
	}

	headersWritten := false
	err := h.manager.Stream(r.Context(), entry, name, start, end, w, func(meta *manager.StreamMetadata) error {
		if err := h.handleSuccessfulResponse(w, meta, start, end); err != nil {
			return err
		}
		headersWritten = true
		return nil
	}, client)
	if err != nil {
		var customErr *customerror.Error
		if errors.As(err, &customErr) {
			customErr.HeadersWritten = headersWritten
			return customErr
		}

		return customerror.NewError(err, http.StatusInternalServerError, "server.internal_error", false, headersWritten)
	}
	return nil
}

func (h *Handler) handleSuccessfulResponse(w http.ResponseWriter, meta *manager.StreamMetadata, start, end int64) error {
	statusCode := http.StatusOK
	if meta != nil {
		if meta.Header != nil {
			if contentLength := meta.Header.Get("Content-Length"); contentLength != "" {
				w.Header().Set("Content-Length", contentLength)
			} else if meta.ContentLength > 0 {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.ContentLength))
			}

			if contentRange := meta.Header.Get("Content-Range"); contentRange != "" {
				w.Header().Set("Content-Range", contentRange)
			}

			if contentType := meta.Header.Get("Content-Type"); contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
		}
		if meta.StatusCode != 0 {
			statusCode = meta.StatusCode
		} else if start > 0 || end > 0 {
			statusCode = http.StatusPartialContent
		}
	} else if start > 0 || end > 0 {
		statusCode = http.StatusPartialContent
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(statusCode)
	return nil
}

// resolveRange maps a Range header to offsets in the file's backing download:
// a file with a byte range is a slice of it, so the requested range is moved
// by the slice's start.
func resolveRange(rangeHeader string, size int64, byteRange *[2]int64) (int64, int64) {
	if rangeHeader == "" {
		if byteRange != nil {
			return byteRange[0], byteRange[1]
		}
		// Signal downstream streaming code to serve the entire file
		return 0, -1
	}

	ranges, err := parseRange(rangeHeader, size)
	if err != nil || len(ranges) != 1 {
		return 0, 0
	}

	start, end := ranges[0].start, ranges[0].end

	if byteRange != nil {
		start += byteRange[0]
		end += byteRange[0]
	}
	return start, end
}
