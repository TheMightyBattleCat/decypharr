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
	return h.streamResponse(entry, info.Name(), info.Size(), w, r)
}

// streamResponse serves one file of an entry by name. Offsets are inside the
// file; where the file is a slice of its backing download, Manager.Stream
// moves them.
func (h *Handler) streamResponse(entry *storage.Entry, name string, size int64, w http.ResponseWriter, r *http.Request) error {
	start, end := resolveRange(r.Header.Get("Range"), size)

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

// resolveRange maps a Range header to offsets inside the file. A file that is
// a slice of its backing download (it has a byte range) gets the same offsets
// as any other: Manager.Stream moves them by the slice's start when it asks
// the link, and checks them against the file's own size first. Adding the
// slice's start here made that check refuse the request, or cut the file's
// tail off.
func resolveRange(rangeHeader string, size int64) (int64, int64) {
	if rangeHeader == "" {
		// Signal downstream streaming code to serve the entire file
		return 0, -1
	}

	ranges, err := parseRange(rangeHeader, size)
	if err != nil || len(ranges) != 1 {
		return 0, 0
	}

	return ranges[0].start, ranges[0].end
}
