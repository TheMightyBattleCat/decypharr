package request

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/bytedance/sonic"
)

// growHintCap bounds how much Content-Length is trusted to preallocate. A body
// larger than this still decodes; the buffer just grows on its own.
const growHintCap = 64 << 20

// responseJSON decodes response bodies. CopyString makes every decoded string
// its own allocation: by default sonic points strings into the input, so one
// string kept from a large response (a path in an index) would keep the whole
// body alive.
var responseJSON = sonic.Config{CopyString: true}.Froze()

// DecodeJSON reads a response body in full, then unmarshals it into out.
//
// Decoding straight off resp.Body with sonic's streaming decoder looks
// cheaper, but it buffers the whole document internally anyway and grows that
// scratch buffer by a fraction of its length on every read off the wire, so
// the copies are quadratic in body size: a 9 MB Sonarr library response
// allocated over 4 GB upstream. One Content-Length sized read keeps it linear.
//
// An empty body returns io.EOF and leaves out untouched, the same as a
// streaming decoder, so callers keep whatever handling they already had for it.
func DecodeJSON(resp *http.Response, out any) error {
	if resp == nil || resp.Body == nil || out == nil {
		return nil
	}
	var buf bytes.Buffer
	if n := resp.ContentLength; n > 0 {
		// +1 so the final empty read that reports EOF does not force a grow.
		buf.Grow(int(min(n, growHintCap)) + 1)
	}
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if len(bytes.TrimSpace(buf.Bytes())) == 0 {
		return io.EOF
	}
	return responseJSON.Unmarshal(buf.Bytes(), out)
}

// maxDrain bounds how much of an unread body is read before the connection is
// closed, so it can go back to the pool without reading a large body for nothing.
const maxDrain = 64 << 10

// DrainAndClose reads up to maxDrain bytes of what is left of body and closes
// it, so a small unread remainder does not cost the keep-alive connection.
func DrainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrain))
	_ = body.Close()
}
