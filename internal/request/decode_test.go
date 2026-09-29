package request

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
)

func jsonResponse(body string) *http.Response {
	return &http.Response{Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func TestDecodeJSON(t *testing.T) {
	var out struct {
		Name string `json:"name"`
	}
	if err := DecodeJSON(jsonResponse(`{"name":"sonarr"}`+"\n"), &out); err != nil || out.Name != "sonarr" {
		t.Fatalf("DecodeJSON = %v, name %q", err, out.Name)
	}

	// An empty body leaves out untouched and reports io.EOF, as the
	// streaming decoder did, so callers keep their empty-body handling.
	for _, body := range []string{"", " \n"} {
		out.Name = "kept"
		resp := jsonResponse(body)
		resp.ContentLength = -1
		if err := DecodeJSON(resp, &out); !errors.Is(err, io.EOF) || out.Name != "kept" {
			t.Fatalf("body %q: err = %v, name %q", body, err, out.Name)
		}
	}

	// Unlike the streaming decoder, data after the JSON value is an error.
	if err := DecodeJSON(jsonResponse(`{"name":"a"} {"name":"b"}`), &out); err == nil {
		t.Fatal("trailing JSON value accepted")
	}
	if err := DecodeJSON(jsonResponse(`{"name":`), &out); err == nil {
		t.Fatal("truncated JSON accepted")
	}
}

var retained string

// A string kept from a large response must not keep the whole response
// alive. sonic's default decoding points strings into its copy of the input.
func TestDecodeJSONDoesNotPinTheBody(t *testing.T) {
	var b strings.Builder
	b.WriteString("[")
	for i := 0; b.Len() < 8<<20; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"path":"/media/tv/Series %d","overview":"%s"}`, i, strings.Repeat("x", 400))
	}
	b.WriteString("]")
	body := b.String()

	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}
	before := heap()
	func() {
		var out []struct {
			Path     string `json:"path"`
			Overview string `json:"overview"`
		}
		if err := DecodeJSON(jsonResponse(body), &out); err != nil {
			t.Fatal(err)
		}
		retained = out[len(out)/2].Path
	}()
	after := heap()
	runtime.KeepAlive(body) // live in both measurements, so it cancels out
	if grown := int64(after) - int64(before); grown > 2<<20 {
		t.Fatalf("keeping one decoded string kept %d MB alive", grown>>20)
	}
	if !strings.HasPrefix(retained, "/media/tv/Series ") {
		t.Fatalf("retained = %q", retained)
	}
}
