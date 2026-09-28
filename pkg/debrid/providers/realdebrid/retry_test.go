package realdebrid

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
)

func TestRetryStatusesRetryGatewayErrors(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	for _, tc := range []struct {
		name      string
		first     int
		wantCalls int32
		wantCode  int
		wantBody  string
	}{
		{name: "bad gateway", first: http.StatusBadGateway, wantCalls: 2, wantCode: http.StatusOK, wantBody: `{}`},
		{name: "gateway timeout", first: http.StatusGatewayTimeout, wantCalls: 2, wantCode: http.StatusOK, wantBody: `{}`},
		{name: "service unavailable", first: http.StatusServiceUnavailable, wantCalls: 1, wantCode: http.StatusServiceUnavailable, wantBody: `{"error":"first reply"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(tc.first)
					_, _ = io.WriteString(w, `{"error":"first reply"}`)
					return
				}
				_, _ = io.WriteString(w, `{}`)
			}))
			defer server.Close()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			client := request.New(request.WithMaxRetries(1), request.WithRetryableStatus(retryStatuses...))
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != tc.wantCalls || resp.StatusCode != tc.wantCode || string(body) != tc.wantBody {
				t.Fatalf("calls = %d, response = %d %q; want %d calls, %d %q",
					calls.Load(), resp.StatusCode, body, tc.wantCalls, tc.wantCode, tc.wantBody)
			}
		})
	}
}
