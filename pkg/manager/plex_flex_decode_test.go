package manager

import (
	"strings"
	"testing"

	json "github.com/bytedance/sonic"
)

func decodeSessionsBody(t *testing.T, body string) (plexSessionsResponse, error) {
	t.Helper()
	var r plexSessionsResponse
	err := json.ConfigDefault.NewDecoder(strings.NewReader(body)).Decode(&r)
	return r, err
}

func sessionsBody(ratingKey, viewOffset, duration string) string {
	return `{"MediaContainer":{"Metadata":[` +
		`{"ratingKey":` + ratingKey + `,"viewOffset":` + viewOffset + `,"duration":` + duration + `,"Player":{"state":"playing"},"Media":[{"Part":[{"file":"/a.mkv"}]}]},` +
		`{"ratingKey":"2","viewOffset":5,"duration":10,"Player":{"state":"playing"},"Media":[{"Part":[{"file":"/b.mkv"}]}]}` +
		`]}}`
}

// One odd numeric field must fall back to a value for that session, never
// fail the whole /status/sessions feed (which, past the grace window, turns
// the precache gate degraded fleet-wide).
func TestPlexSessionsTolerateOddNumbers(t *testing.T) {
	for name, tc := range map[string]struct {
		body       string
		viewOffset int64
		duration   int64
		ratingKey  string
	}{
		"bare":                  {sessionsBody(`"1"`, `443142`, `1000`), 443142, 1000, "1"},
		"quoted":                {sessionsBody(`"1"`, `"443142"`, `1000`), 443142, 1000, "1"},
		"null / empty":          {sessionsBody(`"1"`, `null`, `""`), 0, 0, "1"},
		"float viewOffset":      {sessionsBody(`"1"`, `443142.0`, `1000`), 443142, 1000, "1"},
		"quoted float":          {sessionsBody(`"1"`, `"443142.5"`, `1000`), 443142, 1000, "1"},
		"exponent duration":     {sessionsBody(`"1"`, `5`, `2.7e6`), 5, 2700000, "1"},
		"bool duration":         {sessionsBody(`"1"`, `5`, `true`), 5, 0, "1"},
		"overflow viewOffset":   {sessionsBody(`"1"`, `99999999999999999999`, `1000`), 0, 1000, "1"},
		"max int64":             {sessionsBody(`"1"`, `9223372036854775807`, `1000`), 9223372036854775807, 1000, "1"},
		"bare-number ratingKey": {sessionsBody(`208698`, `5`, `1000`), 5, 1000, "208698"},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := decodeSessionsBody(t, tc.body)
			if err != nil {
				t.Fatalf("whole feed rejected: %v", err)
			}
			if len(r.MediaContainer.Metadata) != 2 {
				t.Fatalf("%d sessions decoded, want 2", len(r.MediaContainer.Metadata))
			}
			m := r.MediaContainer.Metadata[0]
			if int64(m.ViewOffset) != tc.viewOffset || int64(m.Duration) != tc.duration || string(m.RatingKey) != tc.ratingKey {
				t.Fatalf("viewOffset=%d duration=%d ratingKey=%q, want %d %d %q", m.ViewOffset, m.Duration, m.RatingKey, tc.viewOffset, tc.duration, tc.ratingKey)
			}
		})
	}
}

// The library side shares flexInt64: one odd field must not fail a page.
func TestPlexLibraryPageToleratesOddNumbers(t *testing.T) {
	body := `{"MediaContainer":{"size":2,"totalSize":2,"Metadata":[` +
		`{"ratingKey":"1","type":"movie","title":"A","year":2019.0,"Media":[{"id":1,"deletedAt":0,"Part":[{"id":1,"file":"/a","size":1}]}]},` +
		`{"ratingKey":"2","type":"movie","title":"B","year":2020,"Media":[{"id":2,"deletedAt":1700000000,"Part":[{"id":2,"file":"/b","size":2}]}]}` +
		`]}}`
	var r plexItemsResponse
	if err := json.ConfigDefault.NewDecoder(strings.NewReader(body)).Decode(&r); err != nil {
		t.Fatalf("library page rejected on one float year: %v", err)
	}
}
