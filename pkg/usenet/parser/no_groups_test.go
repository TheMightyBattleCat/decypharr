package parser

import (
	"context"
	"errors"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"
)

func noGroupsNZB(names ...string) []byte {
	out := `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><head></head>`
	for i, n := range names {
		out += `<file poster="p" date="1" subject="&quot;` + n + `&quot; yEnc (1/1)"><groups><group>a.b</group></groups><segments>` +
			`<segment bytes="100" number="1">id` + string(rune('a'+i)) + `@x</segment></segments></file>`
	}
	return []byte(out + `</nzb>`)
}

// An NZB whose files are all recognisably non-media is unavailable - queued
// failed so the Arr blocklists it - instead of a bare error the Arr retries
// forever. Obfuscated names need network detection, which a timeout can cut
// short, so they are never tagged that way.
func TestNoValidFileGroups(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	_, _, err := p.Parse(context.Background(), "x", noGroupsNZB("release.nfo", "release.sfv"))
	if err == nil || !errors.Is(err, ErrReleaseUnavailable) {
		t.Fatalf("NZB with no media: err = %v, want ErrReleaseUnavailable", err)
	}

	obfuscated := nzbparser.NzbFiles{{Filename: "a8f3e2b1c4d5", Segments: nzbparser.NzbSegments{{Number: 1, Id: "x@y"}}}}
	if !needsContentDetection(p, obfuscated) {
		t.Fatal("an obfuscated name must count as needing content detection")
	}
	known := nzbparser.NzbFiles{{Filename: "release.nfo", Segments: nzbparser.NzbSegments{{Number: 1, Id: "x@y"}}}}
	if needsContentDetection(p, known) || !holdsOnlyKnownNonMedia(p, known) {
		t.Fatal("a .nfo needs no content detection and is known non-media")
	}
	// No file with any articles: malformed input, not an unavailable release.
	if holdsOnlyKnownNonMedia(p, nzbparser.NzbFiles{{Filename: "x.nfo"}}) {
		t.Fatal("an NZB without articles must keep the plain error")
	}
}
