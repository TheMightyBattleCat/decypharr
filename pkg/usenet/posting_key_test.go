package usenet

import "testing"

func nzbXML(meta string, files ...[2]string) []byte {
	out := `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><head>` + meta + `</head>`
	for _, f := range files {
		out += `<file poster="p" date="1" subject="` + f[0] + `"><groups><group>a.b</group></groups><segments>` +
			`<segment bytes="10" number="2">` + f[1] + `-2@x</segment>` +
			`<segment bytes="10" number="1">` + f[1] + `-1@x</segment>` +
			`</segments></file>`
	}
	return []byte(out + `</nzb>`)
}

// Another indexer's NZB for the same upload - different bytes, file order and
// metadata, same articles - shares the posting key, so a posting marked dead
// is refused however it is re-listed. A different upload does not.
func TestPostingKeyFromNZB(t *testing.T) {
	a := nzbXML(`<meta type="name">Under.Reef.S11E06</meta>`, [2]string{"r00", "aa"}, [2]string{"r01", "bb"})
	b := nzbXML(`<meta type="category">TV</meta>`, [2]string{"part01", "bb"}, [2]string{"part00", "aa"})
	other := nzbXML(``, [2]string{"r00", "cc"}, [2]string{"r01", "bb"})

	ka, kb := postingKeyFromNZB(a), postingKeyFromNZB(b)
	if ka == "" || ka != kb {
		t.Fatalf("same posting, different NZB bytes: %q vs %q", ka, kb)
	}
	if hashNZBContent(a) == hashNZBContent(b) {
		t.Fatal("precondition: the byte hashes differ")
	}
	if postingKeyFromNZB(other) == ka {
		t.Fatal("a different upload shares the posting key")
	}
	if postingKeyFromNZB([]byte("not xml")) != "" || postingKeyFromNZB(nzbXML("")) != "" {
		t.Fatal("no articles must give no key")
	}

	c := newDeadPostingCache()
	c.Mark(ka)
	if !c.Check(kb) {
		t.Fatal("re-listed copy of a dead posting not refused")
	}
}
