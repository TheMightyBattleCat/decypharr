package parser

import (
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Leading bytes of real volumes read on a production install 2026-09-16: Emberly
// FraMeSToR RAR5 volumes 1 and 3, Lamplight FraMeSToR RAR4 volume 2.
func TestReadVolumeHead(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		want VolumeHead
	}{
		{"rar5 first volume has no number field", "526172211a0701001caf232c1001050c010b0101ebfdfff98180808000946e68435f02030bfc9c17", VolumeHead{Version: 5, Volume: true, Number: 0, HasNumber: true}},
		{"rar5 third volume", "526172211a070100c63c9e671101050c03020b0101dffefff981808080003b71de6867021b0bddfdff", VolumeHead{Version: 5, Volume: true, Number: 2, HasNumber: true}},
		{"rar4 second volume", "526172211a0700b97d7351000d00000000000000800374c38148000455a81763857f3703e459accf", VolumeHead{Version: 4, Volume: true}},
		{"rar4 first volume flag", "526172211a0700b97d7351010d00000000000000", VolumeHead{Version: 4, Volume: true, First: true}},
		{"mid-volume article", "b3fa5947243ca88903f53162bf162acaaa2b1a79a78d7e3da5ad2ed2e4273f43", VolumeHead{}},
		{"truncated rar5", "526172211a070100c63c9e67", VolumeHead{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ReadVolumeHead(mustHex(t, c.hex)); got != c.want {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestNameVolumeNumber(t *testing.T) {
	cases := []struct {
		name string
		n    int
		ok   bool
	}{
		{"bpnq7qsgP6n9ta3yOKKycChxNYBtr79.part02.rar", 2, true},
		{"Show.S01E01.rar", 0, true},
		{"Show.S01E01.r07", 8, true},
		{"Show.S01E01.012", 12, true},
		{"wAwUwpTqROwW48gqlZQpkDSOQGYEung3DmwsWzeutJbEQmIh", 0, false},
		{"9ae7a116d98fab6b", 0, false},
		{"notes.nfo", 0, false},
	}
	for _, c := range cases {
		n, ok := NameVolumeNumber(c.name)
		if n != c.n || ok != c.ok {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", c.name, n, ok, c.n, c.ok)
		}
	}
}
