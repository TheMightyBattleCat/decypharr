package utils

import (
	"net"
	"strings"
	"testing"
)

func TestRedactURL(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"apikey", "https://idx.example/getnzb/1.nzb?apikey=SECRET&t=get", "https://idx.example/getnzb/1.nzb?apikey=***&t=get"},
		{"case insensitive", "https://idx.example/x?ApiKey=SECRET&R=S2&Token=S3&KEY=S4&passkey=S5&api_key=S6", "https://idx.example/x?ApiKey=***&R=***&Token=***&KEY=***&passkey=***&api_key=***"},
		{"other params kept", "https://idx.example/x?id=7&del=1", "https://idx.example/x?id=7&del=1"},
		{"no query", "https://idx.example/a.nzb", "https://idx.example/a.nzb"},
		{"userinfo password", "https://user:pw@idx.example/a.nzb", "https://user@idx.example/a.nzb"},
		{"malformed with host", "https://us er:pw@idx.example/a?apikey=SECRET%zz", "https://idx.example"},
		{"no scheme", "not a url ?apikey=SECRET", ""},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactURL(tc.in)
			if got != tc.want {
				t.Fatalf("RedactURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.Contains(got, "SECRET") || strings.Contains(got, "pw@") {
				t.Fatalf("secret left in %q", got)
			}
		})
	}
}

func TestDownloadFileErrorHasNoKey(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close() // nothing listens: connection refused
	_, _, err = DownloadFile("http://" + addr + "/dl?apikey=SECRET&r=SECRET2")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaks the key: %v", err)
	}
	_, _, err = DownloadFile("http://" + addr + "/dl?apikey=SECRET\x7f")
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("bad-URL error leaks the key: %v", err)
	}
}
