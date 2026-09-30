package reader

import (
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// An article every provider serves corrupt reaches the overlay (pad + repair)
// like a 430, instead of failing the read on every attempt.
func TestArticleUnservable(t *testing.T) {
	corrupt := &nntp.Error{Type: nntp.ErrorTypeYencDecode, Message: "crc", Err: nntpyenc.ErrCrcMismatch}
	if !articleUnservable(corrupt) {
		t.Error("corrupt on every provider not treated as unservable")
	}
	if !articleUnservable(&nntp.Error{Type: nntp.ErrorTypeArticleNotFound}) {
		t.Error("430 not treated as unservable")
	}
	for _, err := range []error{
		&nntp.Error{Type: nntp.ErrorTypeTimeout},
		&nntp.Error{Type: nntp.ErrorTypeYencDecode, Message: "writer", Err: errors.New("short write")},
		errors.New("reset"),
	} {
		if articleUnservable(err) {
			t.Errorf("%v treated as unservable", err)
		}
	}
}
