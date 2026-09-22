//go:build cgo

package manager

import (
	"github.com/Tensai75/rapidyenc"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// corruptErr is the error ExecuteWithFailover returns when every provider's
// copy fails its CRC.
var corruptErr error = &nntp.Error{Type: nntp.ErrorTypeYencDecode, Message: "crc", Err: rapidyenc.ErrCrcMismatch}
