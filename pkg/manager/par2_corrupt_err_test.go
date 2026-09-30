package manager

import (
	"github.com/sirrobot01/decypharr/internal/nntp"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// corruptErr is the error ExecuteWithFailover returns when every provider's
// copy fails its CRC.
var corruptErr error = &nntp.Error{Type: nntp.ErrorTypeYencDecode, Message: "crc", Err: nntpyenc.ErrCrcMismatch}
