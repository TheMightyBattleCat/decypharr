//go:build !cgo

package yenc

import (
	"io"
)

// AcquireDecoder returns a Decoder backed by the in-repo pure-Go decoder.
func AcquireDecoder(r io.Reader) *Decoder {
	return acquirePureGoDecoder(r)
}

// Backend names the yEnc decoder in use, for the startup log.
func Backend() string {
	return "pure-go (built without cgo; no CRC32 or size check)"
}

// ReleaseDecoder is a no-op for the pure-Go backend.
func ReleaseDecoder(dec *Decoder) {}
