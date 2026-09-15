//go:build cgo

package yenc

import (
	"errors"
	"io"
	"sync"

	"github.com/Tensai75/rapidyenc"
)

// IsCorruptArticle reports whether err is rapidyenc rejecting the article it
// decoded: short or long against its =ypart size, a CRC32 mismatch, or no
// =yend trailer. That describes the copy this provider holds, not the
// connection or the reader, and another backbone can hold an intact copy.
func IsCorruptArticle(err error) bool {
	return errors.Is(err, rapidyenc.ErrDataCorruption) ||
		errors.Is(err, rapidyenc.ErrCrcMismatch) ||
		errors.Is(err, rapidyenc.ErrDataMissing)
}

// rapidyencAdapter wraps a rapidyenc.Decoder to sync Meta after each Read.
type rapidyencAdapter struct {
	dec     *rapidyenc.Decoder
	yencDec *Decoder
}

var rapidyencAdapterPool = sync.Pool{
	New: func() any {
		yd := &Decoder{}
		return &rapidyencAdapter{yencDec: yd}
	},
}

func (a *rapidyencAdapter) Read(p []byte) (int, error) {
	n, err := a.dec.Read(p)
	// Sync meta from rapidyenc after each read (headers parsed lazily)
	m := a.dec.Meta
	a.yencDec.Meta = DecoderMeta{
		FileName:   m.FileName,
		FileSize:   m.FileSize,
		PartNumber: m.PartNumber,
		TotalParts: m.TotalParts,
		Offset:     m.Offset,
		PartSize:   m.PartSize,
	}
	return n, err
}

// AcquireDecoder returns a Decoder backed by rapidyenc (CGO).
func AcquireDecoder(r io.Reader) *Decoder {
	if UsePureGo {
		return acquirePureGoDecoder(r)
	}
	adapter := rapidyencAdapterPool.Get().(*rapidyencAdapter)
	adapter.dec = rapidyenc.AcquireDecoder(r)
	adapter.yencDec.Reader = adapter
	adapter.yencDec.Meta = DecoderMeta{}
	return adapter.yencDec
}

// Backend names the yEnc decoder in use, for the startup log.
func Backend() string {
	if UsePureGo {
		return "pure-go (YENC_PURE_GO=true; no CRC32 or size check)"
	}
	return "rapidyenc (SIMD; verifies CRC32 and part size)"
}

// ReleaseDecoder returns the underlying rapidyenc decoder to the pool.
func ReleaseDecoder(dec *Decoder) {
	if dec == nil {
		return
	}
	if adapter, ok := dec.Reader.(*rapidyencAdapter); ok {
		rapidyenc.ReleaseDecoder(adapter.dec)
		adapter.dec = nil
		dec.Reader = nil
		dec.Meta = DecoderMeta{}
		rapidyencAdapterPool.Put(adapter)
	}
}
