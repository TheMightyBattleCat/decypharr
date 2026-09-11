package types

import "github.com/sirrobot01/decypharr/pkg/storage"

type Volume struct {
	Index         int
	Name          string
	Size          int64
	Segments      []storage.NZBSegment
	IsEncrypted   bool   // True if data is encrypted
	EncryptionKey []byte // AES-256 key for decryption (32 bytes)
	EncryptionIV  []byte // AES IV for decryption (16 bytes)
}

// RARVolumePart describes each archive part across volumes (internal parser use only)
type RARVolumePart struct {
	Name              string
	DataOffset        int64
	PackedSize        int64
	UnpackedSize      int64
	Stored            bool
	Compressed        bool
	PartNumber        int
	CompressionMethod string
	// TrimmedBytes is how much of the header's data size the RAR4 parser cut
	// off because it ran past the volume's size in our geometry. A real
	// archive never needs that, so non-zero means that size is wrong.
	TrimmedBytes int64
}
