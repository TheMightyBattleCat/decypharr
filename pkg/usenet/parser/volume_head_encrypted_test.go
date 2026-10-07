package parser

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/sirrobot01/decypharr/internal/crypto"
)

// encryptedVolumeHead builds the start of a RAR5 volume whose headers are
// encrypted with password: signature, encryption header, IV, main header.
func encryptedVolumeHead(password string, salt [16]byte, number int, numbered bool) []byte {
	const kdf = 4
	keys := crypto.DeriveKeys([]byte(password), salt[:], kdf)

	enc := []byte{byte(RAR5HeaderTypeEncrypt), 0, 0, 0, kdf} // type, flags, version, enc flags, kdf count
	enc = append(enc, salt[:]...)
	out := append([]byte{}, rar5Signature...)
	out = append(out, withCRC(enc)...)

	archiveFlags := byte(rar5ArchiveFlagVolume)
	main := []byte{1, 0, archiveFlags} // type, flags, archive flags
	if numbered {
		main[2] |= byte(RAR5MainFlagVolumeNumber)
		main = append(main, byte(number))
	}
	plain := withCRC(main)
	for len(plain)%aes.BlockSize != 0 {
		plain = append(plain, 0)
	}
	iv := []byte("0123456789abcdef")
	block, _ := aes.NewCipher(keys.Key)
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(plain, plain)
	out = append(out, iv...)
	out = append(out, plain...)
	return append(out, make([]byte, 64)...) // file data follows
}

// withCRC frames a RAR5 header body: CRC32, size, body.
func withCRC(body []byte) []byte {
	sized := append([]byte{byte(len(body))}, body...)
	return append(binary.LittleEndian.AppendUint32(nil, crc32.ChecksumIEEE(sized)), sized...)
}

func TestVolumeHeadReaderDecryptsEncryptedHeaders(t *testing.T) {
	salt := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	third := encryptedVolumeHead("secret", salt, 3, true)
	first := encryptedVolumeHead("secret", salt, 0, false)

	// Without the password the head says nothing, as before.
	if h := ReadVolumeHead(third); h.Version != 0 {
		t.Fatalf("an encrypted head read without a key: %+v", h)
	}
	r := VolumeHeadReader{Password: "secret"}
	if h := r.Read(third); h.Version != 5 || !h.Volume || !h.HasNumber || h.Number != 3 {
		t.Fatalf("got %+v", h)
	}
	// The first volume carries no number field: it is volume 0.
	if h := r.Read(first); h.Version != 5 || !h.HasNumber || h.Number != 0 {
		t.Fatalf("first volume: got %+v", h)
	}
	if len(r.keys) != 1 {
		t.Fatalf("one salt should derive one key, derived %d", len(r.keys))
	}
	// A wrong password decrypts to noise, which the header CRC rejects.
	wrong := VolumeHeadReader{Password: "other"}
	if h := wrong.Read(third); h.Version != 0 {
		t.Fatalf("a wrong password gave a head: %+v", h)
	}
	// Cut before the main header ends.
	if h := r.Read(third[:len(third)-64-8]); h.Version != 0 {
		t.Fatalf("a cut-off head gave: %+v", h)
	}
	// Plain heads read as they always did.
	plain := append(append([]byte{}, rar5Signature...), withCRC([]byte{1, 0, byte(rar5ArchiveFlagVolume | RAR5MainFlagVolumeNumber), 7})...)
	if h := r.Read(plain); h != ReadVolumeHead(plain) || h.Number != 7 {
		t.Fatalf("plain head: got %+v", h)
	}
}
