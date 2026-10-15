package parser

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sirrobot01/decypharr/internal/crypto"
)

type rar5Encryption struct {
	Encrypted bool
	Key, IV   []byte
}

// rar5CheckIntact reports whether a RAR5 password check value is present
// and undamaged: its last 4 bytes are the SHA-256 of its first 8.
func rar5CheckIntact(check []byte) bool {
	if len(check) != 12 {
		return false
	}
	sum := sha256.Sum256(check[:8])
	return bytes.Equal(sum[:4], check[8:])
}

// parseRAR5Extra reads each record within its declared size.
// Record sizes include the variable-length type field.
// Format: https://www.rarlab.com/technote.htm
func parseRAR5Extra(data []byte, password string) (rar5Encryption, error) {
	var encryption rar5Encryption
	for len(data) > 0 {
		size, n := binary.Uvarint(data)
		if n <= 0 {
			return rar5Encryption{}, fmt.Errorf("invalid RAR5 extra record size")
		}
		data = data[n:]
		if size == 0 || size > uint64(len(data)) {
			return rar5Encryption{}, io.ErrUnexpectedEOF
		}
		record := bytes.NewReader(data[:size])
		data = data[size:]
		kind, err := binary.ReadUvarint(record)
		if err != nil {
			return rar5Encryption{}, fmt.Errorf("RAR5 extra record type: %w", err)
		}
		if kind != RAR5ExtraTypeEncryption {
			continue
		}
		if encryption.Encrypted {
			return rar5Encryption{}, fmt.Errorf("duplicate RAR5 encryption record")
		}
		version, err := binary.ReadUvarint(record)
		if err != nil || version != 0 {
			return rar5Encryption{}, fmt.Errorf("invalid RAR5 encryption version")
		}
		flags, err := binary.ReadUvarint(record)
		if err != nil {
			return rar5Encryption{}, fmt.Errorf("RAR5 encryption flags: %w", err)
		}
		kdf, err := record.ReadByte()
		// Limit key derivation to 2^24 rounds per file.
		if err != nil || kdf > 24 {
			return rar5Encryption{}, fmt.Errorf("invalid RAR5 key derivation count")
		}
		var salt [16]byte
		if _, err := io.ReadFull(record, salt[:]); err != nil {
			return rar5Encryption{}, fmt.Errorf("RAR5 salt: %w", err)
		}
		iv := make([]byte, 16)
		if _, err := io.ReadFull(record, iv); err != nil {
			return rar5Encryption{}, fmt.Errorf("RAR5 IV: %w", err)
		}
		var check []byte
		if flags&1 != 0 {
			check = make([]byte, 12)
			if _, err := io.ReadFull(record, check); err != nil {
				return rar5Encryption{}, fmt.Errorf("RAR5 password check: %w", err)
			}
		}
		encryption.Encrypted = true
		encryption.IV = iv
		if password != "" {
			keys := crypto.DeriveKeys([]byte(password), salt[:], int(kdf))
			// A wrong password gets no key, so the file is refused rather
			// than served as ciphertext. As unrar does, a check value whose
			// own checksum is wrong is damaged and proves nothing.
			if !rar5CheckIntact(check) || crypto.VerifyPassword(keys, check) {
				encryption.Key = keys.Key
			}
		}
	}
	return encryption, nil
}
