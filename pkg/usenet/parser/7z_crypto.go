package parser

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf16"

	"github.com/javi11/sevenzip"
)

// max7zKDFIterations bounds the SHA-256 rounds one 7z key may cost, as
// parseRAR5Extra bounds RAR5's: 7-Zip writes 2^19, and a crafted NZB must
// not buy minutes of CPU per password candidate.
const max7zKDFIterations = 1 << 24

type sevenZipKeyID struct {
	password   string
	salt       string
	iterations int
}

var sevenZipKeys sync.Map // sevenZipKeyID -> []byte

// sevenZipKey derives 7-Zip's AES-256 key: SHA-256 over iterations rounds of
// salt, the UTF-16LE password and a little-endian round counter. Zero
// iterations is 7-Zip's "no hashing" form (cycles 0x3F), where the key is
// salt and password themselves, zero-padded. The archive's every folder
// usually shares one salt and count, so keys are cached.
func sevenZipKey(password string, salt []byte, iterations int) ([]byte, error) {
	if iterations < 0 || iterations > max7zKDFIterations {
		return nil, fmt.Errorf("7z key derivation asks %d rounds, over the %d allowed", iterations, max7zKDFIterations)
	}
	id := sevenZipKeyID{password, string(salt), iterations}
	if key, ok := sevenZipKeys.Load(id); ok {
		return key.([]byte), nil
	}
	data := append([]byte(nil), salt...)
	for _, u := range utf16.Encode([]rune(password)) {
		data = append(data, byte(u), byte(u>>8))
	}
	key := make([]byte, sha256.Size)
	if iterations == 0 {
		copy(key, data)
	} else {
		h := sha256.New()
		var counter [8]byte
		for i := range iterations {
			h.Write(data)
			binary.LittleEndian.PutUint64(counter[:], uint64(i))
			h.Write(counter[:])
		}
		copy(key, h.Sum(nil))
	}
	sevenZipKeys.Store(id, key)
	return key, nil
}

// mediaHeadMatches reports whether head, a file's first 16 bytes, starts
// the way a file of name's type must. known is false for a type with no
// fixed opening, whose head proves nothing either way.
func mediaHeadMatches(name string, head []byte) (known, ok bool) {
	if len(head) < 16 {
		return false, false
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mka", ".mk3d", ".webm":
		return true, bytes.HasPrefix(head, []byte{0x1a, 0x45, 0xdf, 0xa3})
	case ".mp4", ".m4v", ".mov", ".m4a", ".3gp":
		box := string(head[4:8])
		return true, box == "ftyp" || box == "moov" || box == "mdat" || box == "free" || box == "wide" || box == "skip"
	case ".avi":
		return true, string(head[0:4]) == "RIFF" && string(head[8:12]) == "AVI "
	case ".mpg", ".mpeg", ".vob":
		return true, bytes.HasPrefix(head, []byte{0x00, 0x00, 0x01, 0xba})
	case ".wmv", ".asf":
		return true, bytes.HasPrefix(head, []byte{0x30, 0x26, 0xb2, 0x75})
	case ".flv":
		return true, string(head[0:3]) == "FLV"
	case ".ts":
		return true, head[0] == 0x47
	case ".m2ts", ".mts":
		return true, head[4] == 0x47
	}
	return false, false
}

// decrypt7zHead decrypts the first AES block of an encrypted 7z entry. The
// entry must open its own folder, so its first block is the folder's.
func decrypt7zHead(file sevenzip.FileInfo, password string, head []byte) ([]byte, error) {
	key, err := sevenZipKey(password, file.AESSalt, file.KDFIterations)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, aes.BlockSize)
	copy(iv, file.AESIV)
	out := make([]byte, aes.BlockSize)
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, head[:aes.BlockSize])
	return out, nil
}

// choose7zPassword picks the candidate that opens an archive's encrypted
// entries. heads holds the encrypted first block of each entry whose type
// has a fixed opening (mediaHeadMatches); a candidate is right when it
// decrypts every one of them to its type's opening. With no such entry
// nothing can tell candidates apart, and the first is taken unverified.
// verified reports which case applied.
func choose7zPassword(entries []sevenzip.FileInfo, heads map[int][]byte, candidates []string) (password string, verified bool, err error) {
	if len(candidates) == 0 {
		return "", false, ErrArchivePassword
	}
	if len(heads) == 0 {
		return candidates[0], false, nil
	}
	for _, candidate := range candidates {
		opens := true
		for i, head := range heads {
			plain, err := decrypt7zHead(entries[i], candidate, head)
			if err != nil {
				return "", false, err
			}
			if _, ok := mediaHeadMatches(entries[i].Name, plain); !ok {
				opens = false
				break
			}
		}
		if opens {
			return candidate, true, nil
		}
	}
	return "", false, ErrArchivePassword
}

var (
	sevenZipSignature = []byte{'7', 'z', 0xbc, 0xaf, 0x27, 0x1c}
	sevenZipAESMethod = []byte{0x06, 0xf1, 0x07, 0x01}
)

// sevenZipHeaderEncrypted reports whether a 7z archive's header - its file
// list - is encrypted (7z -mhe), so that it cannot be listed without the
// password. The library reports a wrong password there only as a decoding
// error of the header; this tells that case apart from a damaged archive.
// The signature header gives the next header's place; an encrypted one is
// an encoded header (0x17) whose coders include 7-Zip's AES method.
func sevenZipHeaderEncrypted(r io.ReaderAt, size int64) bool {
	sig := make([]byte, 32)
	if _, err := r.ReadAt(sig, 0); err != nil || !bytes.HasPrefix(sig, sevenZipSignature) {
		return false
	}
	offset := binary.LittleEndian.Uint64(sig[12:20])
	length := binary.LittleEndian.Uint64(sig[20:28])
	if length == 0 || length > 1<<16 || offset > uint64(size) || 32+offset+length > uint64(size) {
		return false
	}
	header := make([]byte, length)
	if _, err := r.ReadAt(header, int64(32+offset)); err != nil {
		return false
	}
	return header[0] == 0x17 && bytes.Contains(header, sevenZipAESMethod)
}

// check7zStreamable reports why an encrypted entry cannot be served by the
// reader's AES-CBC path, which decrypts one file as one chain from its first
// byte: the entry must be stored, not compressed, and alone in its folder,
// and its packed size must be its size padded to whole AES blocks.
func check7zStreamable(file sevenzip.FileInfo, folderFiles int) error {
	switch {
	case file.Compressed:
		return fmt.Errorf("%s is compressed inside an encrypted 7z; only stored (-mx0) encrypted archives can be streamed", file.Name)
	case folderFiles != 1:
		return fmt.Errorf("%s shares an encrypted 7z block with %d other files (solid archive); it cannot be streamed", file.Name, folderFiles-1)
	case file.PackedSize%aes.BlockSize != 0 || file.PackedSize < file.Size || file.PackedSize-file.Size >= aes.BlockSize:
		return fmt.Errorf("%s: encrypted 7z entry packs %d bytes for %d; not one AES-CBC stream", file.Name, file.PackedSize, file.Size)
	}
	return nil
}
