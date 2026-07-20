package par2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// FileDesc is one recovery-set member's identity, decoded from a FileDesc
// packet.
type FileDesc struct {
	FileID  [16]byte
	FileMD5 [16]byte // MD5 of the whole file
	MD5_16k [16]byte // MD5 of the first 16KB of the file (or the whole file if shorter)
	Length  int64
	Name    string
}

// SliceChecksum is one input slice's expected MD5+CRC32, decoded from an
// IFSC packet.
type SliceChecksum struct {
	MD5   [16]byte
	CRC32 uint32
}

// RecoverySliceRef locates one RecvSlic packet's recovery data within the
// caller-supplied source list passed to ParseIndex, without copying it -
// index building only needs to know where the data is, not hold it.
type RecoverySliceRef struct {
	Exponent uint32
	Source   int   // index into the []Source given to ParseIndex
	Offset   int64 // byte offset of the recovery data (after the 4-byte exponent) within that source
	Length   int64 // length of the recovery data (== Index.SliceSize for a well-formed packet)
}

// Index is the fully-parsed, deduplicated PAR2 metadata for one recovery
// set, aggregated across every source scanned by ParseIndex.
type Index struct {
	SetID     [16]byte
	SliceSize int64

	// FileOrder is the Main packet's recovery-set FileIDs, sorted ascending -
	// this defines the base offset of each file's slices within the whole
	// recovery set's global slice index space (see SliceBase).
	FileOrder [][16]byte

	Files  map[[16]byte]*FileDesc
	Slices map[[16]byte][]SliceChecksum // by FileID; one entry per slice of that file, in slice order

	Recovery []RecoverySliceRef

	// fileBase[i] is the global slice index at which FileOrder[i]'s slices
	// begin. Same length and order as FileOrder; built once by finalize.
	fileBase  []int64
	numSlices int64
}

// Source is one PAR2 file's raw bytes, for ParseIndex.
type Source struct {
	Name string
	Data []byte
}

// ParseIndex scans every source (the index .par2 file, any recovery volumes,
// or both - PAR2 duplicates Main/FileDesc/IFSC packets across every file in
// the set, precisely so any one of them can rebuild the index if another is
// unavailable) and returns the aggregated, deduplicated metadata. Duplicate
// packets (identical content, appearing in more than one source) are
// recognized by packet MD5 and only processed once. RecvSlic packet bodies
// are never copied - only their location is recorded (RecoverySliceRef) -
// so scanning even a source that includes large recovery volumes stays cheap.
func ParseIndex(sources []Source) (*Index, error) {
	idx := &Index{
		Files:  make(map[[16]byte]*FileDesc),
		Slices: make(map[[16]byte][]SliceChecksum),
	}
	seen := make(map[[16]byte]struct{})
	haveSetID := false

	for srcIdx, src := range sources {
		err := walkPackets(src.Data, func(h packetHeader, packet []byte, offset int64) error {
			if !haveSetID {
				idx.SetID = h.RecoverySetID
				haveSetID = true
			} else if h.RecoverySetID != idx.SetID {
				return fmt.Errorf("par2: %s: packet at offset %d belongs to a different recovery set", src.Name, offset)
			}

			if _, dup := seen[h.MD5]; dup {
				return nil
			}
			seen[h.MD5] = struct{}{}

			body := packet[packetHeaderSize:]
			switch h.Type {
			case typeMain:
				return idx.parseMain(body)
			case typeFileDesc:
				return idx.parseFileDesc(body)
			case typeIFSC:
				return idx.parseIFSC(body)
			case typeRecvSlic:
				return idx.parseRecvSlic(srcIdx, offset, body)
			default:
				return nil // Creator or any other/future packet type - ignore
			}
		})
		if err != nil {
			return nil, fmt.Errorf("par2: %s: %w", src.Name, err)
		}
	}

	if !haveSetID {
		return nil, fmt.Errorf("par2: no PAR2 packets found in any source")
	}
	if idx.SliceSize <= 0 || idx.FileOrder == nil {
		return nil, fmt.Errorf("par2: no Main packet found")
	}
	if err := idx.finalize(); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *Index) parseMain(body []byte) error {
	if len(body) < 12 {
		return fmt.Errorf("truncated Main packet")
	}
	sliceSize := int64(binary.LittleEndian.Uint64(body[0:8]))
	if sliceSize <= 0 || sliceSize%4 != 0 {
		return fmt.Errorf("invalid Main packet slice size %d", sliceSize)
	}
	numFiles := binary.LittleEndian.Uint32(body[8:12])
	need := 12 + int64(numFiles)*16
	if int64(len(body)) < need {
		return fmt.Errorf("truncated Main packet file list")
	}
	ids := make([][16]byte, numFiles)
	for i := range ids {
		copy(ids[i][:], body[12+i*16:12+(i+1)*16])
	}
	// FileID ordering is a little-endian numeric comparison (compare from
	// the last byte down to the first), NOT byte-lexicographic bytes.Compare
	// - PAR2 v2.0's Main packet stores each FileID as a 16-byte value with
	// this comparison defining "sorted", and the recovery data's per-column
	// generator assignment depends on getting this order exactly right.
	sort.Slice(ids, func(a, b int) bool { return fileIDLess(ids[a], ids[b]) })

	idx.SliceSize = sliceSize
	idx.FileOrder = ids
	return nil
}

func (idx *Index) parseFileDesc(body []byte) error {
	const fixed = 16 + 16 + 16 + 8
	if len(body) < fixed {
		return fmt.Errorf("truncated FileDesc packet")
	}
	fd := &FileDesc{}
	copy(fd.FileID[:], body[0:16])
	copy(fd.FileMD5[:], body[16:32])
	copy(fd.MD5_16k[:], body[32:48])
	fd.Length = int64(binary.LittleEndian.Uint64(body[48:56]))

	name := body[fixed:]
	if i := bytes.IndexByte(name, 0); i >= 0 {
		name = name[:i]
	}
	fd.Name = string(name)

	idx.Files[fd.FileID] = fd
	return nil
}

func (idx *Index) parseIFSC(body []byte) error {
	if len(body) < 16 {
		return fmt.Errorf("truncated IFSC packet")
	}
	var fileID [16]byte
	copy(fileID[:], body[0:16])

	rest := body[16:]
	const entrySize = 16 + 4
	if len(rest)%entrySize != 0 {
		return fmt.Errorf("malformed IFSC packet: %d bytes of checksum data", len(rest))
	}
	n := len(rest) / entrySize
	checks := make([]SliceChecksum, n)
	for i := range checks {
		off := i * entrySize
		copy(checks[i].MD5[:], rest[off:off+16])
		checks[i].CRC32 = binary.LittleEndian.Uint32(rest[off+16 : off+20])
	}
	idx.Slices[fileID] = checks
	return nil
}

func (idx *Index) parseRecvSlic(srcIdx int, packetOffset int64, body []byte) error {
	if len(body) < 4 {
		return fmt.Errorf("truncated RecvSlic packet")
	}
	exponent := binary.LittleEndian.Uint32(body[0:4])
	idx.Recovery = append(idx.Recovery, RecoverySliceRef{
		Exponent: exponent,
		Source:   srcIdx,
		Offset:   packetOffset + packetHeaderSize + 4,
		Length:   int64(len(body) - 4),
	})
	return nil
}

// finalize computes fileBase/numSlices from FileOrder + Files, and validates
// that every file Main references actually has a FileDesc.
func (idx *Index) finalize() error {
	idx.fileBase = make([]int64, len(idx.FileOrder))
	total := int64(0)
	for i, id := range idx.FileOrder {
		fd, ok := idx.Files[id]
		if !ok {
			return fmt.Errorf("par2: Main packet references file %x with no FileDesc packet", id)
		}
		idx.fileBase[i] = total
		total += ceilDiv(fd.Length, idx.SliceSize)
	}
	idx.numSlices = total
	if total > maxInputSlices {
		return fmt.Errorf("par2: recovery set has %d input slices, exceeding the %d spec limit", total, maxInputSlices)
	}
	return nil
}

// NumSlices returns the total number of input slices across every file in
// the recovery set.
func (idx *Index) NumSlices() int64 { return idx.numSlices }

// SliceBase returns the global slice index at which fileID's slices begin.
func (idx *Index) SliceBase(fileID [16]byte) (int64, error) {
	for i, id := range idx.FileOrder {
		if id == fileID {
			return idx.fileBase[i], nil
		}
	}
	return 0, fmt.Errorf("par2: file %x is not in the recovery set", fileID)
}

// sliceLocation returns the FileID and within-file slice number a global
// slice index belongs to.
func (idx *Index) sliceLocation(globalIdx int64) (fileID [16]byte, local int64, err error) {
	if globalIdx < 0 || globalIdx >= idx.numSlices {
		return fileID, 0, fmt.Errorf("par2: slice index %d out of range [0, %d)", globalIdx, idx.numSlices)
	}
	// Linear scan: FileOrder is small (one entry per file in the release,
	// typically single digits to low hundreds), so this is cheap relative to
	// the streaming pass that calls it once per slice.
	for i, base := range idx.fileBase {
		var end int64
		if i+1 < len(idx.fileBase) {
			end = idx.fileBase[i+1]
		} else {
			end = idx.numSlices
		}
		if globalIdx >= base && globalIdx < end {
			return idx.FileOrder[i], globalIdx - base, nil
		}
	}
	return fileID, 0, fmt.Errorf("par2: slice index %d not found in any file's range", globalIdx)
}

// DamagedSlices returns the global slice indices fully or partially covered
// by the byte range [byteStart, byteEnd) of the posted file identified by
// fileID. A slice with any damaged byte is treated as fully damaged - PAR2
// repair always operates at whole-slice granularity.
func (idx *Index) DamagedSlices(fileID [16]byte, byteStart, byteEnd int64) ([]int64, error) {
	if byteEnd <= byteStart {
		return nil, nil
	}
	base, err := idx.SliceBase(fileID)
	if err != nil {
		return nil, err
	}
	startSlice := base + byteStart/idx.SliceSize
	endSlice := base + (byteEnd-1)/idx.SliceSize
	out := make([]int64, 0, endSlice-startSlice+1)
	for s := startSlice; s <= endSlice; s++ {
		out = append(out, s)
	}
	return out, nil
}

// verifySliceChecksum reports whether data (exactly SliceSize bytes) matches
// the recorded IFSC MD5 and CRC32 for global slice index globalIdx.
func (idx *Index) verifySliceChecksum(globalIdx int64, data []byte) (bool, error) {
	fileID, local, err := idx.sliceLocation(globalIdx)
	if err != nil {
		return false, err
	}
	checks := idx.Slices[fileID]
	if local < 0 || local >= int64(len(checks)) {
		return false, fmt.Errorf("par2: no IFSC checksum for slice %d (file %x, local slice %d)", globalIdx, fileID, local)
	}
	want := checks[local]
	return md5AndCRC32(data) == want, nil
}

// TrimSlice returns data (exactly SliceSize bytes) trimmed to the real
// (unpadded) length of its slice: every slice is full-length except a
// file's final slice, which PAR2 zero-pads out to SliceSize.
func (idx *Index) TrimSlice(globalIdx int64, data []byte) ([]byte, error) {
	fileID, local, err := idx.sliceLocation(globalIdx)
	if err != nil {
		return nil, err
	}
	fd, ok := idx.Files[fileID]
	if !ok {
		return nil, fmt.Errorf("par2: no FileDesc for file %x", fileID)
	}
	sliceStart := local * idx.SliceSize
	remaining := fd.Length - sliceStart
	if remaining >= int64(len(data)) {
		return data, nil
	}
	if remaining < 0 {
		return nil, fmt.Errorf("par2: slice %d starts past the end of file %x", globalIdx, fileID)
	}
	return data[:remaining], nil
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// fileIDLess orders two FileIDs as PAR2 v2.0 defines "sorted" for the Main
// packet's recovery-set list: a little-endian numeric comparison, i.e.
// starting from the LAST byte and working down to the first - not a
// byte-lexicographic bytes.Compare.
func fileIDLess(a, b [16]byte) bool {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
