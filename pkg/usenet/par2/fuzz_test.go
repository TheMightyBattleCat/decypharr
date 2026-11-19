package par2

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fuzzDeadline bounds one input's parse. Go's fuzzer has no per-input hang
// detector, so a walk that never ends must fail the input itself.
const fuzzDeadline = 10 * time.Second

// mustEnd runs fn on the test goroutine. When fn has not returned by the
// deadline, a watchdog panics, which crashes the fuzz worker and makes the
// fuzzer record the input as failing.
func mustEnd(t *testing.T, fn func()) {
	t.Helper()
	watchdog := time.AfterFunc(fuzzDeadline, func() {
		panic(fmt.Sprintf("%s: parse did not end within %v", t.Name(), fuzzDeadline))
	})
	defer watchdog.Stop()
	fn()
}

// fuzzSeeds returns the committed PAR2 fixtures (index and recovery volumes).
func fuzzSeeds(f *testing.F) [][]byte {
	var out [][]byte
	ents, err := os.ReadDir(filepath.Join("testdata", "par2"))
	if err != nil {
		f.Fatal(err)
	}
	for _, e := range ents {
		if filepath.Ext(e.Name()) != ".par2" {
			continue
		}
		b, err := os.ReadFile(filepath.Join("testdata", "par2", e.Name()))
		if err != nil {
			f.Fatal(err)
		}
		out = append(out, b)
	}
	if len(out) == 0 {
		f.Fatal("no PAR2 fixtures found")
	}
	return out
}

// exerciseIndex checks a parsed index for negative sizes and counts and
// calls what the repair job calls on it.
func exerciseIndex(t *testing.T, idx *Index, sources []Source) {
	t.Helper()
	n := idx.NumSlices()
	if n < 0 || n > maxInputSlices {
		t.Fatalf("NumSlices %d outside [0,%d]", n, maxInputSlices)
	}
	if idx.SliceSize <= 0 || idx.SliceSize%4 != 0 || idx.SliceSize > maxSliceSize {
		t.Fatalf("accepted slice size %d", idx.SliceSize)
	}
	for _, fd := range idx.Files {
		if fd.Length < 0 {
			t.Fatalf("accepted FileDesc length %d", fd.Length)
		}
	}
	for _, r := range idx.Recovery {
		if r.Source < 0 || r.Source >= len(sources) {
			t.Fatalf("recovery ref names source %d of %d", r.Source, len(sources))
		}
		d := sources[r.Source].Data
		if r.Offset < 0 || r.Length < 0 || r.Offset+r.Length > int64(len(d)) {
			t.Fatalf("recovery ref [%d,+%d) outside its %d-byte source", r.Offset, r.Length, len(d))
		}
	}
	var posted []PostedFile
	for _, fid := range idx.FileOrder {
		fd := idx.Files[fid]
		if fd == nil {
			t.Fatalf("FileOrder entry without FileDesc survived finalize")
		}
		base, err := idx.SliceBase(fid)
		if err != nil {
			t.Fatalf("SliceBase: %v", err)
		}
		if base < 0 || base > n {
			t.Fatalf("SliceBase %d outside [0,%d]", base, n)
		}
		dup := 0
		for _, o := range idx.FileOrder {
			if o == fid {
				dup++
			}
		}
		for _, r := range [][2]int64{{0, fd.Length}, {0, 1}, {fd.Length - 1, fd.Length}, {fd.Length, fd.Length + 10}, {1, 1 << 62}} {
			if r[0] < 0 {
				continue // callers never pass a negative start
			}
			ds, err := idx.DamagedSlices(fid, r[0], r[1])
			if err != nil {
				continue
			}
			if int64(len(ds)) > n {
				t.Fatalf("DamagedSlices returned %d slices of %d", len(ds), n)
			}
			for _, g := range ds {
				if g < 0 || g >= n {
					t.Fatalf("DamagedSlices(%x,%d,%d) returned slice %d outside [0,%d)", fid[:4], r[0], r[1], g, n)
				}
				ofid, local, err := idx.SliceLocation(g)
				if err != nil {
					t.Fatalf("SliceLocation(%d): %v", g, err)
				}
				if local < 0 {
					t.Fatalf("SliceLocation(%d) gave local slice %d", g, local)
				}
				// A FileID listed twice in Main aliases; only distinct files count.
				if ofid != fid && dup == 1 {
					t.Fatalf("DamagedSlices of file %x returned slice %d owned by file %x", fid[:4], g, ofid[:4])
				}
			}
		}
		posted = append(posted, PostedFile{Name: fd.Name, Length: fd.Length, MD5_16k: func() ([16]byte, error) { return fd.MD5_16k, nil }})
	}
	if _, _, err := MatchFiles(idx, posted); err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if n > 0 && n <= 64 && idx.SliceSize <= 4096 {
		buf := make([]byte, idx.SliceSize)
		for g := int64(0); g < n; g++ {
			_, _ = idx.VerifySliceChecksum(g, buf)
			_, _ = idx.TrimSlice(g, buf)
		}
		// A one-slice repair with whatever recovery data the input offers.
		for _, r := range idx.Recovery {
			if r.Length != idx.SliceSize {
				continue
			}
			d := sources[r.Source].Data
			rec := []RecoverySlice{{Exponent: r.Exponent, Data: d[r.Offset : r.Offset+r.Length]}}
			_, _ = Repair(idx, []int64{0}, rec, zeroSource{size: idx.SliceSize})
			break
		}
	}
}

type zeroSource struct{ size int64 }

func (z zeroSource) ReadSlice(int64) ([]byte, error) { return make([]byte, z.size), nil }

// FuzzParseIndexRaw feeds arbitrary bytes as one .par2 file. Raw mutation
// rarely keeps a packet MD5 valid, so this mostly covers the packet walk.
func FuzzParseIndexRaw(f *testing.F) {
	for _, s := range fuzzSeeds(f) {
		f.Add(s)
		if len(s) > 200 {
			f.Add(s[:200])
			f.Add(s[100:])
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		srcs := []Source{{Name: "a.par2", Data: data}}
		mustEnd(t, func() {
			_ = SourceSetIDs(context.Background(), srcs)
			idx, err := ParseIndex(context.Background(), srcs)
			if err == nil {
				exerciseIndex(t, idx, srcs)
			}
		})
	})
}

// fuzzPacket wraps body in a packet header with a valid packet MD5.
func fuzzPacket(setID [16]byte, typ packetType, body []byte) []byte {
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	p := make([]byte, packetHeaderSize+len(body))
	copy(p[0:8], packetMagic[:])
	binary.LittleEndian.PutUint64(p[8:16], uint64(len(p)))
	copy(p[32:48], setID[:])
	copy(p[48:64], typ[:])
	copy(p[64:], body)
	sum := md5.Sum(p[32:])
	copy(p[16:32], sum[:])
	return p
}

// FuzzParseIndexStructured builds packets with VALID packet MD5s from the
// fuzz bytes, which a hostile poster can do and raw mutation cannot reach.
// Input: records of [kind byte][len byte][len bytes of body]; kind's low two
// bits pick Main/FileDesc/IFSC/RecvSlic, its top bit a second recovery set.
func FuzzParseIndexStructured(f *testing.F) {
	main := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(main[0:8], 8)
	binary.LittleEndian.PutUint32(main[8:12], 1)
	main[12] = 7
	fdesc := make([]byte, 56+8)
	fdesc[0] = 7
	binary.LittleEndian.PutUint64(fdesc[48:56], 20)
	copy(fdesc[56:], "a.bin")
	ifsc := make([]byte, 16+3*20)
	ifsc[0] = 7
	recv := make([]byte, 4+8)
	rec := func(kind byte, body []byte) []byte {
		return append([]byte{kind, byte(len(body))}, body...)
	}
	f.Add(append(append(append(rec(0, main), rec(1, fdesc)...), rec(2, ifsc)...), rec(3, recv)...))
	// Hostile seeds: huge file length, negative length, huge file count, and
	// a second set mixed in.
	big := append([]byte(nil), fdesc...)
	binary.LittleEndian.PutUint64(big[48:56], 0x7fffffffffffffff)
	f.Add(append(append(rec(0, main), rec(1, big)...), rec(3, recv)...))
	neg := append([]byte(nil), fdesc...)
	binary.LittleEndian.PutUint64(neg[48:56], 0xffffffffffffff00)
	f.Add(append(rec(0, main), rec(1, neg)...))
	manyMain := append([]byte(nil), main...)
	binary.LittleEndian.PutUint32(manyMain[8:12], 0xffffffff)
	f.Add(append(rec(0, manyMain), rec(1, fdesc)...))
	f.Add(append(append(append(rec(0x80, main), rec(0, main)...), rec(1, fdesc)...), rec(0x81, fdesc)...))

	types := []packetType{typeMain, typeFileDesc, typeIFSC, typeRecvSlic}
	f.Fuzz(func(t *testing.T, data []byte) {
		var setID [16]byte
		setID[0] = 1
		var file []byte
		for len(data) >= 2 {
			kind, n := data[0], int(data[1])
			data = data[2:]
			n = min(n, len(data))
			body := data[:n]
			data = data[n:]
			sid := setID
			if kind&0x80 != 0 {
				sid[0] = 2
			}
			file = append(file, fuzzPacket(sid, types[kind%4], append([]byte(nil), body...))...)
		}
		srcs := []Source{{Name: "a.par2", Data: file}}
		mustEnd(t, func() {
			ids := SourceSetIDs(context.Background(), srcs)
			if len(ids) != len(srcs) {
				t.Fatalf("SourceSetIDs returned %d IDs for %d sources", len(ids), len(srcs))
			}
			if idx, err := ParseIndex(context.Background(), srcs); err == nil {
				exerciseIndex(t, idx, srcs)
			}
			if idx, err := ParseIndexSet(context.Background(), srcs, ids[0]); err == nil {
				exerciseIndex(t, idx, srcs)
			}
		})
	})
}
