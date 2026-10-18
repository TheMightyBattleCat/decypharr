package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"

	"github.com/sirrobot01/appendstore"
	"google.golang.org/protobuf/proto"
)

// DBFormat is the version of the exported record format and of the stores'
// content layout. A snapshot written by a build with a higher number cannot
// be restored into this one.
const DBFormat = 1

var exportMagic = []byte("DCBKSTORE1\n")

const (
	recordMarker  byte = 1
	trailerMarker byte = 0
	// maxRecordPart bounds a key or value length read from a stream so a
	// corrupt length cannot allocate without limit.
	maxRecordPart = 1 << 30
)

// exportAfterPass is called after each store has been exported. Tests use it
// to change the stores mid-run; it is nil in production.
var exportAfterPass func(store string)

// StoreNames lists the stores in export order.
func StoreNames() []string { return append([]string(nil), storeNames...) }

func (s *Storage) storeByName(name string) *appendstore.Store {
	switch name {
	case "entries":
		return s.entries
	case "queue":
		return s.queue
	case "items":
		return s.entryItems
	case "repair_state":
		return s.repairState
	case "repair_runs":
		return s.repairRuns
	case "par2_repair_attempts":
		return s.par2Repairs
	case "par2_repair_state":
		return s.par2RepairState
	case "regrab_guard":
		return s.regrabGuard
	case "pending_deletes":
		return s.pendingDeletes
	}
	return nil
}

// StoreCounts returns the number of records in each store.
func (s *Storage) StoreCounts() map[string]int {
	out := make(map[string]int, len(storeNames))
	for _, n := range storeNames {
		out[n] = s.storeByName(n).Len()
	}
	return out
}

// ExportStores streams every store as length-prefixed records. open is called
// once per pass of a store with its name and returns the destination; if it
// returns a closer, it is closed when the pass ends. A store written twice
// (see below) is opened twice and the second pass replaces the first.
//
// Each stream is the magic line, then records (marker 1, uvarint key length,
// key, uvarint value length, value) and a trailer (marker 0, uint64 record
// count, SHA-256 of the record bytes). The stores are not read at one
// instant, so the record count of entries is taken before the run and after the items pass;
// if it moved, entries and items are exported again. The returned map is the
// record count of each store's final pass.
func (s *Storage) ExportStores(open func(store string) (io.Writer, error), progress func(store string, records int)) (map[string]int, error) {
	counts := make(map[string]int, len(storeNames))
	pass := func(name string) error {
		n, err := s.exportStore(name, open)
		if err != nil {
			return fmt.Errorf("export %s: %w", name, err)
		}
		counts[name] = n
		if progress != nil {
			progress(name, n)
		}
		return nil
	}
	entriesBefore := s.storeByName("entries").Len()
	for _, name := range storeNames {
		if err := pass(name); err != nil {
			return nil, err
		}
		if exportAfterPass != nil {
			exportAfterPass(name)
		}
	}
	// Compared after the items pass too, so a change while items were being
	// read is caught.
	if s.storeByName("entries").Len() != entriesBefore {
		// Entries changed under the pass, so items may be out of step with
		// them. Both are small: take them again, entries first.
		for _, name := range []string{"entries", "items"} {
			if err := pass(name); err != nil {
				return nil, err
			}
		}
	}
	return counts, nil
}

func (s *Storage) exportStore(name string, open func(string) (io.Writer, error)) (int, error) {
	dst, err := open(name)
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(dst, 256<<10)
	h := sha256.New()
	if _, err := bw.Write(exportMagic); err != nil {
		return 0, err
	}
	rec := io.MultiWriter(bw, h)
	count := 0
	var lenBuf [binary.MaxVarintLen64]byte
	err = s.storeByName(name).ForEach(func(key string, value []byte) error {
		if _, err := rec.Write([]byte{recordMarker}); err != nil {
			return err
		}
		n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
		if _, err := rec.Write(lenBuf[:n]); err != nil {
			return err
		}
		if _, err := io.WriteString(rec, key); err != nil {
			return err
		}
		n = binary.PutUvarint(lenBuf[:], uint64(len(value)))
		if _, err := rec.Write(lenBuf[:n]); err != nil {
			return err
		}
		if _, err := rec.Write(value); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	if err := bw.WriteByte(trailerMarker); err != nil {
		return 0, err
	}
	var tail [8]byte
	binary.BigEndian.PutUint64(tail[:], uint64(count))
	if _, err := bw.Write(tail[:]); err != nil {
		return 0, err
	}
	if _, err := bw.Write(h.Sum(nil)); err != nil {
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	if c, ok := dst.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return 0, err
		}
	}
	return count, nil
}

// readStore reads one exported stream, calling fn for each record, and checks
// the trailer. It returns the record count.
func readStore(r io.Reader, fn func(key string, value []byte) error) (int, error) {
	br := bufio.NewReaderSize(r, 256<<10)
	magic := make([]byte, len(exportMagic))
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != string(exportMagic) {
		return 0, errors.New("not a store export")
	}
	h := sha256.New()
	tr := &teeByteReader{r: br, h: h}
	count := 0
	for {
		marker, err := br.ReadByte()
		if err != nil {
			return count, fmt.Errorf("truncated export: %w", err)
		}
		if marker == trailerMarker {
			break
		}
		if marker != recordMarker {
			return count, errors.New("corrupt export: bad record marker")
		}
		h.Write([]byte{recordMarker})
		key, err := readPart(tr)
		if err != nil {
			return count, err
		}
		val, err := readPart(tr)
		if err != nil {
			return count, err
		}
		if err := fn(string(key), val); err != nil {
			return count, err
		}
		count++
	}
	var tail [8]byte
	if _, err := io.ReadFull(br, tail[:]); err != nil {
		return count, fmt.Errorf("truncated export: %w", err)
	}
	sum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(br, sum); err != nil {
		return count, fmt.Errorf("truncated export: %w", err)
	}
	if want := binary.BigEndian.Uint64(tail[:]); want != uint64(count) {
		return count, fmt.Errorf("corrupt export: trailer says %d records, read %d", want, count)
	}
	if string(sum) != string(h.Sum(nil)) {
		return count, errors.New("corrupt export: checksum mismatch")
	}
	return count, nil
}

// teeByteReader hashes every byte read through it.
type teeByteReader struct {
	r *bufio.Reader
	h hash.Hash
}

func (t *teeByteReader) ReadByte() (byte, error) {
	b, err := t.r.ReadByte()
	if err == nil {
		t.h.Write([]byte{b})
	}
	return b, err
}

func (t *teeByteReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.h.Write(p[:n])
	return n, err
}

func readPart(t *teeByteReader) ([]byte, error) {
	n, err := binary.ReadUvarint(t)
	if err != nil {
		return nil, fmt.Errorf("truncated export: %w", err)
	}
	if n > maxRecordPart {
		return nil, errors.New("corrupt export: record too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(t, buf); err != nil {
		return nil, fmt.Errorf("truncated export: %w", err)
	}
	return buf, nil
}

// ImportReport is what ImportStores found.
type ImportReport struct {
	// Read is the record count of each exported store; Imported is what was
	// written to the new store.
	Read     map[string]int `json:"read"`
	Imported map[string]int `json:"imported"`
	// OrphanItems and OrphanRepairState were dropped because no entry has
	// their folder.
	OrphanItems       int `json:"orphan_items"`
	OrphanRepairState int `json:"orphan_repair_state"`
	// EntriesWithoutItems counts entries whose folder has no item record;
	// they are kept and flagged.
	EntriesWithoutItems int `json:"entries_without_items"`
	// UndecodableEntries were kept raw because they could not be decoded, so
	// their attributes could not be rebuilt.
	Undecodable int `json:"undecodable"`
}

// ImportStores builds fresh stores in dir from exported streams. open returns
// the stream for a store, or (nil, nil) when the snapshot has none. Entries,
// queue and repair state are re-put with their attributes rebuilt from the
// value; keys starting "__" are put raw. Items and repair state whose entry
// is missing are dropped; entries without items are counted.
func ImportStores(dir string, open func(store string) (io.ReadCloser, error)) (*ImportReport, error) {
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		return nil, fmt.Errorf("import target %s is not empty", dir)
	}
	st, err := NewStorage(dir)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	rep := &ImportReport{Read: map[string]int{}, Imported: map[string]int{}}
	folders := map[string]struct{}{}
	itemNames := map[string]struct{}{}

	each := func(name string, fn func(key string, value []byte) error) error {
		rc, err := open(name)
		if err != nil {
			return fmt.Errorf("open %s: %w", name, err)
		}
		if rc == nil {
			return nil
		}
		defer rc.Close()
		n, err := readStore(rc, fn)
		rep.Read[name] = n
		if err != nil {
			return fmt.Errorf("import %s: %w", name, err)
		}
		return nil
	}
	put := func(name, key string, value []byte, opts *appendstore.PutOptions) error {
		if err := st.storeByName(name).Put(key, value, opts); err != nil {
			return err
		}
		rep.Imported[name]++
		return nil
	}

	for _, name := range []string{"entries", "queue"} {
		name := name
		err := each(name, func(key string, value []byte) error {
			if strings.HasPrefix(key, "__") {
				return put(name, key, value, nil)
			}
			var pb EntryProto
			if err := proto.Unmarshal(value, &pb); err != nil {
				rep.Undecodable++
				return put(name, key, value, nil)
			}
			entry := ProtoToEntry(&pb)
			if name == "entries" {
				folders[entry.GetFolder()] = struct{}{}
			}
			return put(name, key, value, entryPutOptions(entry))
		})
		if err != nil {
			return nil, err
		}
	}

	err = each("items", func(key string, value []byte) error {
		if !strings.HasPrefix(key, "__") {
			if _, ok := folders[key]; !ok {
				rep.OrphanItems++
				return nil
			}
			itemNames[key] = struct{}{}
		}
		return put("items", key, value, nil)
	})
	if err != nil {
		return nil, err
	}

	err = each("repair_state", func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return put("repair_state", key, value, nil)
		}
		if _, ok := folders[key]; !ok {
			rep.OrphanRepairState++
			return nil
		}
		var h struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(value, &h); err != nil {
			rep.Undecodable++
			return put("repair_state", key, value, nil)
		}
		return put("repair_state", key, value, &appendstore.PutOptions{Attributes: map[string]string{attributeStatus: h.Status}})
	})
	if err != nil {
		return nil, err
	}

	for _, name := range storeNames {
		switch name {
		case "entries", "queue", "items", "repair_state":
			continue
		}
		name := name
		err := each(name, func(key string, value []byte) error { return put(name, key, value, nil) })
		if err != nil {
			return nil, err
		}
	}

	for f := range folders {
		if _, ok := itemNames[f]; !ok {
			rep.EntriesWithoutItems++
		}
	}
	if err := st.Sync(); err != nil {
		return nil, err
	}
	return rep, nil
}

// Sync flushes every store to disk.
func (s *Storage) Sync() error {
	var errs []error
	for _, n := range storeNames {
		if err := s.storeByName(n).Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
