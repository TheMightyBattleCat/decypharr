package manager

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// yencArticle is one article as a provider serves it: decoded bytes plus the
// yEnc headers they came with.
type yencArticle struct {
	data []byte
	meta *nntp.YencMetadata
}

// yencFetcher serves articles with their yEnc headers and applies the caller's
// identity check the way usenet.FetchArticleChecked does: a rejected copy is
// a not-found from that provider.
type yencFetcher struct {
	articles map[string]yencArticle
	calls    map[string]int
}

func (f *yencFetcher) fetch(_ context.Context, messageID string, check usenet.ArticleCheck) ([]byte, error) {
	f.calls[messageID]++
	a, ok := f.articles[messageID]
	if !ok {
		return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "no such article"}
	}
	if check != nil {
		if reason := check(a.meta); reason != "" {
			return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "article belongs to a different upload: " + reason}
		}
	}
	return a.data, nil
}

// postedContent is a file's bytes, distinct at every offset so a misplaced
// boundary shows.
func postedContent(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*7 + i/251)
	}
	return out
}

// uniformPosting cuts content into articles of seed bytes (the last one
// shorter) and returns them keyed by message ID, with estimated refs whose
// sizes are off by a few bytes each - the shape of a real posting whose
// refs were never measured.
func uniformPosting(content []byte, seed int, estimates []int64) (map[string]yencArticle, storage.PostedFileRef) {
	articles := map[string]yencArticle{}
	var ref storage.PostedFileRef
	n := (len(content) + seed - 1) / seed
	for i := 0; i < n; i++ {
		start := i * seed
		end := min(start+seed, len(content))
		id := fmt.Sprintf("<p%d>", i+1)
		articles[id] = yencArticle{
			data: content[start:end],
			meta: &nntp.YencMetadata{
				Part: int64(i + 1), Total: int64(n), Size: int64(len(content)),
				Offset: int64(start), PartSize: int64(end - start),
				Begin: int64(start + 1), End: int64(end),
			},
		}
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: estimates[i]})
		ref.Size += estimates[i]
	}
	ref.Name = "a.r00"
	return articles, ref
}

// Five 60-byte articles (the last 40) recorded with estimates of 59-62 bytes.
func estimatedFixture() ([]byte, map[string]yencArticle, storage.PostedFileRef) {
	content := postedContent(280)
	articles, ref := uniformPosting(content, 60, []int64{61, 59, 62, 60, 41})
	return content, articles, ref
}

// The bug this fixes: scaled estimates put every boundary after the first in
// the wrong place, so a read across one returns another offset's bytes.
func TestEstimatedGeometryMisreadsWithoutMeasurement(t *testing.T) {
	content, articles, ref := estimatedFixture()
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())

	got, err := pf.ReadRange(0, int64(len(content)))
	if err == nil && bytes.Equal(got, content) {
		t.Fatal("estimated geometry read the file correctly; the fixture no longer shows the drift")
	}
}

func TestResolveGeometryMeasuresUniformArticles(t *testing.T) {
	content, articles, ref := estimatedFixture()
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())

	pf.resolveGeometry()
	if !pf.exact {
		t.Fatal("geometry not measured")
	}
	wantBases := []int64{0, 60, 120, 180, 240}
	wantSizes := []int64{60, 60, 60, 60, 40}
	for i := range wantBases {
		if pf.base[i] != wantBases[i] || pf.segSizes[i] != wantSizes[i] {
			t.Fatalf("segment %d: base %d size %d, want %d/%d", i, pf.base[i], pf.segSizes[i], wantBases[i], wantSizes[i])
		}
	}
	got, err := pf.ReadRange(0, int64(len(content)))
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("measured geometry read the wrong bytes")
	}
	if f.calls["<p1>"] != 1 {
		t.Fatalf("first article fetched %d times, want once (resolveGeometry leaves it cached)", f.calls["<p1>"])
	}
}

// Headers that do not fit the file length leave the estimates in place.
func TestResolveGeometryKeepsEstimatesWhenMeasurementDoesNotFit(t *testing.T) {
	content, articles, ref := estimatedFixture()
	for _, a := range articles {
		a.meta.Size = 999 // =ybegin size disagrees with the PAR2 length
	}
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	before := append([]int64(nil), pf.base...)

	pf.resolveGeometry()
	if pf.exact {
		t.Fatal("geometry marked exact from a header that disagrees with the file length")
	}
	for i := range before {
		if pf.base[i] != before[i] {
			t.Fatalf("base[%d] changed to %d, want the estimate %d", i, pf.base[i], before[i])
		}
	}
}

// The first article is the dead one: the next measures the same size from
// its own =ypart offset.
func TestResolveGeometryMeasuresFromALaterArticleWhenTheFirstIsDead(t *testing.T) {
	content, articles, ref := estimatedFixture()
	delete(articles, "<p1>")
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())

	pf.resolveGeometry()
	if !pf.exact {
		t.Fatal("geometry not measured from the second article")
	}
	got, err := pf.ReadRange(60, int64(len(content))-60)
	if err != nil {
		t.Fatalf("ReadRange after the dead first article: %v", err)
	}
	if !bytes.Equal(got, content[60:]) {
		t.Fatal("measured geometry read the wrong bytes")
	}
}

// An .sfv recorded at 3300 bytes that really holds 2900: MD5-16k hashes the
// whole real file, not the estimate zero-padded or a short read.
func TestComputeMD5_16kUsesYencFileSize(t *testing.T) {
	real := postedContent(2900)
	articles := map[string]yencArticle{"<sfv>": {
		data: real,
		meta: &nntp.YencMetadata{Part: 1, Total: 1, Size: 2900, Offset: 0, PartSize: 2900, Begin: 1, End: 2900},
	}}
	ref := storage.PostedFileRef{Name: "x.sfv", Size: 3300, Segments: []storage.Par2SegmentRef{{MessageID: "<sfv>", Bytes: 3300}}}
	f := &yencFetcher{articles: articles, calls: map[string]int{}}

	got, err := computeMD5_16k(context.Background(), f.fetch, ref)
	if err != nil {
		t.Fatalf("computeMD5_16k: %v", err)
	}
	if want := md5.Sum(real); got != want {
		t.Fatalf("MD5-16k = %x, want %x (the whole 2900-byte file)", got, want)
	}
}

// A different upload's article under a reused Message-ID is refused, so the
// fetch fails over instead of feeding its bytes to the repair.
func TestPostedFetchRejectsDifferentUpload(t *testing.T) {
	content, articles, ref := estimatedFixture()
	articles["<p3>"] = yencArticle{
		data: postedContent(58),
		meta: &nntp.YencMetadata{Part: 413, Total: 630, Size: 215599104, Offset: 141387776, PartSize: 58},
	}
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()

	_, err := pf.ReadRange(120, 60)
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("ReadRange over the foreign article: %v, want a not-found", err)
	}
}

func TestPostedArticleMismatch(t *testing.T) {
	right := &nntp.YencMetadata{Part: 3, Total: 5, Size: 280, Offset: 120, PartSize: 60}
	cases := []struct {
		name   string
		meta   *nntp.YencMetadata
		exact  bool
		reject bool
	}{
		{"no headers", nil, true, false},
		{"right article", right, true, false},
		{"right article, estimated geometry", right, false, false},
		{"other part number only", &nntp.YencMetadata{Part: 9, Total: 5, Size: 280, Offset: 120, PartSize: 60}, true, false},
		{"other file size only", &nntp.YencMetadata{Part: 3, Total: 5, Size: 281, Offset: 120, PartSize: 60}, true, false},
		{"other upload", &nntp.YencMetadata{Part: 413, Total: 630, Size: 215599104, Offset: 141387776, PartSize: 58}, true, true},
		{"other upload, estimated geometry", &nntp.YencMetadata{Part: 413, Total: 630, Size: 215599104, Offset: 141387776, PartSize: 58}, false, true},
		{"wrong offset and part", &nntp.YencMetadata{Part: 4, Total: 5, Size: 280, Offset: 180, PartSize: 60}, true, true},
	}
	for _, tc := range cases {
		got := postedArticleMismatch(tc.meta, 2, 120, tc.exact, 280)
		if (got != "") != tc.reject {
			t.Errorf("%s: mismatch = %q, want reject=%v", tc.name, got, tc.reject)
		}
	}
}

// A recorded-dead segment whose article is back is read fresh and proven by
// the IFSC of every slice it touches; a copy that fails is not accepted.
func TestReadVerifiedRangeHealsOnlyAProvenArticle(t *testing.T) {
	content, articles, ref := estimatedFixture()
	const sliceSize = 40 // PAR2 slice sizes are multiples of 4
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x07}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.r00", slices)

	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()
	rng := postedRange{fileID: fileID, start: pf.base[1], end: pf.base[1] + pf.segSizes[1]}

	got, ok := readVerifiedRange(idx, pf, rng, rng.start, rng.end)
	if !ok {
		t.Fatal("intact article not verified")
	}
	if !bytes.Equal(got, content[60:120]) {
		t.Fatal("verified range holds the wrong bytes")
	}

	bad := articles["<p2>"]
	bad.data = append([]byte(nil), bad.data...)
	bad.data[5] ^= 0xFF
	articles["<p2>"] = bad
	pf = newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()
	if _, ok := readVerifiedRange(idx, pf, rng, rng.start, rng.end); ok {
		t.Fatal("a corrupted copy passed as verified")
	}
}

func TestPar2SuspectFailuresTurnTerminalAfterRepeats(t *testing.T) {
	intactAbort := fmt.Errorf("repair: 9 intact slice(s) decoded shorter than their recorded size: %w",
		fmt.Errorf("%w: 3 intact slices failed their own IFSC checksum", par2.ErrChecksumMismatch))
	shortOnly := fmt.Errorf("repair: 4 intact slice(s) decoded shorter than their recorded size: %w", fmt.Errorf("par2: read intact slice 7: boom"))
	for attempts := 1; attempts < par2SuspectAttemptLimit; attempts++ {
		if c := par2OutcomeClass(shortOnly, attempts); c.terminal || !c.suspect {
			t.Fatalf("short read at attempt %d: %+v, want suspect and retried", attempts, c)
		}
	}
	if c := par2OutcomeClass(shortOnly, par2SuspectAttemptLimit); !c.terminal {
		t.Fatalf("short read at attempt %d: not terminal", par2SuspectAttemptLimit)
	}
	// A bare checksum sentinel wrapping text is not Repair's intact abort:
	// without the typed marker it stays terminal.
	if c := classifyPar2Failure(intactAbort); !c.terminal {
		t.Fatalf("untyped checksum mismatch: %+v, want terminal", c)
	}
	confirmed := fmt.Errorf("repair: 3 intact slice(s) unreadable - 2 confirmed missing across every provider, 1 decoded shorter than their recorded size: %w", fmt.Errorf("x"))
	if c := classifyPar2Failure(confirmed); !c.terminal {
		t.Fatalf("confirmed-missing failure: %+v, want terminal", c)
	}
}

// Repair's own intact-slice abort is typed, so it is recognised through any
// wrapping and backs off instead of going terminal on the first attempt.
func TestPar2IntactChecksumAbortIsSuspect(t *testing.T) {
	err := intactAbortFromRepair(t)
	wrapped := fmt.Errorf("repair: 9 intact slice(s) decoded shorter than their recorded size: %w", err)
	c := classifyPar2Failure(wrapped)
	if c.terminal || !c.suspect {
		t.Fatalf("intact checksum abort: %+v, want suspect", c)
	}
	if c := par2OutcomeClass(wrapped, par2SuspectAttemptLimit); !c.terminal {
		t.Fatalf("intact checksum abort at the limit: %+v, want terminal", c)
	}
}

// intactAbortFromRepair runs par2.Repair over intact slices that all fail
// their IFSC and returns the abort it produces.
func intactAbortFromRepair(t *testing.T) error {
	t.Helper()
	const sliceSize = 100
	fileID := [16]byte{0x09}
	var slices [][]byte
	for i := 0; i < 5; i++ {
		slices = append(slices, repeatByte(byte(i+1), sliceSize))
	}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.rar", slices)
	_, err := par2.Repair(idx, []int64{0}, []par2.RecoverySlice{{Exponent: 0, Data: make([]byte, sliceSize)}}, wrongSlices{sliceSize})
	if err == nil {
		t.Fatal("Repair accepted slices that fail their checksums")
	}
	if !par2.IsIntactChecksumAbort(err) {
		t.Fatalf("Repair error %v is not the intact-slice abort", err)
	}
	return err
}

// wrongSlices serves every intact slice as zeros, failing its IFSC.
type wrongSlices struct{ size int }

func (w wrongSlices) ReadSlice(int64) ([]byte, error) { return make([]byte, w.size), nil }

// healFetchableDeadSegments end to end against a real overlay store: a dead
// segment whose article is back is patched with its verified bytes; one the
// STAT sweep saw missing, and one whose fetch fails, are left for parity.
func TestHealFetchableDeadSegmentsPatchesOnlyProvenArticles(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}

	content, articles, ref := estimatedFixture()
	const sliceSize = 40
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x07}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.r00", slices)

	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()
	fetchers := map[[16]byte]*postedFileFetcher{fileID: pf}

	const nzbID = "nzb-heal"
	store := m.usenet
	// The reader reads each article whole here (SegmentDataStart 0).
	readerFile := storage.NZBFile{Name: ref.Name}
	for i, seg := range ref.Segments {
		readerFile.Segments = append(readerFile.Segments, storage.NZBSegment{
			Number: i + 1, MessageID: seg.MessageID, Bytes: pf.segSizes[i],
		})
	}
	nzb := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{readerFile}}
	var refs []par2DeadRef
	for _, i := range []int{1, 2, 3} {
		seg := ref.Segments[i]
		if err := store.RecordOverlayDead(nzbID, ref.Name, i, seg.MessageID, pf.segSizes[i]); err != nil {
			t.Fatalf("record dead %d: %v", i, err)
		}
		refs = append(refs, par2DeadRef{
			file: ref.Name,
			seg:  overlay.DeadSegment{Index: i, MessageID: seg.MessageID, Bytes: pf.segSizes[i]},
			rng:  postedRange{fileID: fileID, start: pf.base[i], end: pf.base[i] + pf.segSizes[i]},
		})
	}
	delete(articles, "<p4>") // segment 3: still gone
	statMissing := map[string]struct{}{"<p3>": {}}

	remaining, err := p.healFetchableDeadSegments(context.Background(), nzbID, "Some.Release", nzb, idx, fetchers, refs, statMissing)
	if err != nil {
		t.Fatalf("heal: %v", err)
	}
	if len(remaining) != 2 || remaining[0].seg.Index != 2 || remaining[1].seg.Index != 3 {
		t.Fatalf("remaining = %+v, want segments 2 and 3", remaining)
	}
	patch, ok := store.OverlayPatchBytes(nzbID, ref.Name, 1)
	if !ok || !bytes.Equal(patch, content[60:120]) {
		t.Fatalf("segment 1 patch = %d bytes (ok=%v), want its real 60 bytes", len(patch), ok)
	}
	if _, ok := store.OverlayPatchBytes(nzbID, ref.Name, 3); ok {
		t.Fatal("segment 3 patched although its article is gone")
	}
}

// A recovery volume with one dead article still yields the recovery slices
// the gap does not touch. It used to be dropped whole, and Under Reef
// S11E06 (2026-09-22) was declared to have 0 recovery slices.
func TestFetchWholePar2FileSkipsADeadArticle(t *testing.T) {
	vol, err := os.ReadFile("../usenet/par2/testdata/par2/fixture.vol1+2.par2")
	if err != nil {
		t.Fatal(err)
	}
	index, err := os.ReadFile("../usenet/par2/testdata/par2/fixture.par2")
	if err != nil {
		t.Fatal(err)
	}
	full, err := par2.ParseIndex([]par2.Source{{Name: "i", Data: index}, {Name: "v", Data: vol}})
	if err != nil || len(full.Recovery) != 2 {
		t.Fatalf("fixture: %d recovery slices, err %v; want 2", len(full.Recovery), err)
	}

	// Cut the volume into 1000-byte "articles" and kill one.
	const article = 1000
	ref := storage.Par2FileRef{Name: "fixture.vol1+2.par2", Size: int64(len(vol))}
	bodies := map[string][]byte{}
	for off, i := 0, 0; off < len(vol); off, i = off+article, i+1 {
		id := fmt.Sprintf("<v%d>", i)
		bodies[id] = vol[off:min(off+article, len(vol))]
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: int64(len(bodies[id]))})
	}
	dead := fmt.Sprintf("<v%d>", (len(vol)/4)/article) // inside the first recovery packet: the second is found only by resyncing
	fetch := func(_ context.Context, id string) ([]byte, error) {
		if id == dead {
			return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "gone"}
		}
		return bodies[id], nil
	}

	data, err := fetchWholePar2File(context.Background(), fetch, ref)
	if err != nil {
		t.Fatalf("fetchWholePar2File with one dead article: %v", err)
	}
	idx, err := par2.ParseIndex([]par2.Source{{Name: "i", Data: index}, {Name: "v", Data: data}})
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(idx.Recovery) != 1 {
		t.Fatalf("%d recovery slices parsed, want the 1 the dead article does not touch", len(idx.Recovery))
	}

	// Every article gone: that is not-found, as before.
	allDead := func(context.Context, string) ([]byte, error) {
		return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "gone"}
	}
	if _, err := fetchWholePar2File(context.Background(), allDead, ref); !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("all articles dead: %v, want not-found", err)
	}
}

// Concurrent article fetches keep article order; a transport error still
// fails the file (it says nothing about the article); a 430 is left out.
func TestFetchPar2FileConcurrentKeepsOrder(t *testing.T) {
	var ref storage.Par2FileRef
	var want []byte
	for i := 0; i < 40; i++ {
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: fmt.Sprintf("<a%d>", i)})
		want = append(want, byte(i), byte(i), byte(i))
	}
	fetch := func(_ context.Context, id string) ([]byte, error) {
		var i int
		fmt.Sscanf(id, "<a%d>", &i)
		time.Sleep(time.Duration((i*7)%5) * time.Millisecond)
		return []byte{byte(i), byte(i), byte(i)}, nil
	}
	got, err := fetchPar2FileConcurrent(context.Background(), fetch, ref, 6)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("concurrent fetch: err=%v, bytes in order=%v", err, bytes.Equal(got, want))
	}

	transport := func(_ context.Context, id string) ([]byte, error) {
		if id == "<a17>" {
			return nil, &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "slow"}
		}
		return fetch(context.Background(), id)
	}
	if _, err := fetchPar2FileConcurrent(context.Background(), transport, ref, 6); err == nil || !nntp.IsTimeoutError(err) {
		t.Fatalf("transport error: %v, want the timeout", err)
	}
}

func TestPar2JobTimeoutScalesWithReleaseSize(t *testing.T) {
	if got := par2JobTimeoutFor(0); got != par2JobTimeout {
		t.Fatalf("unknown size: %s, want the base %s", got, par2JobTimeout)
	}
	// Tale of Castles S08E05: 6.29 GB took 24m53s for two passes.
	if got := par2JobTimeoutFor(6_292_222_528); got < 60*time.Minute {
		t.Fatalf("6.3 GB release: %s, want room for three passes", got)
	}
	if got := par2JobTimeoutFor(200 << 30); got != par2JobTimeoutMax {
		t.Fatalf("200 GB release: %s, want the %s cap", got, par2JobTimeoutMax)
	}
}

// A RAR volume's first article carries the volume header, so the reader's
// slot starts inside the article: the patch must hold that window, not the
// whole article. WritePatch used to refuse the whole-article patch, and a
// repaired first article of a volume could never be stored.
func TestHealPatchesTheReaderSlotInsideAnArticle(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}

	content, articles, ref := estimatedFixture()
	const sliceSize = 40
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x07}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.r00", slices)
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()

	// The member starts 20 bytes into article 0 and ends 5 bytes before the
	// end of article 1 - the shape of a volume header plus a member boundary.
	const header, tailCut = 20, 5
	readerFile := storage.NZBFile{Name: "member.mkv"}
	for i := range ref.Segments {
		seg := storage.NZBSegment{Number: i + 1, MessageID: ref.Segments[i].MessageID, Bytes: pf.segSizes[i]}
		switch i {
		case 0:
			seg.SegmentDataStart, seg.Bytes = header, pf.segSizes[0]-header
		case 1:
			seg.Bytes = pf.segSizes[1] - tailCut
		}
		readerFile.Segments = append(readerFile.Segments, seg)
	}
	const nzbID = "nzb-slot"
	nzb := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{readerFile}}
	var refs []par2DeadRef
	for _, i := range []int{0, 1} {
		seg := readerFile.Segments[i]
		if err := m.usenet.RecordOverlayDead(nzbID, readerFile.Name, i, seg.MessageID, seg.Bytes); err != nil {
			t.Fatalf("record dead %d: %v", i, err)
		}
		refs = append(refs, par2DeadRef{
			file: readerFile.Name,
			seg:  overlay.DeadSegment{Index: i, MessageID: seg.MessageID, Bytes: seg.Bytes},
			rng:  postedRange{fileID: fileID, start: pf.base[i], end: pf.base[i] + pf.segSizes[i]},
		})
	}

	remaining, err := p.healFetchableDeadSegments(context.Background(), nzbID, "Some.Release", nzb, idx,
		map[[16]byte]*postedFileFetcher{fileID: pf}, refs, map[string]struct{}{})
	if err != nil || len(remaining) != 0 {
		t.Fatalf("heal: err=%v remaining=%+v, want both patched", err, remaining)
	}
	head, ok := m.usenet.OverlayPatchBytes(nzbID, readerFile.Name, 0)
	if !ok || !bytes.Equal(head, content[header:pf.segSizes[0]]) {
		t.Fatalf("segment 0 patch = %d bytes (ok=%v), want the %d bytes after the volume header", len(head), ok, pf.segSizes[0]-header)
	}
	tail, ok := m.usenet.OverlayPatchBytes(nzbID, readerFile.Name, 1)
	if !ok || !bytes.Equal(tail, content[pf.base[1]:pf.base[1]+pf.segSizes[1]-tailCut]) {
		t.Fatalf("segment 1 patch = %d bytes (ok=%v), want the member's bytes only", len(tail), ok)
	}
}
