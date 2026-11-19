package manager

import (
	"errors"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2RecoveryShortfall is runRepair's "not enough recovery slices" failure.
// It is terminal - the recovery data is gone - unless PAR2 article fetches
// failed this pass without a 430 (timeouts, resets): each can drop a whole
// recovery volume, which is never retried within the pass, so the shortfall
// then says nothing about the data and the pass backs off instead. The same
// holds when the index the count came from skipped packets whose MD5 failed
// (skippedPackets): a provider served a wrong or broken body for a PAR2
// article that no other provider replaced this pass, and a later pass may
// be served the right one. A packet broken only by a 430'd article of its
// volume is not counted there (see par2UnexplainedSkips): that data is gone.
func par2RecoveryShortfall(damaged, got, transportFails, skippedPackets int) error {
	msg := fmt.Sprintf("%d damaged slices but only %d recovery slices fetched/available", damaged, got)
	if transportFails == 0 && skippedPackets > 0 {
		return par2Failf(par2Transient, "%s (transient: %d PAR2 packet(s) failed their checksum, a mis-served article?)", msg, skippedPackets)
	}
	return par2FetchShortfall(msg, transportFails)
}

// par2FetchShortfall is msg as a failure that is terminal only when every
// PAR2 article fetch this pass either succeeded or was a 430. After a timeout
// or a reset the missing PAR2 data may well still be on the servers, so the
// failure is tagged transient and the pass backs off and retries instead of
// giving up on the release.
func par2FetchShortfall(msg string, transportFails int) error {
	if transportFails > 0 {
		return par2Failf(par2Transient, "%s (transient: %d PAR2 article fetch(es) failed without a 430)", msg, transportFails)
	}
	return par2Fail(par2Terminal, errors.New(msg))
}

// par2SourceGaps returns the offsets in src at which fetchPar2FileConcurrent
// left out an article of ref: the bytes before such an offset end one
// article and the bytes from it start a later one. pv is the pass's PAR2
// provenance: in a source fetchPar2FileConcurrent returned, an article pv
// has no record of was a 430, since any other failure fails the whole
// file. ok is false when the recorded lengths do not add up to the source's
// length (a source pv did not see fetched), so its gaps are not known.
func par2SourceGaps(src par2.Source, ref storage.Par2FileRef, pv *par2Provenance) ([]int, bool) {
	var gaps []int
	off := 0
	for _, seg := range ref.Segments {
		a, ok := pv.get(seg.MessageID)
		if !ok {
			if len(gaps) == 0 || gaps[len(gaps)-1] != off {
				gaps = append(gaps, off)
			}
			continue
		}
		off += a.n
	}
	return gaps, off == len(src.Data)
}

// par2UnexplainedSkips counts the packets idx skipped for a bad MD5 that a
// 430'd article does not explain. A recovery volume fetched around a 430
// joins the article before the gap to the one after it, so the packet that
// spans the gap fails its MD5: that packet's data is gone, as a whole 430'd
// volume's is, and a later pass gets the same bytes. Only a packet whose
// declared range runs across a gap is explained; any other skip (a wrong
// body, a broken copy, a source whose gaps are not known) is counted, and
// keeps the shortfall transient. sources is the slice idx was parsed from.
func par2UnexplainedSkips(idx *par2.Index, sources []par2.Source, files map[string]storage.Par2FileRef, pv *par2Provenance) int {
	if idx == nil {
		return 0
	}
	type sourceGaps struct {
		gaps []int
		ok   bool
	}
	known := make(map[int]sourceGaps)
	n := 0
	for _, sp := range idx.SkippedPackets {
		if sp.Source < 0 || sp.Source >= len(sources) || pv == nil {
			n++
			continue
		}
		src := sources[sp.Source]
		sg, seen := known[sp.Source]
		if !seen {
			if ref, ok := files[src.Name]; ok {
				sg.gaps, sg.ok = par2SourceGaps(src, ref, pv)
			}
			known[sp.Source] = sg
		}
		explained := false
		if sg.ok {
			start, end := par2SkippedPacketRange(src.Data, sp.Offset)
			for _, g := range sg.gaps {
				if start < g && g < end {
					explained = true
					break
				}
			}
		}
		if !explained {
			n++
		}
	}
	return n
}
