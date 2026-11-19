package manager

import (
	"errors"
	"fmt"
)

// par2RecoveryShortfall is runRepair's "not enough recovery slices" failure.
// It is terminal - the recovery data is gone - unless PAR2 article fetches
// failed this pass without a 430 (timeouts, resets): each can drop a whole
// recovery volume, which is never retried within the pass, so the shortfall
// then says nothing about the data and the pass backs off instead. The same
// holds when the index the count came from skipped packets whose MD5 failed
// (skippedPackets): a provider served a wrong or broken body for a PAR2
// article that no other provider replaced this pass, and a later pass may
// be served the right one.
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
