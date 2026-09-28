package manager

import (
	"errors"
	"fmt"
)

// par2RecoveryShortfall is runRepair's "not enough recovery slices" failure.
// It is terminal - the recovery data is gone - unless PAR2 article fetches
// failed this pass without a 430 (timeouts, resets): each can drop a whole
// recovery volume, which is never retried within the pass, so the shortfall
// then says nothing about the data and the pass backs off instead.
func par2RecoveryShortfall(damaged, got, transportFails int) error {
	return par2FetchShortfall(fmt.Sprintf("%d damaged slices but only %d recovery slices fetched/available", damaged, got), transportFails)
}

// par2FetchShortfall is msg as a failure that is terminal only when every
// PAR2 article fetch this pass either succeeded or was a 430. After a timeout
// or a reset the missing PAR2 data may well still be on the servers, so the
// failure is tagged transient and the pass backs off and retries instead of
// giving up on the release.
func par2FetchShortfall(msg string, transportFails int) error {
	if transportFails > 0 {
		return fmt.Errorf("%s (transient: %d PAR2 article fetch(es) failed without a 430)", msg, transportFails)
	}
	return errors.New(msg)
}
