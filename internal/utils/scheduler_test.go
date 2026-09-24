package utils

import "testing"

func TestConvertToClockOrCronJobDef(t *testing.T) {
	for _, s := range []string{"12:00", " 04:30 ", "0 3 * * *", "30 2 * * 1-5", "@daily"} {
		if _, err := ConvertToClockOrCronJobDef(s); err != nil {
			t.Errorf("%q: unexpected error %v", s, err)
		}
	}
	// Intervals restart their countdown on every re-registration.
	for _, s := range []string{"", "6h", "30m", "@every 6h", "25:00", "12:60", "noon", "0 3 * *"} {
		if _, err := ConvertToClockOrCronJobDef(s); err == nil {
			t.Errorf("%q: accepted, want an error", s)
		}
	}
	// ConvertToJobDef keeps accepting intervals for the repair sweep.
	if _, err := ConvertToJobDef("6h"); err != nil {
		t.Errorf("ConvertToJobDef(6h): %v", err)
	}
}
