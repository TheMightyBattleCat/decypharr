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

// A schedule that is only a time zone panicked inside the cron library
// (it slices up to the first space after "TZ="). Both entry points must
// refuse it with an error instead. A time-zone prefix before a real
// expression still works.
func TestScheduleWithOnlyATimeZoneIsRefusedNotAPanic(t *testing.T) {
	for _, s := range []string{"TZ=UTC", "CRON_TZ=Europe/London", " TZ=UTC "} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%q panicked: %v", s, r)
				}
			}()
			if _, err := ConvertToClockOrCronJobDef(s); err == nil {
				t.Errorf("ConvertToClockOrCronJobDef(%q) accepted it", s)
			}
			if _, err := ConvertToJobDef(s); err == nil {
				t.Errorf("ConvertToJobDef(%q) accepted it", s)
			}
		}()
	}
	if _, err := ConvertToClockOrCronJobDef("TZ=Europe/London 0 3 * * *"); err != nil {
		t.Errorf("a time zone before a cron expression: %v", err)
	}
}

// Schedules the scheduler can never run are refused when they are saved,
// rather than accepted and then silently never registered.
func TestScheduleThatNeverRunsIsRefused(t *testing.T) {
	for _, s := range []string{
		"0 0 30 2 *",       // the 30th of February
		"TZ=UTC @every 1m", // an interval behind a time zone prefix
		"CRON_TZ=UTC @every 6h",
	} {
		if _, err := ConvertToClockOrCronJobDef(s); err == nil {
			t.Errorf("%q: accepted, want an error", s)
		}
	}
}
