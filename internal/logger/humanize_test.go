package logger

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		512:            "512 B",
		4_000:          "4.0 KB",
		812_000_000:    "812 MB",
		2_100_000_000:  "2.1 GB",
		19089929491:    "19.1 GB",
		29_171_624_635: "29.2 GB",
	}
	for n, want := range cases {
		if got := FormatBytes(n); got != want {
			t.Fatalf("FormatBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestFormatRate(t *testing.T) {
	if got := FormatRate(16.34); got != "16.3 MiB/s" {
		t.Fatalf("got %q", got)
	}
	if got := FormatRate(112.6); got != "113 MiB/s" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatDuration(t *testing.T) {
	cases := map[time.Duration]string{
		850 * time.Millisecond:         "850ms",
		9200 * time.Millisecond:        "9.2s",
		3*time.Minute + 12*time.Second: "3m12s",
		time.Hour + 4*time.Minute:      "1h04m",
	}
	for d, want := range cases {
		if got := FormatDuration(d); got != want {
			t.Fatalf("FormatDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestDisplayName(t *testing.T) {
	cases := map[string]string{
		"The.Marlowes.S01E03.Winter.Frost.Beginnings.1080p.AMZN.WEB-DL.DDP2.0.H.264-WADU.mkv": "The Marlowes S01E03 Winter Frost Beginnings",
		"The.Orb.2018.1080p.BluRay.REMUX.AVC.Atmos-EPSiLON":                                   "The Orb 2018",
		"The Pelican Letters 2024 iNTERNAL BluRay 1080p REMUX AVC DTS-HD MA 5.1-Aisha.mkv":    "The Pelican Letters 2024",
		"/mnt/media/The.Dawn.of.Us.S01E01.1080p.AMZN.WEB-DL.DDPa5.1.H.264-NTb.mkv":            "The Dawn of Us S01E01",
		"Madeleine's.Web.2006.1080p.BluRay.x264":                                              "Madeleine's Web 2006",
		"Northern.Pursuit.1990.1080p.BluRay.x264":                                             "Northern Pursuit 1990",
		"a95536a3bd8fdfb5a0d0f3ed3da8948a.44":                                                 "a95536a3bd8fdfb5a0d0f3ed3da8948a 44",
		"":                                                                                    "",
	}
	for in, want := range cases {
		if got := DisplayName(in); got != want {
			t.Fatalf("DisplayName(%q) = %q, want %q", in, got, want)
		}
	}
}
