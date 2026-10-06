package core

import (
	"testing"
	"time"
)

func TestLossDetectTimeout(t *testing.T) {
	for _, c := range []struct{ ttl, want time.Duration }{
		{3 * time.Second, 2500 * time.Millisecond}, // interval 1s + 1.5s floor
		{6 * time.Second, 3500 * time.Millisecond}, // interval 2s + 1.5s floor
		{10 * time.Second, 5 * time.Second},        // 3.33s + 1.67s
		{30 * time.Second, 15 * time.Second},       // 10s + 5s
		{60 * time.Second, 30 * time.Second},       // 20s + 10s
	} {
		if got := LossDetectTimeout(c.ttl); got.Round(time.Millisecond) != c.want {
			t.Errorf("LossDetectTimeout(%v) = %v, want %v", c.ttl, got, c.want)
		}
	}
}

func TestFitKillAfter(t *testing.T) {
	for _, c := range []struct {
		name     string
		ttl, ka  time.Duration
		explicit bool
		want     time.Duration
		warn     bool
	}{
		{"default fits at ttl 30s", 30 * time.Second, 5 * time.Second, false, 5 * time.Second, false},
		{"default lowered at ttl 10s", 10 * time.Second, 5 * time.Second, false, 2500 * time.Millisecond, false},
		{"default lowered at ttl 6s", 6 * time.Second, 5 * time.Second, false, 1250 * time.Millisecond, false},
		{"explicit kept and warned at ttl 10s", 10 * time.Second, 5 * time.Second, true, 5 * time.Second, true},
		{"explicit that fits is quiet", 10 * time.Second, 2 * time.Second, true, 2 * time.Second, false},
		{"ttl too small: default kept, warned", 5 * time.Second, 5 * time.Second, false, 5 * time.Second, true},
		{"ttl 3s: default kept, warned", 3 * time.Second, 5 * time.Second, false, 5 * time.Second, true},
	} {
		got, warning := FitKillAfter(c.ttl, c.ka, c.explicit)
		if got.Round(time.Millisecond) != c.want || (warning != "") != c.warn {
			t.Errorf("%s: FitKillAfter(%v, %v, %v) = %v, %q; want %v, warn=%v", c.name, c.ttl, c.ka, c.explicit, got, warning, c.want, c.warn)
		}
		if warning == "" && LossDetectTimeout(c.ttl)+got+time.Second >= c.ttl {
			t.Errorf("%s: returned %v without a warning but it breaks the rule", c.name, got)
		}
	}
}
