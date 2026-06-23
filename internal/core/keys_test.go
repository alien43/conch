package core

import (
	"testing"
)

func TestKeys(t *testing.T) {
	if got := ElectElectionKey("office1"); got != "/conch/v1/elect/office1" {
		t.Errorf("unexpected ElectElectionKey: %q", got)
	}

	if got := ElectPrefix("office1"); got != "/conch/v1/elect/office1/" {
		t.Errorf("unexpected ElectPrefix: %q", got)
	}

	if got := SemaPrefix("sema1", 3); got != "/conch/v1/sema/sema1/3/" {
		t.Errorf("unexpected SemaPrefix: %q", got)
	}

	if got := SemaKey("sema1", 3, true, "host1", 123); got != "/conch/v1/sema/sema1/3/host1" {
		t.Errorf("unexpected SemaKey spread: %q", got)
	}

	if got := SemaKey("sema1", 3, false, "host1", 0xabc); got != "/conch/v1/sema/sema1/3/abc" {
		t.Errorf("unexpected SemaKey lease: %q", got)
	}

	if got := CronJobPrefix(); got != "/conch/v1/cron/job/" {
		t.Errorf("unexpected CronJobPrefix: %q", got)
	}

	if got := CronJobKey("job1"); got != "/conch/v1/cron/job/job1" {
		t.Errorf("unexpected CronJobKey: %q", got)
	}

	if got := CronFirePrefix("job1"); got != "/conch/v1/cron/fire/job1/" {
		t.Errorf("unexpected CronFirePrefix: %q", got)
	}

	if got := CronFireKey("job1", 12345); got != "/conch/v1/cron/fire/job1/12345" {
		t.Errorf("unexpected CronFireKey: %q", got)
	}

	if got := CronResultPrefix("job1"); got != "/conch/v1/cron/result/job1/" {
		t.Errorf("unexpected CronResultPrefix: %q", got)
	}

	if got := CronResultKey("job1", 12345); got != "/conch/v1/cron/result/job1/12345" {
		t.Errorf("unexpected CronResultKey: %q", got)
	}
}
