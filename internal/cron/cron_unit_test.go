package cron

import (
	"testing"
	"time"

	cron "github.com/robfig/cron/v3"
)

func TestCronNextTick(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	// Test standard minute schedule
	sched, err := parser.Parse("*/15 * * * *")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}

	// 2026-06-12T10:00:00Z
	start := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	next := sched.Next(start)
	expected := time.Date(2026, 6, 12, 10, 15, 0, 0, time.UTC)

	if !next.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, next)
	}

	// Test @hourly descriptor
	schedHourly, err := parser.Parse("@hourly")
	if err != nil {
		t.Fatalf("failed to parse @hourly: %v", err)
	}
	nextHourly := schedHourly.Next(start)
	expectedHourly := time.Date(2026, 6, 12, 11, 0, 0, 0, time.UTC)

	if !nextHourly.Equal(expectedHourly) {
		t.Errorf("expected %v, got %v", expectedHourly, nextHourly)
	}
}
