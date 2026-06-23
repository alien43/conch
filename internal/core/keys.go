package core

import "fmt"

const PrefixV1 = "/conch/v1"

// ElectElectionKey is passed to concurrency.NewElection (without trailing slash)
func ElectElectionKey(office string) string {
	return fmt.Sprintf("%s/elect/%s", PrefixV1, office)
}

// ElectPrefix is used to watch or fetch candidates for an office
func ElectPrefix(office string) string {
	return fmt.Sprintf("%s/elect/%s/", PrefixV1, office)
}

// SemaPrefix is the prefix for a semaphore name and capacity
func SemaPrefix(name string, max int) string {
	return fmt.Sprintf("%s/sema/%s/%d/", PrefixV1, name, max)
}

// SemaKey is the leased key for a specific holder
func SemaKey(name string, max int, spread bool, host string, leaseID int64) string {
	if spread {
		return fmt.Sprintf("%s/sema/%s/%d/%s", PrefixV1, name, max, host)
	}
	return fmt.Sprintf("%s/sema/%s/%d/%x", PrefixV1, name, max, leaseID)
}

// CronJobPrefix is the prefix for all cron jobs
func CronJobPrefix() string {
	return fmt.Sprintf("%s/cron/job/", PrefixV1)
}

// CronJobKey is the spec key for a cron job
func CronJobKey(name string) string {
	return fmt.Sprintf("%s/cron/job/%s", PrefixV1, name)
}

// CronFirePrefix is the prefix for all tick fire keys of a job
func CronFirePrefix(name string) string {
	return fmt.Sprintf("%s/cron/fire/%s/", PrefixV1, name)
}

// CronFireKey is the fire key for a job's specific tick
func CronFireKey(name string, tickUnix int64) string {
	return fmt.Sprintf("%s/cron/fire/%s/%d", PrefixV1, name, tickUnix)
}

// CronResultPrefix is the prefix for all results of a job
func CronResultPrefix(name string) string {
	return fmt.Sprintf("%s/cron/result/%s/", PrefixV1, name)
}

// CronResultKey is the result key for a job's specific tick
func CronResultKey(name string, tickUnix int64) string {
	return fmt.Sprintf("%s/cron/result/%s/%d", PrefixV1, name, tickUnix)
}
