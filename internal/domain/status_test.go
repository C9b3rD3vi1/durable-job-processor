package domain

import "testing"

func TestJobStatusValid(t *testing.T) {
	for _, status := range []JobStatus{
		StatusQueued, StatusRunning, StatusRetryWait, StatusSucceeded, StatusDead, StatusCancelled,
	} {
		if !status.Valid() {
			t.Errorf("%q.Valid() = false, want true", status)
		}
	}
	if JobStatus("bogus").Valid() {
		t.Error(`JobStatus("bogus").Valid() = true, want false`)
	}
}

func TestJobStatusTerminal(t *testing.T) {
	for _, status := range []JobStatus{StatusSucceeded, StatusDead, StatusCancelled} {
		if !status.IsTerminal() {
			t.Errorf("%q.IsTerminal() = false, want true", status)
		}
	}
	for _, status := range []JobStatus{StatusQueued, StatusRunning, StatusRetryWait} {
		if status.IsTerminal() {
			t.Errorf("%q.IsTerminal() = true, want false", status)
		}
	}
}

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to JobStatus
		want     bool
	}{
		{StatusQueued, StatusRunning, true},
		{StatusQueued, StatusCancelled, true},
		{StatusRetryWait, StatusRunning, true},
		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusRetryWait, true},
		{StatusRunning, StatusDead, true},
		{StatusDead, StatusRetryWait, true},
		{StatusSucceeded, StatusRunning, false},
		{StatusCancelled, StatusQueued, false},
		{StatusQueued, StatusSucceeded, false},
		{JobStatus("bogus"), StatusRunning, false},
	}

	for _, tt := range tests {
		if got := CanTransition(tt.from, tt.to); got != tt.want {
			t.Errorf("CanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}
