package clock

import (
	"testing"
	"time"
)

func TestFake(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	if !f.Now().Equal(start) {
		t.Fatal("NewFake should start at the given time")
	}
	f.Advance(time.Minute)
	if !f.Now().Equal(start.Add(time.Minute)) {
		t.Fatal("Advance did not move the clock")
	}
	f.Set(start)
	if !f.Now().Equal(start) {
		t.Fatal("Set did not move the clock")
	}
	if Real.Now(Real{}).IsZero() {
		t.Fatal("real clock returned zero time")
	}
}
