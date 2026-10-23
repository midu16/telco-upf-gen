package usage

import (
	"testing"
	"time"
)

func TestLedgerTouchAndClose(t *testing.T) {
	l := NewLedger("upf-0", 0, 50*time.Millisecond, 5*time.Millisecond)
	now := time.Now()
	_ = now

	l.Touch(5, DirUL, 4, 100, "10.1.1.1", "10.99.0.5")
	l.Touch(5, DirUL, 4, 100, "10.1.1.1", "10.99.0.5")
	l.Touch(5, DirDL, 5, 200, "10.99.0.5", "10.1.1.1")
	l.Touch(7, DirUL, 4, 50, "10.1.1.2", "10.99.0.7")

	if got := l.Active(); got != 2 {
		t.Fatalf("active = %d, want 2", got)
	}

	// not idle yet
	l.CloseIdle(time.Now())
	if l.HasPending() {
		t.Fatalf("session closed too early")
	}

	time.Sleep(80 * time.Millisecond)
	l.CloseIdle(time.Now())
	cdrs := l.DrainCDRs()
	if len(cdrs) != 2 {
		t.Fatalf("cdrs = %d, want 2", len(cdrs))
	}
	var c5, c7 *CDR
	for i := range cdrs {
		switch cdrs[i].SessionID {
		case 5:
			c5 = &cdrs[i]
		case 7:
			c7 = &cdrs[i]
		}
	}
	if c5 == nil || c7 == nil {
		t.Fatalf("missing sessions: %+v", cdrs)
	}
	if c5.UpPkts != 2 || c5.UpBytes != 200 || c5.DownPkts != 1 || c5.DownBytes != 200 {
		t.Fatalf("c5 counters: %+v", c5)
	}
	if c5.UeAddr != "10.1.1.1" || c5.DnAddr != "10.99.0.5" {
		t.Fatalf("c5 endpoints: %s -> %s", c5.UeAddr, c5.DnAddr)
	}
	if c5.AnchorPod != "upf-0" || c5.NodeID != 0 || c5.Status != "closed" {
		t.Fatalf("c5 meta: %+v", c5)
	}
	if got := c5.Apps[4]; got[0] != 2 || got[1] != 200 {
		t.Fatalf("c5 app4: %v", got)
	}
	if got := c5.Apps[5]; got[0] != 1 || got[1] != 200 {
		t.Fatalf("c5 app5: %v", got)
	}
	if c7.UpPkts != 1 {
		t.Fatalf("c7: %+v", c7)
	}
	// closed sessions drop out of the active table
	if got := l.Active(); got != 0 {
		t.Fatalf("active after close = %d", got)
	}
	// drain is one-shot
	if len(l.DrainCDRs()) != 0 {
		t.Fatalf("drain must be one-shot")
	}
}

func TestLedgerMinLifetime(t *testing.T) {
	l := NewLedger("upf-0", 0, 10*time.Millisecond, 5*time.Second)
	l.Touch(1, DirUL, 0, 10, "10.1.1.1", "10.99.0.1")
	time.Sleep(30 * time.Millisecond)
	l.CloseIdle(time.Now())
	if l.HasPending() {
		t.Fatalf("session shorter than min lifetime must not close")
	}
}

func TestLedgerPendingBound(t *testing.T) {
	l := NewLedger("upf-0", 0, time.Millisecond, time.Microsecond)
	for i := uint32(0); i < maxPending+100; i++ {
		l.Touch(i, DirUL, 0, 10, "10.1.1.1", "10.99.0.1")
	}
	l.CloseIdle(time.Now())
	if n := l.PendingCount(); n > maxPending {
		t.Fatalf("pending %d exceeds bound %d", n, maxPending)
	}
}
