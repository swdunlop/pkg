package ratelimit

import (
	"net/http"
	"testing"
	"time"
)

func TestAllowKey(t *testing.T) {
	lim, err := New(3, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// First 3 requests should be allowed.
	for i := range 3 {
		if !lim.AllowKey("192.0.2.1") {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}

	// 4th request should be denied.
	if lim.AllowKey("192.0.2.1") {
		t.Fatal("request 4 should be denied")
	}

	// A different key should still be allowed.
	if !lim.AllowKey("192.0.2.2") {
		t.Fatal("different key should be allowed")
	}
}

func TestWindowExpiry(t *testing.T) {
	lim, err := New(1, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if !lim.AllowKey("192.0.2.1") {
		t.Fatal("first request should be allowed")
	}
	if lim.AllowKey("192.0.2.1") {
		t.Fatal("second request should be denied")
	}

	// Manually expire the timestamps by backdating them.
	lim.mu.Lock()
	for i := range lim.stamps {
		if lim.stamps[i] != 0 {
			lim.stamps[i] -= 2 // push 2 seconds into the past
		}
	}
	lim.mu.Unlock()

	if !lim.AllowKey("192.0.2.1") {
		t.Fatal("request after expiry should be allowed")
	}
}

func TestAllow(t *testing.T) {
	lim, err := New(2, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := http.NewRequest("POST", "/login", nil)
	r.RemoteAddr = "203.0.113.1:12345"

	if !lim.Allow(r) {
		t.Fatal("first request should be allowed")
	}
	if !lim.Allow(r) {
		t.Fatal("second request should be allowed")
	}
	if lim.Allow(r) {
		t.Fatal("third request should be denied")
	}
}

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"direct", "203.0.113.1:1234", "", "203.0.113.1"},
		{"direct ignores xff", "203.0.113.1:1234", "198.51.100.1", "203.0.113.1"},
		{"loopback trusts xff", "127.0.0.1:1234", "198.51.100.1", "198.51.100.1"},
		{"loopback xff chain", "127.0.0.1:1234", "198.51.100.1, 10.0.0.1", "198.51.100.1"},
		{"loopback no xff", "127.0.0.1:1234", "", "127.0.0.1"},
		{"ipv6 loopback", "[::1]:1234", "198.51.100.1", "198.51.100.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("GET", "/", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				r.Header.Set("X-Forwarded-For", tt.xff)
			}
			if got := ClientIP(r); got != tt.want {
				t.Errorf("ClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBucketBounds(t *testing.T) {
	// Verify that every possible bucket index produces a valid slice
	// of the stamps array (no out-of-bounds panic).
	lim, err := New(5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for idx := range numBuckets {
		lo := idx * lim.max
		hi := (idx + 1) * lim.max
		if lo < 0 || hi > len(lim.stamps) {
			t.Fatalf("bucket %d: stamps[%d:%d] out of bounds (len=%d)", idx, lo, hi, len(lim.stamps))
		}
	}
}
