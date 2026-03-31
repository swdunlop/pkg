// Package ratelimit provides a fixed-memory IP rate limiter for HTTP handlers.
//
// The limiter uses a fixed array of timestamp buckets indexed by a salted
// FNV-1a hash of the client IP address. This design has several properties:
//
//   - Constant memory usage regardless of traffic volume (no maps, no GC pressure).
//   - O(1) per-request overhead.
//   - Salted hashing prevents attackers from predicting bucket collisions.
//
// The trade-off is that unrelated IPs may share a bucket if they hash to the
// same slot. With 65536 buckets this is rare in practice, and the worst case
// is a slightly lower effective limit for the colliding IPs — never a bypass.
//
// # Usage
//
// Create a [Limiter] with [New], then call [Limiter.Allow] in your HTTP handler
// to decide whether to proceed:
//
//	lim, err := ratelimit.New(3, 15*time.Minute)  // 3 requests per 15 minutes
//	if err != nil { ... }
//
//	func handleLogin(w http.ResponseWriter, r *http.Request) {
//	    if !lim.Allow(r) {
//	        // Return the same response as success to avoid revealing the limit.
//	        renderSent(w)
//	        return
//	    }
//	    // ... proceed with login
//	}
package ratelimit

import (
	"crypto/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const numBuckets = 65536 // 2^16; must be a power of two for masking

// Limiter is a fixed-memory rate limiter that tracks request timestamps per
// client IP. It is safe for concurrent use.
//
// Each client IP is hashed into one of 65536 buckets. Each bucket stores the
// timestamps of the most recent requests (up to the configured maximum). A
// request is allowed only if fewer than max timestamps within the window are
// recorded in the bucket.
//
// All memory is allocated up front by [New]. No further allocations occur
// during operation.
type Limiter struct {
	mu      sync.Mutex
	salt    [16]byte
	window  int64 // window duration in seconds
	max     int   // max requests per window
	stamps  []int64
	// buckets[i] is stamps[i*max : (i+1)*max]
}

// New creates a rate limiter that allows max requests per window per client IP.
// It allocates all memory up front: numBuckets * max int64 timestamps.
// It initializes a random salt from crypto/rand, returning an error if the
// system random source is unavailable.
func New(max int, window time.Duration) (*Limiter, error) {
	l := &Limiter{
		window: int64(window.Seconds()),
		max:    max,
		stamps: make([]int64, numBuckets*max),
	}
	if _, err := rand.Read(l.salt[:]); err != nil {
		return nil, err
	}
	return l, nil
}

// Allow reports whether the HTTP request should be permitted under the rate
// limit. If the request is allowed, its timestamp is recorded. If the limit
// has been reached, no timestamp is recorded and false is returned.
//
// The client IP is determined by [ClientIP]: when the direct connection is
// from a loopback address (indicating a reverse proxy like Caddy or nginx),
// the leftmost X-Forwarded-For entry is trusted. Otherwise RemoteAddr is used.
func (l *Limiter) Allow(r *http.Request) bool {
	return l.AllowKey(ClientIP(r))
}

// AllowKey reports whether a request with the given key (typically an IP
// address) should be permitted. This is useful when the caller has already
// determined the client identity or wants to rate-limit on something other
// than IP.
func (l *Limiter) AllowKey(key string) bool {
	idx := int(fnv1a(l.salt[:], key) & (numBuckets - 1))
	now := time.Now().Unix()

	l.mu.Lock()
	defer l.mu.Unlock()

	bucket := l.stamps[idx*l.max : (idx+1)*l.max]
	cutoff := now - l.window

	// Count recent requests and find the oldest entry.
	recent := 0
	oldest := 0
	for i, ts := range bucket {
		if ts > cutoff {
			recent++
		}
		if bucket[i] < bucket[oldest] {
			oldest = i
		}
	}
	if recent >= l.max {
		return false
	}
	bucket[oldest] = now
	return true
}

// ClientIP extracts the client's IP address from an HTTP request.
//
// When the direct connection originates from a loopback address (127.0.0.1,
// ::1), the request is assumed to be proxied and the leftmost entry in the
// X-Forwarded-For header is returned. This matches the behavior of reverse
// proxies like Caddy, which prepend the real client IP.
//
// When the connection is not from loopback, X-Forwarded-For is ignored and
// the host portion of RemoteAddr is returned directly. This prevents header
// spoofing when the server is directly exposed.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i != -1 {
				xff = xff[:i]
			}
			return strings.TrimSpace(xff)
		}
	}
	return host
}

// fnv1a computes FNV-1a over the salt and string s. The salt prevents
// attackers from crafting inputs that deliberately collide into the same bucket.
func fnv1a(salt []byte, s string) uint32 {
	h := uint32(2166136261) // FNV offset basis
	for _, b := range salt {
		h ^= uint32(b)
		h *= 16777619 // FNV prime
	}
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
