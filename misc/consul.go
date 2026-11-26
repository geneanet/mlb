package misc

import "time"

// ConsulWait returns the blocking query wait hint to send to Consul for a client-side timeout.
// Consul adds up to wait/16 of jitter, so 7/8 of the timeout keeps the answer well before the deadline.
func ConsulWait(timeout time.Duration) time.Duration {
	return timeout - timeout/8
}
