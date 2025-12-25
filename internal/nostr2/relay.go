package nostr2

import (
	"context"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type Relay struct {
	Connection *nostr.Relay
	LastAccess time.Time
	Timeout time.Duration
	CancelFunc context.CancelFunc
}

func (r Relay) IsValid() bool {
	if r.Connection == nil {
		return false
	}

	if !r.Connection.IsConnected() {
		return false
	}

	if r.Connection.Subscriptions.Size() != 0 {
		// If there are active subscriptions we shouldn't drop the connection
		return true
	}

	return time.Since(r.LastAccess) <= r.Timeout
}
