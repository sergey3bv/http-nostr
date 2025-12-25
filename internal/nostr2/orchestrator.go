package nostr2

import (
	"context"
	"net/http"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
)

// Object to orchestrate connections to relays.
type RelayOrchestrator struct {
	relays *xsync.MapOf[string, Relay]
	root   context.Context
	ticker *time.Ticker

	Conf RelayConfig
}

// Configuration that is used for each relay.
type RelayConfig struct {
	// Headers to be set for each outgoing request.
	Headers http.Header

	// Connection to Relay is closed after a period of inactivity. Connections
	// with active subscriptions are immune to it.
	//
	// Defaults to 10 minutes.
	MaximumIdle time.Duration
}

// Constructs new orchestrator and starts goroutine to clean stale connections
func NewRelayOrchestrator(conf RelayConfig) (relay RelayOrchestrator) {
	if conf.MaximumIdle == 0 {
		conf.MaximumIdle = 600 * time.Second
	}

	relay = RelayOrchestrator{
		Conf:   conf,
		relays: xsync.NewMapOf[string, Relay](),
	}

	go relay.cleanupRoutine()

	return
}

func (r *RelayOrchestrator) Publish(
	ctx context.Context,
	url string,
	event Event,
) (err error) {
	relay, err := r.getOrCreateRelay(url)
	if err != nil {
		return err
	}

	err = relay.Connection.Publish(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (r *RelayOrchestrator) Subscribe(url string, filters []Filter) (*Subscription, error) {
	relay, err := r.getOrCreateRelay(url)
	if err != nil {
		return nil, err
	}
	sub, err := relay.Connection.Subscribe(r.root, filters)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (r *RelayOrchestrator) cleanupRoutine() {
	for {
		select {
		case <-r.root.Done():
			return
		case <-r.ticker.C:
			r.removeInvalidRelays()
		}
	}
}

func (r *RelayOrchestrator) removeInvalidRelays() {
	r.relays.Range(func(url string, relay Relay) bool {
		if relay.IsValid() {
			return true
		}
		relay.CancelFunc()
		r.relays.Delete(url)

		return true
	})
}

func (r *RelayOrchestrator) getOrCreateRelay(url string) (result Relay, err error) {
	result, _ = r.relays.LoadOrCompute(url, func() Relay {
		relayContext, cancel := context.WithCancel(r.root)
		relay := nostr.NewRelay(r.root, url)
		relay.RequestHeader = r.Conf.Headers

		err = relay.Connect(relayContext)
		if err != nil {
			cancel()

			return Relay{}
		}

		return Relay{
			Connection: relay,
			LastAccess: time.Now(),
			Timeout:    r.Conf.MaximumIdle,
			CancelFunc: cancel,
		}
	})

	return
}
