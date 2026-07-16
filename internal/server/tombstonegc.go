package server

import (
	"sync"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

type TombstoneGC struct {
	store    store.Store
	grace    time.Duration
	interval time.Duration
	stop     chan struct{} // closed by Stop to tell run to exit
	done     chan struct{} // closed by run once it has exited
	stopOnce sync.Once     // guards close(stop) so Stop is idempotent
}

func NewTombstoneGC(store store.Store, grace, interval time.Duration) *TombstoneGC {
	t := &TombstoneGC{
		store:    store,
		grace:    grace,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go t.run()

	return t
}

func (t *TombstoneGC) sweep(now time.Time) int {
	var expired []string
	t.store.Range(func(key string, blob []byte) {
		versions, err := version.Decode(blob)
		if err != nil {
			return
		}
		if reapable(versions, now, t.grace) {
			expired = append(expired, key)
		}
	})

	reaped := 0
	for _, key := range expired {
		if t.store.DeleteIf(key, func(old []byte) bool {
			versions, err := version.Decode(old)
			if err != nil {
				return false
			}
			return reapable(versions, now, t.grace)
		}) {
			reaped++
		}
	}

	return reaped
}

func reapable(versions []version.Version, now time.Time, grace time.Duration) bool {
	if len(versions) == 0 {
		return false
	}

	if len(version.Live(versions)) > 0 {
		return false
	}

	for _, v := range versions {
		if now.Sub(time.Unix(v.DeletedAt, 0)) <= grace {
			return false
		}
	}

	return true
}

func (t *TombstoneGC) run() {
	defer close(t.done)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.sweep(time.Now())
		case <-t.stop:
			return
		}
	}
}

func (t *TombstoneGC) Stop() {
	t.stopOnce.Do(func() { close(t.stop) })
	<-t.done
}
