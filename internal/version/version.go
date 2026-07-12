// Package version layers vector-clock versioning on top of the opaque byte store.
// A key's stored value is an ENCODED set of Versions (siblings); this package owns
// the sibling type, the reconcile rule that folds a write into a set, and the codec
// that turns a set into the bytes the store holds. The store stays version-agnostic:
// it never imports this package or vclock — the replication layer sits in between.
package version

import (
	"bytes"

	"github.com/Bernardo-FMF/kyria/internal/binenc"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
)

// Version is one value of a key together with the vector clock stamping its causal
// history. A key may hold several concurrent Versions — siblings — when writes race,
// which is how vector clocks avoid silently dropping a conflicting update.
type Version struct {
	Value []byte
	Clock vclock.Clock
}

// Reconcile folds incoming into the existing sibling set and returns the new set of
// current versions: it drops any existing version that incoming supersedes, and adds
// incoming unless an existing version already supersedes (or equals) it — in which
// case incoming is stale and dropped. Versions concurrent with incoming are kept as
// siblings. This is what a replica does on every write.
func Reconcile(existing []Version, incoming Version) []Version {
	result := make([]Version, 0)
	covered := false

	for _, v := range existing {
		switch v.Clock.Compare(incoming.Clock) {
		case vclock.After, vclock.Equal:
			result = append(result, v) // keep the existing entry since it superseeds, incoming adds nothing
			covered = true
		case vclock.Concurrent:
			result = append(result, v) // keep this sibling alongside incoming
		case vclock.Before:
		}
	}

	if !covered {
		result = append(result, incoming)
	}

	return result
}

// Frontier returns the causal frontier of a sibling set — the merge (pointwise max)
// of every version's clock. A coordinator increments the writer's counter on this to
// mint a clock that descends all current siblings, so the new write supersedes and
// collapses them. An empty set has an empty (nil) frontier.
func Frontier(versions []Version) vclock.Clock {
	var merged vclock.Clock
	for _, v := range versions {
		merged = v.Clock.Merge(merged)
	}

	return merged
}

// Equal reports whether two sibling sets hold the same versions, regardless of order.
// Read-repair uses it to skip replicas that already have the reconciled result. Sibling
// sets are tiny (usually a single version), so a nested scan beats a map's allocation;
// and because a reconciled set has no duplicates, equal length plus a ⊆ b implies the
// sets are equal.
func Equal(a, b []Version) bool {
	if len(a) != len(b) {
		return false
	}
	for _, va := range a {
		found := false
		for _, vb := range b {
			if bytes.Equal(va.Value, vb.Value) && va.Clock.Compare(vb.Clock) == vclock.Equal {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Encode serializes a sibling set into the opaque bytes the store holds under a key.
// Encoding can't fail (node IDs never exceed a uint16, values fit a uint32 length),
// so there is no error return.
func Encode(versions []Version) []byte {
	buf := new(bytes.Buffer)
	binenc.PutUint32(buf, uint32(len(versions)))
	for _, v := range versions {
		binenc.PutUint32(buf, uint32(len(v.Value)))
		buf.Write(v.Value)

		binenc.PutUint32(buf, uint32(len(v.Clock)))
		for n, counter := range v.Clock {
			_ = binenc.PutString(buf, n)
			binenc.PutUint64(buf, counter)
		}
	}
	return buf.Bytes()
}

// Decode parses bytes produced by Encode back into a sibling set. Empty input decodes
// to an empty set (a key with no value yet). The bytes come from our own store, but
// decode defensively — binenc's readers bounds-check every read and return
// ErrMalformed rather than panicking on a truncated or corrupt blob.
func Decode(b []byte) ([]Version, error) {
	if len(b) == 0 {
		return nil, nil
	}

	count, cursor, err := binenc.Uint32(b, 0)
	if err != nil {
		return nil, err
	}

	results := make([]Version, 0, count)
	for range count {
		var valueLen, clockN uint32
		var value []byte
		if valueLen, cursor, err = binenc.Uint32(b, cursor); err != nil {
			return nil, err
		}
		if value, cursor, err = binenc.Bytes(b, cursor, int(valueLen)); err != nil {
			return nil, err
		}
		if clockN, cursor, err = binenc.Uint32(b, cursor); err != nil {
			return nil, err
		}
		clock := make(vclock.Clock, clockN)
		for range clockN {
			var node string
			var counter uint64
			if node, cursor, err = binenc.String(b, cursor); err != nil {
				return nil, err
			}
			if counter, cursor, err = binenc.Uint64(b, cursor); err != nil {
				return nil, err
			}
			clock[node] = counter
		}
		results = append(results, Version{Value: value, Clock: clock})
	}
	return results, nil
}
