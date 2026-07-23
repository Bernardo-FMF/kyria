package version

import (
	"bytes"

	"github.com/Bernardo-FMF/kyria/internal/binenc"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
)

// Version is one value of a key together with the vector clock stamping its causal
// history. A key may hold several concurrent Versions, called siblings, when writes race,
// which is how vector clocks avoid silently dropping a conflicting update.
//
// Deleted marks a tombstone: a version whose value is "gone". It reconciles by clock like any
// other (a delete mints a superseding clock, so it beats the value it replaces), and the read
// path treats a winning tombstone as a miss.
//
// DeletedAt is the wall-clock unix time (seconds) at which a tombstone was minted, stamped once by
// the coordinator and propagated so every replica agrees on the deadline. It is zero and
// meaningless unless Deleted; the tombstone GC reaps a key once every tombstone in its set is older
// than the grace period.
type Version struct {
	Value     []byte
	Clock     vclock.Clock
	Deleted   bool
	DeletedAt int64
}

// Reconcile folds incoming into the existing sibling set and returns the new set of
// current versions: it drops any existing version that incoming supersedes, and adds
// incoming unless an existing version already supersedes (or equals) it - in which
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

// Frontier returns the causal frontier of a sibling set - the merge of every version's clock.
// A coordinator increments the writer's counter on this to mint a clock that descends all
// current siblings, so the new write supersedes and collapses them.
func Frontier(versions []Version) vclock.Clock {
	var merged vclock.Clock
	for _, v := range versions {
		merged = v.Clock.Merge(merged)
	}

	return merged
}

// Tombstone returns a "gone" version stamped with clock and a nil value. The coordinator uses
// it to write a delete as a version with a superseding clock, and buries the value it replaces.
func Tombstone(clock vclock.Clock, deletedAt int64) Version {
	return Version{
		Clock:     clock,
		Deleted:   true,
		DeletedAt: deletedAt,
	}
}

// Live returns the non-tombstone versions (Deleted == false). The read path calls it on the
// reconciled set: if there are no live versions then the key is absent (a miss), even if tombstones remain.
// A value concurrent with a delete stays Live, so a live write survives a concurrent tombstone.
func Live(versions []Version) []Version {
	l := make([]Version, 0, len(versions))

	for _, v := range versions {
		if !v.Deleted {
			l = append(l, v)
		}
	}

	return l
}

// Equal reports whether two sibling sets hold the same versions, regardless of order.
// Read-repair uses it to skip replicas that already have the reconciled result.
func Equal(a, b []Version) bool {
	if len(a) != len(b) {
		return false
	}
	for _, va := range a {
		found := false
		for _, vb := range b {
			// A value and a tombstone with the same clock are different versions,
			// so Deleted is part of identity here.
			if bytes.Equal(va.Value, vb.Value) && va.Clock.Compare(vb.Clock) == vclock.Equal && va.Deleted == vb.Deleted {
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

// Encode serializes a sibling set into the bytes the store holds under a key.
func Encode(versions []Version) []byte {
	buf := new(bytes.Buffer)
	binenc.PutUint32(buf, uint32(len(versions)))
	for _, v := range versions {
		binenc.PutUint32(buf, uint32(len(v.Value)))
		buf.Write(v.Value)
		binenc.PutBool(buf, v.Deleted)
		if v.Deleted {
			binenc.PutUint64(buf, uint64(v.DeletedAt))
		}

		binenc.PutUint32(buf, uint32(len(v.Clock)))
		for n, counter := range v.Clock {
			_ = binenc.PutString(buf, n)
			binenc.PutUint64(buf, counter)
		}
	}
	return buf.Bytes()
}

// Decode parses bytes produced by Encode back into a sibling set.
func Decode(b []byte) ([]Version, error) {
	if len(b) == 0 {
		return nil, nil
	}

	count, cursor, err := binenc.Uint32(b, 0)
	if err != nil {
		return nil, err
	}

	results := make([]Version, 0, count)
	var valueLen, clockN uint32
	var value []byte
	var deleted bool
	for range count {
		if valueLen, cursor, err = binenc.Uint32(b, cursor); err != nil {
			return nil, err
		}
		if value, cursor, err = binenc.Bytes(b, cursor, int(valueLen)); err != nil {
			return nil, err
		}
		if deleted, cursor, err = binenc.Bool(b, cursor); err != nil {
			return nil, err
		}

		var deletedAt uint64
		if deleted {
			if deletedAt, cursor, err = binenc.Uint64(b, cursor); err != nil {
				return nil, err
			}
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

		results = append(results, Version{Value: value, Clock: clock, Deleted: deleted, DeletedAt: int64(deletedAt)})
	}
	return results, nil
}
