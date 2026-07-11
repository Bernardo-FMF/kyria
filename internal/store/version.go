package store

import "github.com/Bernardo-FMF/kyria/internal/vclock"

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
			result = append(result, v) // v already supersedes/matches incoming — keep it, incoming adds nothing
			covered = true
		case vclock.Concurrent:
			result = append(result, v) // a genuine sibling — keep it alongside incoming
		case vclock.Before:
			// incoming supersedes v → drop it (append nothing)
		}
	}

	if !covered {
		result = append(result, incoming)
	}

	return result
}
