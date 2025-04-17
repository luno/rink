package rink

import (
	"sort"
	"time"
)

type memberChanges struct {
	Remained []string
	Added    []string
	Removed  []string
	Replaced map[string]string
	Waiting  []string
}

func getMemberChanges(
	members map[string]time.Time,
	ranks ranks,
	now time.Time,
	newMemberWait time.Duration,
) memberChanges {
	unranked := Difference(members, ranks)
	missing := Difference(ranks, members)

	changes := memberChanges{
		Remained: Intersect(ranks, members),
	}
	// Sort waiting members by earliest first
	sort.Slice(unranked, func(i, j int) bool {
		return members[unranked[i]].Before(members[unranked[j]])
	})

	// Find how many members will be taking the place of
	// missing members
	replaced := len(unranked)
	if len(missing) < replaced {
		replaced = len(missing)
	}
	if replaced > 0 {
		changes.Replaced = make(map[string]string)
	}
	for i := 0; i < replaced; i++ {
		changes.Replaced[missing[i]] = unranked[i]
	}

	if len(changes.Remained) == 0 && len(changes.Replaced) == 0 {
		// Add all nodes if we've got an empty cluster
		changes.Added = unranked
	} else {
		// See if we'll be adding some new nodes
		for i := replaced; i < len(unranked); i++ {
			m := unranked[i]
			if members[m].Add(newMemberWait).After(now) {
				changes.Waiting = unranked[i:]
				break
			}
			changes.Added = append(changes.Added, m)
		}
	}
	for i := replaced; i < len(missing); i++ {
		changes.Removed = append(changes.Removed, missing[i])
	}
	return changes
}

func getNewRanks(last ranks, changes memberChanges) ranks {
	numRanks := len(changes.Remained) + len(changes.Added) + len(changes.Replaced)
	if numRanks == 0 {
		return nil
	}
	// Make a slice where index == rank
	rankSlots := make([]string, numRanks)
	var orphaned []string

	for _, m := range changes.Remained {
		oldRank := last[m]
		if oldRank < int32(len(rankSlots)) {
			rankSlots[oldRank] = m
		} else {
			orphaned = append(orphaned, m)
		}
	}
	for gone, added := range changes.Replaced {
		oldRank := last[gone]
		if oldRank < int32(len(rankSlots)) {
			rankSlots[oldRank] = added
		} else {
			orphaned = append(orphaned, added)
		}
	}
	unassigned := append(changes.Added, orphaned...)
	next := make(ranks)
	for rank, member := range rankSlots {
		if member == "" {
			member = unassigned[0]
			unassigned = unassigned[1:]
		}
		next[member] = int32(rank)
	}
	return next
}
