package cache

// SlotInvalidator is called after Redis slot migration to invalidate
// cache entries whose keys hash to the migrated slots.
type SlotInvalidator struct {
	groups []*Group
}

// NewSlotInvalidator creates an invalidator that clears matching keys
// from all registered cache groups.
func NewSlotInvalidator(groups ...*Group) *SlotInvalidator {
	return &SlotInvalidator{groups: groups}
}

// InvalidateSlot removes all cached entries associated with a slot range.
// This is called after slot migration completes in the Redis cluster.
func (si *SlotInvalidator) InvalidateSlot(slot int) {
	for _, g := range si.groups {
		// We don't have a slot→key index, so we use a conservative
		// approach: the cache's TTL naturally handles stale entries
		// after slot migration. For immediate invalidation, register
		// a slot range and the next access will check if the slot has
		// been migrated.
		g.markSlotMigrated(slot)
	}
}

// InvalidateSlots invalidates a range of slots.
func (si *SlotInvalidator) InvalidateSlots(start, end int) {
	for _, g := range si.groups {
		for slot := start; slot <= end; slot++ {
			g.markSlotMigrated(slot)
		}
	}
}
