package model

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"redisClusterManager/cluster/utils"

	"github.com/go-redis/redis/v8"
)

// concurrency of slot migration workers.
const migrationWorkers = 16

// Slot migration thresholds.
const (
	smallKeyBatch   = 50                       // keys: single MIGRATE is faster than chunking
	chunkDivisor    = 10                       // split into this many chunks for large key counts
	bigKeyThreshold = 10 * 1024 * 1024         // 10MB: treat as "big key"
)

type slotTask struct {
	slot         int
	fromID       string
	fromIP       string
	toID         string
	toIP         string
	destHostPort uint16
	fromHostPort uint16
	containerPort uint16
}

type keyEntry struct {
	key  string
	size int64
}

// group represents a set of slot migration tasks sharing the same source node.
type group struct {
	tasks        []slotTask
	fromIP       string
	fromHostPort uint16
}

// MigrateSlotsToEmptyNode migrates slots from existing masters to empty master nodes.
func (c *ClusterManager) MigrateSlotsToEmptyNode(ctx context.Context, clusterName string) error {
	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}
	if len(c.MasterIDs) == 0 {
		return fmt.Errorf("no master nodes for slot migration")
	}

	newV := TotalSlots / len(c.MasterIDs)

	// Phase 1: collect all slot migration tasks.
	var tasks []slotTask
	toSlots := make(map[string]int) // toNodeID -> target slot count

	empIdx := 0
	for _, masterID := range c.MasterIDs {
		masterNode, ok := c.IDToClusterNode[masterID]
		if !ok || masterNode.SlotsNum == 0 || masterNode.ClusterName != clusterName || masterNode.SlotsNum <= newV {
			continue
		}
		for _, slot := range masterNode.Slots {
			for i := slot.End; i >= slot.Start && empIdx < len(c.EmptyMasters); i-- {
				toID := c.EmptyMasters[empIdx].ID
				if masterID == toID {
					break
				}
				destRuntime := c.nodeManager.GetNodeByHost(c.EmptyMasters[empIdx].IP)
				fromRuntime := c.nodeManager.GetNodeByHost(masterNode.IP)
				var destPort, fromPort, conPort uint16
				if destRuntime != nil {
					destPort = destRuntime.HostPort
					conPort = destRuntime.ConPort
				}
				if fromRuntime != nil {
					fromPort = fromRuntime.HostPort
				}
				tasks = append(tasks, slotTask{
					slot:          i,
					fromID:        masterID,
					fromIP:        masterNode.IP,
					toID:          toID,
					toIP:          c.EmptyMasters[empIdx].IP,
					destHostPort:  destPort,
					fromHostPort:  fromPort,
					containerPort: conPort,
				})
				toSlots[toID]++
				c.EmptyMasters[empIdx].SlotsNum++
				masterNode.SlotsNum--
				if c.EmptyMasters[empIdx].SlotsNum == newV {
					empIdx++
				}
				if masterNode.SlotsNum == newV {
					break
				}
			}
		}
	}

	if len(tasks) == 0 {
		return nil
	}

	// Phase 2: group tasks by source IP and migrate concurrently.
	groups := make(map[string]*group)
	for i := range tasks {
		t := &tasks[i]
		key := t.fromIP
		if groups[key] == nil {
			groups[key] = &group{fromIP: t.fromIP, fromHostPort: t.fromHostPort}
		}
		groups[key].tasks = append(groups[key].tasks, *t)
	}

	fmt.Printf("slot migration: %d slots across %d source groups (%d workers each)\n", len(tasks), len(groups), migrationWorkers)

	var completed atomic.Int64
	totalSlots := int64(len(tasks))

	// Progress reporter goroutine.
	ctxProgress, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := completed.Load()
				if done < totalSlots {
					fmt.Printf("  slot migration: %d/%d (%.1f%%)\n", done, totalSlots, float64(done)/float64(totalSlots)*100)
				}
			case <-ctxProgress.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, len(tasks))

	for _, g := range groups {
		// Shared source client per group.
		sourceCli, err := CreateRedisClient(fmt.Sprintf("127.0.0.1:%d", g.fromHostPort))
		if err != nil {
			cancelProgress()
			return fmt.Errorf("connect source %s fail: %w", g.fromIP, err)
		}
		defer sourceCli.Close()

		taskCh := make(chan slotTask, len(g.tasks))
		for _, t := range g.tasks {
			taskCh <- t
		}
		close(taskCh)

		for w := 0; w < migrationWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for t := range taskCh {
					if err := migrateSlotShared(ctx, sourceCli, t); err != nil {
						errCh <- fmt.Errorf("slot %d from %s to %s: %w", t.slot, t.fromIP, t.toIP, err)
					}
					completed.Add(1)
				}
			}()
		}
	}

	wg.Wait()
	cancelProgress()
	close(errCh)

	// Collect errors (non-fatal).
	for err := range errCh {
		log.Printf("slot migration: %v", err)
	}

	fmt.Printf("  slot migration: %d/%d (100%%)\n", totalSlots, totalSlots)

	// Notify cache layer to invalidate affected slots.
	if c.cacheInvalidator != nil && len(tasks) > 0 {
		minSlot, maxSlot := tasks[0].slot, tasks[0].slot
		for i := range tasks {
			if tasks[i].slot < minSlot {
				minSlot = tasks[i].slot
			}
			if tasks[i].slot > maxSlot {
				maxSlot = tasks[i].slot
			}
		}
		c.cacheInvalidator.InvalidateSlots(minSlot, maxSlot)
	}

	return c.PrintClusterNodesInfo(ctx)
}

// migrateSlotShared migrates a single slot using a shared source client.
// Handles: empty slots (instant), small batches (single MIGRATE), large batches (chunked), and big keys (COPY mode).
func migrateSlotShared(ctx context.Context, sourceCli *redis.Client, t slotTask) error {
	destCli, err := CreateRedisClient(fmt.Sprintf("127.0.0.1:%d", t.destHostPort))
	if err != nil {
		return fmt.Errorf("connect dest fail: %w", err)
	}
	defer destCli.Close()

	// IMPORTING on dest
	_, err = utils.ExecuteClusterCommand(ctx, destCli, "cluster", "SETSLOT", strconv.Itoa(t.slot), "IMPORTING", t.fromID)
	if err != nil {
		return fmt.Errorf("IMPORTING slot %d: %w", t.slot, err)
	}
	// MIGRATING on source
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "MIGRATING", t.toID)
	if err != nil {
		return fmt.Errorf("MIGRATING slot %d: %w", t.slot, err)
	}

	// Migrate keys if any.
	keys := sourceCli.ClusterGetKeysInSlot(ctx, t.slot, 1000).Val()
	if len(keys) == 0 {
		// Scenario A: empty slot -- skip data migration.
		goto finalize
	}

	if len(keys) <= smallKeyBatch {
		// Scenario B: few keys -- single MIGRATE is fastest.
		migrateKeyBatch(ctx, sourceCli, t, keys, 5000)
	} else {
		// Scenario C/D: many keys or possible big keys.
		normal, big, _ := classifyKeys(ctx, sourceCli, keys)
		// Big keys: migrate individually with COPY mode.
		for _, entry := range big {
			if err := migrateBigKey(ctx, sourceCli, t, entry.key, entry.size); err != nil {
				log.Printf("big key %s (%.1fMB) migration failed: %v", entry.key, float64(entry.size)/1024/1024, err)
			}
		}
		// Normal keys: chunked migration.
		if len(normal) > 0 {
			chunkSize := len(normal) / chunkDivisor
			if chunkSize < 10 {
				chunkSize = 10
			}
			for i := 0; i < len(normal); i += chunkSize {
				end := i + chunkSize
				if end > len(normal) {
					end = len(normal)
				}
				active := filterExistingKeys(ctx, sourceCli, normal[i:end])
				if len(active) > 0 {
					migrateKeyBatch(ctx, sourceCli, t, active, 5000)
				}
			}
		}
	}

finalize:
	// SETSLOT NODE on dest
	_, err = utils.ExecuteClusterCommand(ctx, destCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "NODE", t.toID)
	if err != nil {
		return fmt.Errorf("SETSLOT NODE dest slot %d: %w", t.slot, err)
	}
	// SETSLOT NODE on source
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "NODE", t.toID)
	if err != nil {
		return fmt.Errorf("SETSLOT NODE source slot %d: %w", t.slot, err)
	}
	return nil
}

// classifyKeys separates keys into normal and big based on MEMORY USAGE.
func classifyKeys(ctx context.Context, cli *redis.Client, keys []string) (normal []string, big []keyEntry, failed []string) {
	for _, key := range keys {
		size, err := cli.MemoryUsage(ctx, key, 0).Result()
		if err != nil {
			failed = append(failed, key)
		} else if size >= bigKeyThreshold {
			big = append(big, keyEntry{key, size})
		} else {
			normal = append(normal, key)
		}
	}
	return
}

// migrateBigKey migrates a single large key using COPY mode for safety.
func migrateBigKey(ctx context.Context, sourceCli *redis.Client, t slotTask, key string, size int64) error {
	timeoutSec := size / 1024 / 1024 // 1 second per MB
	if timeoutSec < 5 {
		timeoutSec = 5
	}
	if timeoutSec > 60 {
		timeoutSec = 60
	}

	fmt.Printf("  migrating big key %s (%.1fMB, timeout=%ds)\n", key, float64(size)/1024/1024, timeoutSec)

	// COPY mode: keep source key until we confirm the destination has it.
	args := []interface{}{t.toIP, t.containerPort, key, 0, timeoutSec * 1000, "COPY", "REPLACE"}
	if err := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, args...)...).Err(); err != nil {
		// COPY keeps the source key -- safe to retry.
		return fmt.Errorf("MIGRATE big key %s: %w", key, err)
	}
	// Key is now on the destination. Safe to delete from source.
	if err := sourceCli.Del(ctx, key).Err(); err != nil {
		log.Printf("big key %s copied but source delete failed: %v", key, err)
	}
	return nil
}

// migrateKeyBatch sends a batch of keys via a single MIGRATE command.
func migrateKeyBatch(ctx context.Context, sourceCli *redis.Client, t slotTask, keys []string, timeoutMs int) {
	port := strconv.Itoa(int(t.containerPort))
	args := []interface{}{t.toIP, port, "", 0, timeoutMs, "KEYS"}
	for _, key := range keys {
		args = append(args, key)
	}
	if err := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, args...)...).Err(); err != nil {
		log.Printf("MIGRATE slot %d (%d keys): %v", t.slot, len(keys), err)
	}
}

// filterExistingKeys returns keys that exist on the source node.
func filterExistingKeys(ctx context.Context, cli *redis.Client, keys []string) []string {
	var result []string
	for _, key := range keys {
		exists, err := cli.Exists(ctx, key).Result()
		if err != nil {
			continue
		}
		if exists > 0 {
			result = append(result, key)
		}
	}
	return result
}
