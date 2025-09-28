package mapreducerpc

import (
	"log"
	"map-reduce/mapreduce-rpc/rpc"
	"sync"
	"testing"
	"time"
)

type Harness struct {
	cluster *Cluster
	n       int
	t       *testing.T
}

func NewHarness(n int, t *testing.T) *Harness {
	cluster, err := StartCluster("testdata/input.txt", n, n, func(kv KeyValue) []KeyValue {
		return []KeyValue{kv}
	}, func(key string, values []string) KeyValue {
		return KeyValue{key, values[0]}
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Harness{
		cluster: cluster,
		n:       n,
		t:       t,
	}
}

// TimerTestHarness provides convenient methods for testing worker timers
type TimerTestHarness struct {
	*Harness
	originalTimeout time.Duration
}

// NewTimerTestHarness creates a harness specifically for timer testing
func NewTimerTestHarness(n int, t *testing.T) *TimerTestHarness {
	harness := NewHarness(n, t)

	// Store original timeout and set a short timeout for testing
	originalTimeout := harness.cluster.master.heartbeatTimeout
	harness.cluster.master.heartbeatTimeout = 100 * time.Millisecond

	return &TimerTestHarness{
		Harness:         harness,
		originalTimeout: originalTimeout,
	}
}

// GetMaster returns the master instance for direct access
func (h *TimerTestHarness) GetMaster() *Master {
	return h.cluster.master
}

// GetWorkerState returns the current state of a worker
func (h *TimerTestHarness) GetWorkerState(workerId int) rpc.WorkerState {
	h.cluster.master.mu.Lock()
	defer h.cluster.master.mu.Unlock()
	return h.cluster.master.WorkerStates[workerId]
}

// SimulateHeartbeat simulates a heartbeat response from a worker
func (h *TimerTestHarness) SimulateHeartbeat(workerId int) {
	h.cluster.master.resetWorkerTimer(workerId)
	log.Printf("Simulated heartbeat for worker %d", workerId)
}

// WaitForTimeout waits for the heartbeat timeout period
func (h *TimerTestHarness) WaitForTimeout() {
	time.Sleep(h.cluster.master.heartbeatTimeout + 50*time.Millisecond)
}

// WaitForTimeoutAndCheck waits for timeout and checks if worker is dead
func (h *TimerTestHarness) WaitForTimeoutAndCheck(workerId int) bool {
	h.WaitForTimeout()
	return h.GetWorkerState(workerId) == rpc.Dead
}

// StartHeartbeatSimulation starts a goroutine that sends periodic heartbeats
func (h *TimerTestHarness) StartHeartbeatSimulation(workerId int, interval time.Duration) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if h.GetWorkerState(workerId) == rpc.Dead {
					return
				}
				h.SimulateHeartbeat(workerId)
			}
		}
	}()

	return &wg
}

// AssertWorkerState asserts that a worker is in the expected state
func (h *TimerTestHarness) AssertWorkerState(workerId int, expectedState rpc.WorkerState) {
	actualState := h.GetWorkerState(workerId)
	if actualState != expectedState {
		h.t.Errorf("Expected worker %d to be in state %v, got %v", workerId, expectedState, actualState)
	}
}

// AssertWorkerAlive asserts that a worker is alive (not dead)
func (h *TimerTestHarness) AssertWorkerAlive(workerId int) {
	h.AssertWorkerState(workerId, rpc.Idle)
}

// AssertWorkerDead asserts that a worker is dead
func (h *TimerTestHarness) AssertWorkerDead(workerId int) {
	h.AssertWorkerState(workerId, rpc.Dead)
}

// Cleanup restores original timeout and shuts down the cluster
func (h *TimerTestHarness) Cleanup() {
	h.cluster.master.heartbeatTimeout = h.originalTimeout
	h.cluster.Shutdown()
}
