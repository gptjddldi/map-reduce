package mapreducerpc

import (
	"log"
	"map-reduce/mapreduce-rpc/rpc"
	"sync"
	"testing"
	"time"
)

// TestWorkerTimerTimeout tests that worker timers properly timeout when no heartbeat is received
func TestWorkerTimerTimeout(t *testing.T) {
	// Create a master with a short timeout for testing
	master := &Master{
		NumMapWorkers:    2,
		WorkerStates:     make([]rpc.WorkerState, 3),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond, // Very short timeout for testing
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 3)
	master.workerTimeouts = make([]time.Time, 3)

	// Initialize worker states
	for i := 1; i <= 2; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	// Start timer for worker 1
	master.startWorkerTimer(1)

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Check if worker 1 is marked as dead
	master.mu.Lock()
	if master.WorkerStates[1] != rpc.Dead {
		t.Errorf("Expected worker 1 to be marked as dead after timeout, got state: %v", master.WorkerStates[1])
	}
	master.mu.Unlock()

	log.Printf("✓ Worker timer timeout test passed")
}

// TestWorkerTimerReset tests that worker timers are properly reset when heartbeat is received
func TestWorkerTimerReset(t *testing.T) {
	// Create a master with a short timeout for testing
	master := &Master{
		NumMapWorkers:    2,
		WorkerStates:     make([]rpc.WorkerState, 3),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond, // Very short timeout for testing
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 3)
	master.workerTimeouts = make([]time.Time, 3)

	// Initialize worker states
	for i := 1; i <= 2; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	// Start timer for worker 1
	master.startWorkerTimer(1)

	// Reset timer after 50ms (before timeout)
	time.Sleep(50 * time.Millisecond)
	master.resetWorkerTimer(1)

	// Wait for original timeout period
	time.Sleep(100 * time.Millisecond)

	// Check if worker 1 is still alive (not dead)
	master.mu.Lock()
	if master.WorkerStates[1] == rpc.Dead {
		t.Errorf("Expected worker 1 to still be alive after timer reset, got state: %v", master.WorkerStates[1])
	}
	master.mu.Unlock()

	log.Printf("✓ Worker timer reset test passed")
}

// TestMultipleWorkerTimers tests that multiple worker timers work independently
func TestMultipleWorkerTimers(t *testing.T) {
	// Create a master with a short timeout for testing
	master := &Master{
		NumMapWorkers:    3,
		WorkerStates:     make([]rpc.WorkerState, 4),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond, // Very short timeout for testing
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 4)
	master.workerTimeouts = make([]time.Time, 4)

	// Initialize worker states
	for i := 1; i <= 3; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	// Start timers for all workers
	for i := 1; i <= 3; i++ {
		master.startWorkerTimer(i)
	}

	// Reset timer for worker 2 only
	time.Sleep(50 * time.Millisecond)
	master.resetWorkerTimer(2)

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Check results
	master.mu.Lock()
	if master.WorkerStates[1] != rpc.Dead {
		t.Errorf("Expected worker 1 to be dead, got state: %v", master.WorkerStates[1])
	}
	if master.WorkerStates[2] == rpc.Dead {
		t.Errorf("Expected worker 2 to still be alive (timer was reset), got state: %v", master.WorkerStates[2])
	}
	if master.WorkerStates[3] != rpc.Dead {
		t.Errorf("Expected worker 3 to be dead, got state: %v", master.WorkerStates[3])
	}
	master.mu.Unlock()

	log.Printf("✓ Multiple worker timers test passed")
}

// TestDeadWorkerTimerHandling tests that timers don't start for already dead workers
func TestDeadWorkerTimerHandling(t *testing.T) {
	// Create a master
	master := &Master{
		NumMapWorkers:    2,
		WorkerStates:     make([]rpc.WorkerState, 3),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond,
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 3)
	master.workerTimeouts = make([]time.Time, 3)

	// Initialize worker states
	for i := 1; i <= 2; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	// Mark worker 1 as dead
	master.WorkerStates[1] = rpc.Dead

	// Try to reset timer for dead worker
	master.resetWorkerTimer(1)

	// Start timer for dead worker
	master.startWorkerTimer(1)

	// Wait for timeout period
	time.Sleep(150 * time.Millisecond)

	// Check that dead worker remains dead and no new timer was started
	master.mu.Lock()
	if master.WorkerStates[1] != rpc.Dead {
		t.Errorf("Expected dead worker 1 to remain dead, got state: %v", master.WorkerStates[1])
	}
	master.mu.Unlock()

	log.Printf("✓ Dead worker timer handling test passed")
}

// TestTimerCleanup tests that timers are properly cleaned up
func TestTimerCleanup(t *testing.T) {
	// Create a master
	master := &Master{
		NumMapWorkers:    2,
		WorkerStates:     make([]rpc.WorkerState, 3),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond,
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 3)
	master.workerTimeouts = make([]time.Time, 3)

	// Initialize worker states
	for i := 1; i <= 2; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	// Start timers
	for i := 1; i <= 2; i++ {
		master.startWorkerTimer(i)
	}

	// Verify timers are running
	for i := 1; i <= 2; i++ {
		if master.workerTimers[i] == nil {
			t.Errorf("Expected timer for worker %d to be started", i)
		}
	}

	// Cleanup timers
	for i := 0; i < len(master.workerTimers); i++ {
		if master.workerTimers[i] != nil {
			master.workerTimers[i].Stop()
		}
	}

	log.Printf("✓ Timer cleanup test passed")
}

// BenchmarkWorkerTimerPerformance benchmarks the timer operations
func BenchmarkWorkerTimerPerformance(b *testing.B) {
	master := &Master{
		NumMapWorkers:    10,
		WorkerStates:     make([]rpc.WorkerState, 11),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 100 * time.Millisecond,
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, 11)
	master.workerTimeouts = make([]time.Time, 11)

	// Initialize worker states
	for i := 1; i <= 10; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Start timers
		for j := 1; j <= 10; j++ {
			master.startWorkerTimer(j)
		}
		// Reset timers
		for j := 1; j <= 10; j++ {
			master.resetWorkerTimer(j)
		}
	}
}
