package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTaskQueue_AddTask(t *testing.T) {
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 1000, 100, nil)

	task := &Task{
		ID:            "task-1",
		WorkflowID:    "workflow-1",
		RunID:         "run-1",
		ScheduledTime: time.Now(),
	}

	if err := tq.AddTask(task); err != nil {
		t.Fatalf("AddTask error = %v", err)
	}

	if tq.PendingTaskCount() != 1 {
		t.Errorf("PendingTaskCount = %d, want 1", tq.PendingTaskCount())
	}

	// Adding same task again should return false
	if err := tq.AddTask(task); err != ErrTaskExists {
		t.Errorf("duplicate AddTask error = %v, want %v", err, ErrTaskExists)
	}
}

func TestTaskQueue_PollTask(t *testing.T) {
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 1000, 100, nil)

	task := &Task{
		ID:            "task-1",
		WorkflowID:    "workflow-1",
		RunID:         "run-1",
		ScheduledTime: time.Now(),
	}

	if err := tq.AddTask(task); err != nil {
		t.Fatalf("AddTask error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	polled, err := tq.Poll(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Poll error = %v", err)
	}

	if polled.ID != task.ID {
		t.Errorf("Polled task ID = %q, want %q", polled.ID, task.ID)
	}

	if tq.PendingTaskCount() != 0 {
		t.Errorf("PendingTaskCount after poll = %d, want 0", tq.PendingTaskCount())
	}
}

func TestTaskQueue_PollTaskContextCancellation(t *testing.T) {
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 1000, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())

	// Start polling in background
	done := make(chan error, 1)
	go func() {
		_, err := tq.Poll(ctx, "worker-1")
		done <- err
	}()

	// Give poller time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Poll error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Error("Poll did not return after context cancellation")
	}
}

func TestTaskQueue_ConcurrentAddPoll(t *testing.T) {
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 10000, 1000, nil)

	const numTasks = 100
	var wg sync.WaitGroup

	// Add tasks concurrently
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := &Task{
				ID:            taskID(id),
				WorkflowID:    "workflow",
				ScheduledTime: time.Now(),
			}
			if err := tq.AddTask(task); err != nil {
				t.Errorf("AddTask error = %v", err)
			}
		}(i)
	}

	wg.Wait()

	if tq.PendingTaskCount() != numTasks {
		t.Errorf("PendingTaskCount = %d, want %d", tq.PendingTaskCount(), numTasks)
	}

	// Poll all tasks
	polledTasks := make(map[string]bool)
	for i := 0; i < numTasks; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		task, err := tq.Poll(ctx, "worker")
		cancel()
		if err != nil {
			t.Fatalf("Poll error = %v", err)
		}
		if polledTasks[task.ID] {
			t.Errorf("Task %s was polled twice", task.ID)
		}
		polledTasks[task.ID] = true
	}

	if len(polledTasks) != numTasks {
		t.Errorf("Polled %d unique tasks, want %d", len(polledTasks), numTasks)
	}
}

func TestTaskQueue_CompleteTask(t *testing.T) {
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 1000, 100, nil)

	task := &Task{
		ID:            "task-1",
		WorkflowID:    "workflow-1",
		ScheduledTime: time.Now(),
	}

	if err := tq.AddTask(task); err != nil {
		t.Fatalf("AddTask error = %v", err)
	}

	if !tq.CompleteTask("task-1") {
		t.Error("CompleteTask should return true for existing task")
	}

	if tq.CompleteTask("task-1") {
		t.Error("CompleteTask should return false for already completed task")
	}

	if tq.CompleteTask("nonexistent") {
		t.Error("CompleteTask should return false for nonexistent task")
	}
}

func TestTaskQueue_RateLimiting(t *testing.T) {
	// Create a queue with very low rate limit
	tq := NewTaskQueue("test-queue", TaskQueueKindNormal, 1, 1, nil) // 1 req/sec, burst 1

	// Add some tasks
	for i := 0; i < 5; i++ {
		if err := tq.AddTask(&Task{
			ID:            taskID(i),
			ScheduledTime: time.Now(),
		}); err != nil {
			t.Fatalf("AddTask error = %v", err)
		}
	}

	ctx := context.Background()

	// First poll should succeed
	_, err := tq.Poll(ctx, "worker")
	if err != nil {
		t.Fatalf("First poll error = %v", err)
	}

	// Second poll should be rate limited
	_, err = tq.Poll(ctx, "worker")
	if err != ErrRateLimited {
		t.Errorf("Expected ErrRateLimited, got %v", err)
	}
}

func taskID(i int) string {
	return fmt.Sprintf("task-%d", i)
}
