package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const walFileName = "matching_wal.jsonl"

// WALEntry represents a single operation recorded in the write-ahead log.
type WALEntry struct {
	Operation string    `json:"op"`
	Task      *Task     `json:"task,omitempty"`
	TaskID    string    `json:"task_id,omitempty"`
	Timestamp time.Time `json:"ts"`
}

// WAL (Write-Ahead Log) provides crash recovery for in-memory task queues.
type WAL struct {
	dir     string
	file    *os.File
	encoder *json.Encoder
	mu      sync.Mutex
	logger  *slog.Logger
}

// NewWAL creates a new WAL in the given directory.
func NewWAL(dir string, logger *slog.Logger) (*WAL, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		dir:     dir,
		file:    f,
		encoder: json.NewEncoder(f),
		logger:  logger,
	}, nil
}

// WriteAdd records a task addition to the WAL.
func (w *WAL) WriteAdd(task *Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Operation: "add",
		Task:      task,
		TaskID:    task.ID,
		Timestamp: time.Now(),
	}
	if err := w.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write WAL add entry: %w", err)
	}
	return w.file.Sync()
}

// WriteComplete records a task completion to the WAL.
func (w *WAL) WriteComplete(taskID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Operation: "complete",
		TaskID:    taskID,
		Timestamp: time.Now(),
	}
	if err := w.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write WAL complete entry: %w", err)
	}
	return w.file.Sync()
}

// Recover replays the WAL and returns tasks that were added but never completed.
func (w *WAL) Recover() ([]*Task, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, walFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open WAL for recovery: %w", err)
	}
	defer f.Close()

	pending := make(map[string]*Task)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // 10MB max line size
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			w.logger.Warn("skipping corrupt WAL entry", slog.String("error", err.Error()))
			continue
		}

		switch entry.Operation {
		case "add":
			if entry.Task != nil {
				pending[entry.TaskID] = entry.Task
			}
		case "complete":
			delete(pending, entry.TaskID)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan WAL: %w", err)
	}

	tasks := make([]*Task, 0, len(pending))
	for _, task := range pending {
		tasks = append(tasks, task)
	}

	w.logger.Info("WAL recovery complete", slog.Int("recovered_tasks", len(tasks)))
	return tasks, nil
}

// Rotate compacts the WAL by removing completed tasks and rewriting active entries.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read all entries
	path := filepath.Join(w.dir, walFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open WAL for rotation: %w", err)
	}

	pending := make(map[string]*Task)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // 10MB max line size
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		switch entry.Operation {
		case "add":
			if entry.Task != nil {
				pending[entry.TaskID] = entry.Task
			}
		case "complete":
			delete(pending, entry.TaskID)
		}
	}
	f.Close()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan WAL for rotation: %w", err)
	}

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Write compacted WAL
	tmpPath := path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp WAL: %w", err)
	}

	encoder := json.NewEncoder(tmpFile)
	for _, task := range pending {
		entry := WALEntry{
			Operation: "add",
			Task:      task,
			TaskID:    task.ID,
			Timestamp: time.Now(),
		}
		if err := encoder.Encode(entry); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write compacted WAL entry: %w", err)
		}
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync temp WAL: %w", err)
	}
	tmpFile.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename compacted WAL: %w", err)
	}

	// Re-open for appending
	w.file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL after rotation: %w", err)
	}
	w.encoder = json.NewEncoder(w.file)

	w.logger.Info("WAL rotated", slog.Int("remaining_tasks", len(pending)))
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
