package archival

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

var (
	ErrArchiveNotFound = errors.New("archive not found")
	ErrArchiveFailed   = errors.New("archival failed")
)

// Archiver handles workflow history archival.
type Archiver struct {
	storage BlobStorage
	policy  *Policy
	logger  *slog.Logger
}

// BlobStorage is the interface for blob storage backends.
type BlobStorage interface {
	Put(ctx context.Context, key string, data io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// Policy defines archival policy.
type Policy struct {
	Enabled           bool
	RetentionPeriod   time.Duration
	ArchiveAfter      time.Duration
	CompressionType   string // none, gzip, zstd
	EncryptionEnabled bool
}

// DefaultPolicy returns the default archival policy.
func DefaultPolicy() *Policy {
	return &Policy{
		Enabled:           true,
		RetentionPeriod:   365 * 24 * time.Hour, // 1 year
		ArchiveAfter:      30 * 24 * time.Hour,  // 30 days
		CompressionType:   "gzip",
		EncryptionEnabled: false,
	}
}

// NewArchiver creates a new archiver.
func NewArchiver(storage BlobStorage, policy *Policy, logger *slog.Logger) *Archiver {
	if policy == nil {
		policy = DefaultPolicy()
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Archiver{
		storage: storage,
		policy:  policy,
		logger:  logger,
	}
}

// ArchiveRequest represents a request to archive an execution.
type ArchiveRequest struct {
	NamespaceID string
	ExecutionID string
	WorkflowID  string
	Events      []*types.HistoryEvent
	ClosedAt    time.Time
}

// Archive archives a workflow execution.
func (a *Archiver) Archive(ctx context.Context, req *ArchiveRequest) error {
	if !a.policy.Enabled {
		return nil
	}

	// Create archive
	archive := &Archive{
		ExecutionID: req.ExecutionID,
		WorkflowID:  req.WorkflowID,
		NamespaceID: req.NamespaceID,
		Events:      req.Events,
		ArchivedAt:  time.Now(),
		ClosedAt:    req.ClosedAt,
		Version:     1,
	}

	// Serialize
	data, err := json.Marshal(archive)
	if err != nil {
		return fmt.Errorf("failed to serialize archive: %w", err)
	}

	// Generate key
	key := a.generateKey(req.NamespaceID, req.ExecutionID, archive.ArchivedAt)

	// Store
	reader := &bytesReader{data: data}
	if err := a.storage.Put(ctx, key, reader); err != nil {
		return fmt.Errorf("failed to store archive: %w", err)
	}

	a.logger.Info("execution archived",
		slog.String("execution_id", req.ExecutionID),
		slog.String("key", key),
		slog.Int("event_count", len(req.Events)),
	)

	return nil
}

// Archive represents an archived execution.
type Archive struct {
	ExecutionID string                `json:"execution_id"`
	WorkflowID  string                `json:"workflow_id"`
	NamespaceID string                `json:"namespace_id"`
	Events      []*types.HistoryEvent `json:"events"`
	ArchivedAt  time.Time             `json:"archived_at"`
	ClosedAt    time.Time             `json:"closed_at"`
	Version     int                   `json:"version"`
}

// Retrieve retrieves an archived execution.
func (a *Archiver) Retrieve(ctx context.Context, namespaceID, executionID string) (*Archive, error) {
	// List possible keys for this execution
	prefix := fmt.Sprintf("%s/%s/", namespaceID, executionID)
	keys, err := a.storage.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, ErrArchiveNotFound
	}

	// Get the most recent archive (last key when sorted)
	key := keys[len(keys)-1]

	reader, err := a.storage.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get archive: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read archive: %w", err)
	}

	var archive Archive
	if err := json.Unmarshal(data, &archive); err != nil {
		return nil, fmt.Errorf("failed to parse archive: %w", err)
	}

	return &archive, nil
}

// Delete deletes an archived execution.
func (a *Archiver) Delete(ctx context.Context, namespaceID, executionID string) error {
	prefix := fmt.Sprintf("%s/%s/", namespaceID, executionID)
	keys, err := a.storage.List(ctx, prefix)
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := a.storage.Delete(ctx, key); err != nil {
			return fmt.Errorf("failed to delete archive %s: %w", key, err)
		}
	}

	return nil
}

// CleanupExpired removes archives past retention period.
func (a *Archiver) CleanupExpired(ctx context.Context, namespaceID string) (int, error) {
	if a.policy.RetentionPeriod == 0 {
		return 0, nil // Infinite retention
	}

	cutoff := time.Now().Add(-a.policy.RetentionPeriod)
	prefix := namespaceID + "/"

	keys, err := a.storage.List(ctx, prefix)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, key := range keys {
		// Get archive to check date
		reader, err := a.storage.Get(ctx, key)
		if err != nil {
			continue
		}

		data, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			continue
		}

		var archive Archive
		if err := json.Unmarshal(data, &archive); err != nil {
			continue
		}

		if archive.ArchivedAt.Before(cutoff) {
			if err := a.storage.Delete(ctx, key); err != nil {
				a.logger.Warn("failed to delete expired archive",
					slog.String("key", key),
					slog.String("error", err.Error()),
				)
				continue
			}
			deleted++
		}
	}

	a.logger.Info("expired archives cleaned up",
		slog.String("namespace_id", namespaceID),
		slog.Int("deleted", deleted),
	)

	return deleted, nil
}

func (a *Archiver) generateKey(namespaceID, executionID string, archivedAt time.Time) string {
	return fmt.Sprintf("%s/%s/%s.json",
		namespaceID,
		executionID,
		archivedAt.Format("2006-01-02T15-04-05"),
	)
}

// bytesReader is a simple io.Reader wrapper for []byte.
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// InMemoryStorage is an in-memory blob storage for testing.
type InMemoryStorage struct {
	data map[string][]byte
}

// NewInMemoryStorage creates a new in-memory storage.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[string][]byte),
	}
}

func (s *InMemoryStorage) Put(ctx context.Context, key string, data io.Reader) error {
	bytes, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.data[key] = bytes
	return nil
}

func (s *InMemoryStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, exists := s.data[key]
	if !exists {
		return nil, ErrArchiveNotFound
	}
	return io.NopCloser(&bytesReader{data: data}), nil
}

func (s *InMemoryStorage) Delete(ctx context.Context, key string) error {
	delete(s.data, key)
	return nil
}

func (s *InMemoryStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range s.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}
