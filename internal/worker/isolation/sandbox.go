package isolation

import (
	"context"
	"os"
	"path/filepath"

	"github.com/linkflow/engine/internal/worker/executor"
)

type Sandbox interface {
	Execute(ctx context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResponse, error)
	Cleanup() error
}

type ProcessSandbox struct {
	workDir string
}

func NewProcessSandbox(workDir string) (*ProcessSandbox, error) {
	if workDir == "" {
		var err error
		workDir, err = os.MkdirTemp("", "sandbox-*")
		if err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return nil, err
	}

	return &ProcessSandbox{
		workDir: workDir,
	}, nil
}

func (s *ProcessSandbox) Execute(ctx context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResponse, error) {
	return &executor.ExecuteResponse{
		Error: &executor.ExecutionError{
			Message: "process sandbox execution not yet implemented",
			Type:    executor.ErrorTypeNonRetryable,
		},
	}, nil
}

func (s *ProcessSandbox) Cleanup() error {
	if s.workDir == "" {
		return nil
	}

	return os.RemoveAll(s.workDir)
}

func (s *ProcessSandbox) WorkDir() string {
	return s.workDir
}

func (s *ProcessSandbox) CreateWorkFile(name string, content []byte) (string, error) {
	path := filepath.Join(s.workDir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return "", err
	}
	return path, nil
}
