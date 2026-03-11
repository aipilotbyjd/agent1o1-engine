package matching

import "errors"

var (
	ErrTaskQueueNotFound = errors.New("task queue not found")
	ErrTaskNotFound      = errors.New("task not found")
	ErrRateLimited       = errors.New("rate limited")
)
