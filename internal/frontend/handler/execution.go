package handler

import (
	"context"

	"github.com/linkflow/engine/internal/frontend"
	"github.com/linkflow/engine/internal/frontend/validator"
)

type ExecutionHandler struct {
	service *frontend.Service
}

func NewExecutionHandler(service *frontend.Service) *ExecutionHandler {
	return &ExecutionHandler{
		service: service,
	}
}

func (h *ExecutionHandler) GetExecution(
	ctx context.Context,
	req *frontend.GetExecutionRequest,
) (*frontend.GetExecutionResponse, error) {
	if err := validator.ValidateGetExecutionRequest(req); err != nil {
		return nil, err
	}

	return h.service.GetExecution(ctx, req)
}

func (h *ExecutionHandler) ListExecutions(
	ctx context.Context,
	req *frontend.ListExecutionsRequest,
) (*frontend.ListExecutionsResponse, error) {
	if err := validator.ValidateListExecutionsRequest(req); err != nil {
		return nil, err
	}

	return h.service.ListExecutions(ctx, req)
}

func (h *ExecutionHandler) DescribeExecution(
	ctx context.Context,
	req *frontend.DescribeExecutionRequest,
) (*frontend.DescribeExecutionResponse, error) {
	if err := validator.ValidateDescribeExecutionRequest(req); err != nil {
		return nil, err
	}

	return h.service.DescribeExecution(ctx, req)
}
