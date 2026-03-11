package handler

import (
	"context"

	"github.com/linkflow/engine/internal/frontend"
	"github.com/linkflow/engine/internal/frontend/validator"
)

type WorkflowHandler struct {
	service *frontend.Service
}

func NewWorkflowHandler(service *frontend.Service) *WorkflowHandler {
	return &WorkflowHandler{
		service: service,
	}
}

func (h *WorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	req *frontend.StartWorkflowExecutionRequest,
) (*frontend.StartWorkflowExecutionResponse, error) {
	if err := validator.ValidateStartWorkflowRequest(req); err != nil {
		return nil, err
	}

	return h.service.StartWorkflowExecution(ctx, req)
}

func (h *WorkflowHandler) SignalWorkflowExecution(
	ctx context.Context,
	req *frontend.SignalWorkflowExecutionRequest,
) error {
	if err := validator.ValidateSignalWorkflowRequest(req); err != nil {
		return err
	}

	return h.service.SignalWorkflowExecution(ctx, req)
}

func (h *WorkflowHandler) TerminateWorkflowExecution(
	ctx context.Context,
	req *frontend.TerminateWorkflowExecutionRequest,
) error {
	if err := validator.ValidateTerminateWorkflowRequest(req); err != nil {
		return err
	}

	return h.service.TerminateWorkflowExecution(ctx, req)
}

func (h *WorkflowHandler) QueryWorkflow(
	ctx context.Context,
	req *frontend.QueryWorkflowRequest,
) (*frontend.QueryWorkflowResponse, error) {
	if err := validator.ValidateQueryWorkflowRequest(req); err != nil {
		return nil, err
	}

	return h.service.QueryWorkflow(ctx, req)
}
