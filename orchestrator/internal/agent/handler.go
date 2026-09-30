package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// Handler exposes agent operations through HTTP.
type Handler struct {
	agent *Agent
}

// NewHandler creates an HTTP handler for an agent.
func NewHandler(agent *Agent) *Handler {
	return &Handler{
		agent: agent,
	}
}

// Register adds agent routes to an Echo instance.
func (h *Handler) Register(e *echo.Echo) {
	v1 := e.Group("/v1")
	v1.GET("/allocations", h.handleList)
	v1.POST("/allocations", h.handleRun)
	v1.POST("/allocations/:id/drain", h.handleDrain)
	v1.DELETE("/allocations/:id/drain", h.handleResume)
	v1.POST("/network-plans", h.handleNetworkPlan)
	v1.DELETE("/allocations/:id", h.handleDelete)
	v1.GET("/allocations/:id/logs", h.handleLogs)
	v1.GET("/allocations/:id/exec", h.handleExec)
	v1.GET("/allocations/:id/metrics", h.handleMetrics)
}

func (h *Handler) handleNetworkPlan(c *echo.Context) error {
	var request nodeapi.NetworkPlanRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Namespace == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "namespace is required")
	}
	if request.Epoch == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidEpoch.Error())
	}
	if err := h.agent.UpdateNetworkPlan(c.Request().Context(), &request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, nodeapi.OperationResponse{Code: nodeapi.OperationOK, Epoch: request.Epoch})
}

func (h *Handler) handleDrain(c *echo.Context) error {
	request := nodeapi.DrainAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.AllocationID == "" {
		request.AllocationID = c.Param("id")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidGeneration.Error())
	}
	if request.Epoch == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidEpoch.Error())
	}
	if err := h.agent.DrainGroup(&request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, nodeapi.OperationResponse{Code: nodeapi.OperationOK, Generation: request.Generation})
}

func (h *Handler) handleResume(c *echo.Context) error {
	request := nodeapi.DrainAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidGeneration.Error())
	}
	if request.Epoch == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidEpoch.Error())
	}
	if err := h.agent.ResumeGroup(&request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, nodeapi.OperationResponse{Code: nodeapi.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
}

func (h *Handler) handleLogs(c *echo.Context) error {
	tail, err := strconv.Atoi(c.QueryParam("tail"))
	if c.QueryParam("tail") == "" {
		tail, err = 100, nil
	}
	if err != nil || tail < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "tail must be a non-negative integer")
	}
	logs, err := h.agent.TaskLogs(
		c.Request().Context(),
		c.Param("id"),
		c.QueryParam("task"),
		c.QueryParam("follow") == "true",
		tail,
	)
	if err != nil {
		if errors.Is(err, ErrAllocationNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	defer func() { _ = logs.Close() }()
	c.Response().Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.Response().WriteHeader(http.StatusOK)
	// Send the headers now: a followed task may not log for a long time.
	_ = http.NewResponseController(c.Response()).Flush()
	_, err = io.Copy(c.Response(), logs)
	return err
}

func (h *Handler) handleList(c *echo.Context) error {
	allocs := h.agent.GetAllocations()
	return c.JSON(http.StatusOK, allocs)
}

func (h *Handler) handleRun(c *echo.Context) error {
	ctx := c.Request().Context()

	var request nodeapi.AllocationRequest
	err := c.Bind(&request)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}

	if len(request.Tasks) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "allocation tasks are required")
	}
	if request.AllocationID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "allocation_id is required")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidGeneration.Error())
	}
	if request.JobRevision <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "job_revision must be greater than zero")
	}
	if request.ExecutionHash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "execution_hash is required")
	}
	if request.Epoch == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidEpoch.Error())
	}
	if err := validateCanonicalRequest(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	defer func() {
		for i := range request.Secrets {
			clear(request.Secrets[i].Value)
		}
	}()
	// The agent accepts a fenced start and pulls images and creates tasks in
	// the background; heartbeats report progress and failure.
	if err := h.agent.StartGroup(ctx, &request); err != nil {
		h.agent.log.Error("accept allocation start failed", "allocation", request.AllocationID, "error", err)
		return operationError(err)
	}

	return c.JSON(http.StatusOK, nodeapi.OperationResponse{Code: nodeapi.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
}

func (h *Handler) handleDelete(c *echo.Context) error {
	ctx := c.Request().Context()

	request := nodeapi.StopAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.AllocationID == "" {
		request.AllocationID = c.Param("id")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidGeneration.Error())
	}
	if request.Epoch == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, ErrInvalidEpoch.Error())
	}
	err := h.agent.StopGroup(ctx, &request)
	if err != nil {
		return operationError(err)
	}

	return c.JSON(http.StatusOK, nodeapi.OperationResponse{Code: nodeapi.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
}

// handleExec serves an exec stream. Request errors are ordinary HTTP
// responses; once the connection is upgraded, the stream reports the
// process's end with an exit or error frame.
func (h *Handler) handleExec(c *echo.Context) error {
	if !execstream.IsUpgradeRequest(c.Request()) {
		return echo.NewHTTPError(http.StatusBadRequest, "exec requires an HTTP/1.1 upgrade to "+execstream.Protocol)
	}
	execRequest, epoch, err := execstream.DecodeAgentRequest(c.QueryParams())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	request := nodeapi.AgentExecRequest{ExecRequest: execRequest, Epoch: epoch}
	reservation, err := h.agent.ReserveExec(c.Request().Context(), c.Param("id"), request)
	if err != nil {
		return execError(err)
	}
	conn, err := execstream.Accept(c.Response())
	if err != nil {
		h.agent.ReleaseExec(reservation)
		h.agent.log.Warn("accept exec stream", "allocation", c.Param("id"), "error", err)
		return nil
	}
	h.agent.RunExec(conn, reservation)
	return nil
}

// execError maps a rejected exec request to its HTTP status.
func execError(err error) error {
	switch {
	case errors.Is(err, ErrAllocationNotFound):
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	case errors.Is(err, ErrExecTaskRequired), errors.Is(err, ErrInvalidEpoch):
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrStaleEpoch):
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	case errors.Is(err, ErrExecSessionLimit):
		return echo.NewHTTPError(http.StatusTooManyRequests, err.Error())
	case errors.Is(err, ErrAgentShuttingDown):
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	default:
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
}

func (h *Handler) handleMetrics(c *echo.Context) error {
	metrics, err := h.agent.AllocationMetrics(c.Request().Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, ErrAllocationNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, metrics)
}

func operationError(err error) error {
	status, code := http.StatusInternalServerError, nodeapi.OperationFailed
	switch {
	case errors.Is(err, ErrStaleEpoch):
		status, code = http.StatusConflict, nodeapi.OperationStaleEpoch
	case errors.Is(err, ErrStaleGeneration):
		status, code = http.StatusConflict, nodeapi.OperationStaleGeneration
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrAllocationExists):
		status, code = http.StatusConflict, nodeapi.OperationConflict
	case errors.Is(err, ErrInvalidEpoch), errors.Is(err, ErrInvalidGeneration):
		status = http.StatusBadRequest
	case errors.Is(err, ErrRestartBudgetExhausted):
		status, code = http.StatusConflict, nodeapi.OperationRestartExhausted
	}
	raw, _ := json.Marshal(nodeapi.OperationResponse{Code: code, Message: err.Error()})
	return echo.NewHTTPError(status, string(raw))
}

// validateCanonicalRequest refuses a start whose task group settings were not
// resolved by job canonicalization. The agent applies no defaults of its own.
func validateCanonicalRequest(request *nodeapi.AllocationRequest) error {
	if !spec.Runtime(request.Runtime).Valid() || request.Runtime == string(spec.RuntimeDefault) {
		return fmt.Errorf("runtime must be explicit")
	}
	if request.Restart == nil {
		return fmt.Errorf("restart policy is required")
	}
	for i := range request.Tasks {
		if err := spec.ValidateCanonicalTask(&request.Tasks[i]); err != nil {
			return fmt.Errorf("task %q: %w", request.Tasks[i].Name, err)
		}
	}
	return nil
}
