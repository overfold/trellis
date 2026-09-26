package agent

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/clofour/trellis/internal/api"
	"github.com/labstack/echo/v5"
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
	v1.POST("/allocations/:id/exec", h.handleExec)
	v1.POST("/allocations/:id/exec/sessions", h.handleCreateExecSession)
	v1.POST("/allocations/:id/exec/sessions/:session/input", h.handleExecSessionInput)
	v1.GET("/allocations/:id/exec/sessions/:session/output", h.handleExecSessionOutput)
	v1.POST("/allocations/:id/exec/sessions/:session/resize", h.handleExecSessionResize)
	v1.DELETE("/allocations/:id/exec/sessions/:session", h.handleExecSessionClose)
	v1.GET("/allocations/:id/metrics", h.handleMetrics)
}

func (h *Handler) handleNetworkPlan(c *echo.Context) error {
	var request api.NetworkPlanRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Namespace == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "namespace is required")
	}
	if err := h.agent.UpdateNetworkPlan(c.Request().Context(), &request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, api.OperationResponse{Code: api.OperationOK, Epoch: request.Epoch})
}

func (h *Handler) handleDrain(c *echo.Context) error {
	request := api.DrainAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.AllocationID == "" {
		request.AllocationID = c.Param("id")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "generation must be greater than zero")
	}
	if err := h.agent.DrainGroup(&request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, api.OperationResponse{Code: api.OperationOK, Generation: request.Generation})
}

func (h *Handler) handleResume(c *echo.Context) error {
	request := api.DrainAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Generation == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "generation must be greater than zero")
	}
	if err := h.agent.ResumeGroup(&request); err != nil {
		return operationError(err)
	}
	return c.JSON(http.StatusOK, api.OperationResponse{Code: api.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
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
	_, err = io.Copy(c.Response(), logs)
	return err
}

func (h *Handler) handleList(c *echo.Context) error {
	allocs := h.agent.GetAllocations()
	return c.JSON(http.StatusOK, allocs)
}

func (h *Handler) handleRun(c *echo.Context) error {
	ctx := c.Request().Context()

	var request api.AllocationRequest
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
		return echo.NewHTTPError(http.StatusBadRequest, "generation must be greater than zero")
	}
	if request.ExecutionHash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "execution_hash is required")
	}
	defer func() {
		for i := range request.Secrets {
			clear(request.Secrets[i].Value)
		}
	}()
	err = h.agent.RunGroup(ctx, &request)
	if err != nil {
		h.agent.log.Error("start allocation failed", "allocation", request.AllocationID, "error", err)
		return operationError(err)
	}

	return c.JSON(http.StatusOK, api.OperationResponse{Code: api.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
}

func (h *Handler) handleDelete(c *echo.Context) error {
	ctx := c.Request().Context()

	request := api.StopAllocationRequest{AllocationID: c.Param("id")}
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.AllocationID == "" {
		request.AllocationID = c.Param("id")
	}
	err := h.agent.StopGroup(ctx, &request)
	if err != nil {
		return operationError(err)
	}

	return c.JSON(http.StatusOK, api.OperationResponse{Code: api.OperationOK, Generation: request.Generation, Epoch: request.Epoch})
}

func (h *Handler) handleExec(c *echo.Context) error {
	var request api.AgentExecRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if len(request.Command) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "command is required")
	}
	result, err := h.agent.ExecAllocation(c.Request().Context(), c.Param("id"), request.Task, request.Command)
	if err != nil {
		if errors.Is(err, ErrAllocationNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleCreateExecSession(c *echo.Context) error {
	var request api.ExecSessionCreateRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if len(request.Command) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "command is required")
	}
	if request.Cols == 0 {
		request.Cols = 80
	}
	if request.Rows == 0 {
		request.Rows = 24
	}
	if request.Cols > 1000 || request.Rows > 1000 {
		return echo.NewHTTPError(http.StatusBadRequest, "terminal dimensions are too large")
	}
	result, err := h.agent.CreateExecSession(c.Request().Context(), c.Param("id"), request.Task, request.Command, request.Term, request.Cols, request.Rows)
	if err != nil {
		if errors.Is(err, ErrAllocationNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, result)
}

func (h *Handler) handleExecSessionInput(c *echo.Context) error {
	var request api.ExecSessionInputRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	data, err := base64.StdEncoding.DecodeString(request.DataBase64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "data_base64 must contain valid base64")
	}
	if len(data) > 64*1024 {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "terminal input chunk is too large")
	}
	if err := h.agent.WriteExecSession(c.Param("id"), c.Param("session"), data); err != nil {
		if errors.Is(err, ErrExecSessionNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExecSessionOutput(c *echo.Context) error {
	offset, err := strconv.ParseInt(c.QueryParam("offset"), 10, 64)
	if c.QueryParam("offset") == "" {
		offset, err = 0, nil
	}
	if err != nil || offset < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "offset must be a non-negative integer")
	}
	result, err := h.agent.ReadExecSession(c.Param("id"), c.Param("session"), offset)
	if err != nil {
		if errors.Is(err, ErrExecSessionNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleExecSessionResize(c *echo.Context) error {
	var request api.ExecSessionResizeRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Cols == 0 || request.Rows == 0 || request.Cols > 1000 || request.Rows > 1000 {
		return echo.NewHTTPError(http.StatusBadRequest, "terminal dimensions must be between 1 and 1000")
	}
	if err := h.agent.ResizeExecSession(c.Request().Context(), c.Param("id"), c.Param("session"), request.Cols, request.Rows); err != nil {
		if errors.Is(err, ErrExecSessionNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExecSessionClose(c *echo.Context) error {
	if err := h.agent.CloseExecSession(c.Request().Context(), c.Param("id"), c.Param("session")); err != nil {
		if errors.Is(err, ErrExecSessionNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
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
	status, code := http.StatusInternalServerError, api.OperationFailed
	switch {
	case errors.Is(err, ErrStaleEpoch):
		status, code = http.StatusConflict, api.OperationStaleEpoch
	case errors.Is(err, ErrStaleGeneration):
		status, code = http.StatusConflict, api.OperationStaleGeneration
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrAllocationExists):
		status, code = http.StatusConflict, api.OperationConflict
	}
	raw, _ := json.Marshal(api.OperationResponse{Code: code, Message: err.Error()})
	return echo.NewHTTPError(status, string(raw))
}
