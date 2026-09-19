package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
	"github.com/clofour/trellis/internal/catalog"
	"github.com/clofour/trellis/internal/plan"
	secretstore "github.com/clofour/trellis/internal/secrets"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler exposes a Server through HTTP routes.
type Handler struct {
	server *Server
}

type contextKey string

// NamespaceContextKey stores the authenticated namespace or encoded scoped authorization in a request context.
const NamespaceContextKey contextKey = "trellis-namespace"

// AdminContextKey stores bootstrap cluster-administrator status in a request context.
const AdminContextKey contextKey = "trellis-admin"

type requestAuthorization struct {
	root      bool
	scope     auth.AccessScope
	access    auth.AccessLevel
	namespace string
}

func authorization(c *echo.Context) requestAuthorization {
	if admin, _ := c.Request().Context().Value(AdminContextKey).(bool); admin {
		return requestAuthorization{root: true, scope: auth.AccessCluster, access: auth.AccessWrite}
	}
	value, _ := c.Request().Context().Value(NamespaceContextKey).(string)
	if scope, access, namespace, ok := auth.DecodeScope(value); ok {
		return requestAuthorization{scope: scope, access: access, namespace: namespace}
	}
	return requestAuthorization{}
}

func requestNamespace(c *echo.Context) string {
	authz := authorization(c)
	if authz.scope == auth.AccessNamespace {
		return authz.namespace
	}
	return c.Request().Header.Get("X-Trellis-Namespace")
}

func requireRoot(c *echo.Context, message string) error {
	if !authorization(c).root {
		return echo.NewHTTPError(http.StatusForbidden, message)
	}
	return nil
}

func requireClusterRead(c *echo.Context, message string) error {
	authz := authorization(c)
	if !authz.root && authz.scope != auth.AccessCluster {
		return echo.NewHTTPError(http.StatusForbidden, message)
	}
	return nil
}

func requireWrite(c *echo.Context, message string) error {
	authz := authorization(c)
	if !authz.root && authz.access != auth.AccessWrite {
		return echo.NewHTTPError(http.StatusForbidden, message)
	}
	return nil
}

func requireClusterWrite(c *echo.Context, message string) error {
	authz := authorization(c)
	if !authz.root && (authz.scope != auth.AccessCluster || authz.access != auth.AccessWrite) {
		return echo.NewHTTPError(http.StatusForbidden, message)
	}
	return nil
}

func requireAPIAccessDelegation(c *echo.Context, job *spec.JobSpec) error {
	authz := authorization(c)
	if authz.root {
		return nil
	}
	if authz.scope != auth.AccessCluster && authz.scope != auth.AccessNamespace {
		return echo.NewHTTPError(http.StatusForbidden, "API access delegation requires an authenticated scoped credential")
	}
	for i := range job.TaskGroups {
		requested := job.TaskGroups[i].APIAccess
		if requested == nil {
			continue
		}
		if authz.scope == auth.AccessNamespace && auth.AccessScope(requested.Scope) == auth.AccessCluster {
			return echo.NewHTTPError(http.StatusForbidden, "job api_access exceeds caller scope")
		}
		if authz.access == auth.AccessRead && auth.AccessLevel(requested.Access) == auth.AccessWrite {
			return echo.NewHTTPError(http.StatusForbidden, "job api_access exceeds caller access")
		}
	}
	return nil
}

// NewHandler creates an HTTP handler for server.
func NewHandler(server *Server) *Handler { return &Handler{server: server} }

// Register adds server routes to an Echo instance.
func (h *Handler) Register(e *echo.Echo) {
	e.GET("/metrics", h.handleMetrics)
	v1 := e.Group("/v1")
	v1.POST("/credentials", h.handleCreateCredential)
	v1.GET("/nodes", h.handleListNodes)
	v1.POST("/nodes", h.handleRegisterNode)
	v1.POST("/nodes/:id/heartbeat", h.handleHeartbeat)
	v1.POST("/nodes/:id/drain", h.handleDrainNode)
	v1.DELETE("/nodes/:id/drain", h.handleUndrainNode)
	v1.GET("/jobs", h.handleListJobs)
	v1.POST("/jobs", h.handleRegisterJob)
	v1.POST("/jobs/plan", h.handlePlanJob)
	v1.GET("/jobs/:name", h.handleGetJob)
	v1.DELETE("/jobs/:name", h.handleDeleteJob)
	v1.POST("/jobs/:name/restart", h.handleRestartJob)
	v1.GET("/jobs/:name/revisions", h.handleListJobRevisions)
	v1.GET("/namespaces", h.handleListNamespaces)
	v1.GET("/allocations", h.handleListAllocations)
	v1.DELETE("/allocations/:id", h.handleStopAllocation)
	v1.GET("/allocations/:id/events", h.handleAllocationEvents)
	v1.GET("/allocations/:id/logs", h.handleAllocationLogs)
	v1.POST("/allocations/:id/exec", h.handleExecAllocation)
	v1.POST("/allocations/:id/exec/sessions", h.handleCreateExecSession)
	v1.POST("/allocations/:id/exec/sessions/:session/input", h.handleExecSessionInput)
	v1.GET("/allocations/:id/exec/sessions/:session/output", h.handleExecSessionOutput)
	v1.POST("/allocations/:id/exec/sessions/:session/resize", h.handleExecSessionResize)
	v1.DELETE("/allocations/:id/exec/sessions/:session", h.handleExecSessionClose)
	v1.GET("/allocations/:id/metrics", h.handleAllocationMetrics)
	v1.GET("/events", h.handleEvents)
	v1.GET("/internal/discovery", h.handleListDiscovery)
	v1.POST("/raft/join", h.handleRaftJoin)
	v1.DELETE("/raft/members/:id", h.handleRaftMemberRemove)
	v1.POST("/raft/leadership-transfer", h.handleRaftLeadershipTransfer)
	v1.GET("/backup", h.handleBackupCreate)
	v1.POST("/backup/restore", h.handleBackupRestore)
	v1.PUT("/namespaces/:namespace/secrets/:name", h.handleSetSecret)
	v1.GET("/namespaces/:namespace/secrets", h.handleListSecrets)
	v1.GET("/namespaces/:namespace/secrets/:name", h.handleGetSecret)
	v1.DELETE("/namespaces/:namespace/secrets/:name", h.handleDeleteSecret)
}

func (h *Handler) handleCreateCredential(c *echo.Context) error {
	if err := requireRoot(c, "credential creation requires the bootstrap cluster credential"); err != nil {
		return err
	}
	var request api.CredentialCreateRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	scope := auth.AccessScope(request.Scope)
	access := auth.AccessLevel(request.Access)
	if scope != auth.AccessNamespace && scope != auth.AccessCluster {
		return echo.NewHTTPError(http.StatusBadRequest, "scope must be namespace or cluster")
	}
	if access != auth.AccessRead && access != auth.AccessWrite {
		return echo.NewHTTPError(http.StatusBadRequest, "access must be read or write")
	}
	if scope == auth.AccessNamespace {
		if !spec.ValidIdentifier(request.Namespace) {
			return echo.NewHTTPError(http.StatusBadRequest, "namespace scope requires a valid namespace")
		}
	} else if request.Namespace != "" {
		return echo.NewHTTPError(http.StatusBadRequest, "cluster scope must not include a namespace")
	}
	token, err := h.server.CreateCredential(c.Request().Context(), scope, access, request.Namespace)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusCreated, api.CredentialCreateResponse{Token: token})
}

func (h *Handler) handleBackupCreate(c *echo.Context) error {
	if err := requireRoot(c, "backup operations require the bootstrap cluster credential"); err != nil {
		return err
	}
	backup, err := h.server.Backup(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, backup)
}

func (h *Handler) handleBackupRestore(c *echo.Context) error {
	if err := requireRoot(c, "backup operations require the bootstrap cluster credential"); err != nil {
		return err
	}
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, 64<<20)
	var backup api.BackupSnapshot
	if err := c.Bind(&backup); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid backup snapshot")
	}
	if err := h.server.Restore(c.Request().Context(), &backup); err != nil {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func secretNamespace(c *echo.Context) (string, error) {
	c.Response().Header().Set("Cache-Control", "no-store")
	ns := c.Param("namespace")
	if !spec.ValidIdentifier(ns) {
		return "", echo.NewHTTPError(http.StatusBadRequest, "invalid namespace")
	}
	if requested := requestNamespace(c); requested != "" && requested != ns {
		return "", echo.NewHTTPError(http.StatusForbidden, "namespace does not match selected namespace")
	}
	return ns, nil
}

func (h *Handler) handleSetSecret(c *echo.Context) error {
	if err := requireClusterWrite(c, "setting secrets requires cluster/write authorization"); err != nil {
		return err
	}
	ns, err := secretNamespace(c)
	if err != nil {
		return err
	}
	if !spec.ValidIdentifier(c.Param("name")) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid secret name")
	}
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, 96<<10)
	var request api.SecretWriteRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	value, err := base64.StdEncoding.DecodeString(request.ValueBase64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "value_base64 is invalid")
	}
	defer clear(value)
	meta, err := h.server.SetSecret(c.Request().Context(), ns, c.Param("name"), value, request.ExpectedVersion)
	if errors.Is(err, secretstore.ErrVersionConflict) {
		return echo.NewHTTPError(http.StatusConflict, "secret version conflict")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to store secret")
	}
	return c.JSON(http.StatusOK, meta)
}

func (h *Handler) handleListSecrets(c *echo.Context) error {
	if err := requireClusterRead(c, "secret metadata requires cluster/read authorization"); err != nil {
		return err
	}
	ns, err := secretNamespace(c)
	if err != nil {
		return err
	}
	items, err := h.server.ListSecrets(c.Request().Context(), ns)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to list secrets")
	}
	return c.JSON(http.StatusOK, items)
}

func (h *Handler) handleGetSecret(c *echo.Context) error {
	if err := requireClusterRead(c, "secret metadata requires cluster/read authorization"); err != nil {
		return err
	}
	ns, err := secretNamespace(c)
	if err != nil {
		return err
	}
	meta, err := h.server.GetSecretMetadata(c.Request().Context(), ns, c.Param("name"))
	if errors.Is(err, secretstore.ErrNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "secret not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to read secret metadata")
	}
	return c.JSON(http.StatusOK, meta)
}

func (h *Handler) handleDeleteSecret(c *echo.Context) error {
	if err := requireClusterWrite(c, "deleting secrets requires cluster/write authorization"); err != nil {
		return err
	}
	ns, err := secretNamespace(c)
	if err != nil {
		return err
	}
	if err := h.server.DeleteSecret(c.Request().Context(), ns, c.Param("name")); errors.Is(err, secretstore.ErrNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "secret not found")
	} else if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to delete secret")
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleAllocationEvents(c *echo.Context) error {
	events, ok := h.server.AllocationEvents(requestNamespace(c), c.Param("id"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "allocation not found")
	}
	if events == nil {
		events = api.AllocationEventListResponse{}
	}
	return c.JSON(http.StatusOK, events)
}

func (h *Handler) handleAllocationLogs(c *echo.Context) error {
	tail, err := strconv.Atoi(c.QueryParam("tail"))
	if c.QueryParam("tail") == "" {
		tail, err = 100, nil
	}
	if err != nil || tail < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "tail must be a non-negative integer")
	}
	logs, err := h.server.AllocationTaskLogsForNamespace(c.Request().Context(), requestNamespace(c), c.Param("id"), c.QueryParam("task"), c.QueryParam("follow") == "true", tail)
	if errors.Is(err, ErrTaskSelection) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "allocation or task logs not found")
	}
	defer func() { _ = logs.Close() }()
	c.Response().Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.Response().WriteHeader(http.StatusOK)
	_, err = io.Copy(c.Response(), logs)
	return err
}

func (h *Handler) handleDrainNode(c *echo.Context) error {
	if err := requireClusterWrite(c, "draining nodes requires cluster/write authorization"); err != nil {
		return err
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid node ID")
	}
	if err := h.server.DrainNode(c.Request().Context(), id); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "node not found")
	}
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleUndrainNode(c *echo.Context) error {
	if err := requireClusterWrite(c, "undraining nodes requires cluster/write authorization"); err != nil {
		return err
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid node ID")
	}
	if err := h.server.UndrainNode(c.Request().Context(), id); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "node not found")
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListNodes(c *echo.Context) error {
	if err := requireClusterRead(c, "nodes are cluster-scoped"); err != nil {
		return err
	}
	nodes := h.server.ListNodes()
	result := make(api.NodeListResponse, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, *h.convertNode(&node))
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleRegisterNode(c *echo.Context) error {
	if err := requireRoot(c, "node registration requires the bootstrap cluster credential"); err != nil {
		return err
	}
	var request api.NodeRegistrationRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := h.server.RegisterNode(c.Request().Context(), &NodeRegistration{
		ID: request.ID, Host: request.Host, Port: request.Port, CPU: request.CPU, Memory: request.Memory,
		OS: request.OS, Arch: request.Arch, Labels: request.Labels, Volumes: request.Volumes, Capabilities: request.Capabilities,
		WireGuardPublicKey: request.WireGuardPublicKey, WireGuardEndpoint: request.WireGuardEndpoint,
		WireGuardPortBase: request.WireGuardPortBase, WireGuardPortCount: request.WireGuardPortCount,
	}); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "unable to register node")
	}
	return c.JSON(http.StatusCreated, api.NodeRegistrationResponse{ID: request.ID})
}

func (h *Handler) handleHeartbeat(c *echo.Context) error {
	if err := requireRoot(c, "node heartbeats require the bootstrap cluster credential"); err != nil {
		return err
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	var request api.HeartbeatRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := h.server.Heartbeat(c.Request().Context(), id, request.Allocations, request.Version, request.Volumes, request.Capabilities); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "unable to process heartbeat")
	}
	return c.JSON(http.StatusOK, h.server.HeartbeatResponse(id))
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	return c.JSON(http.StatusOK, h.server.ListJobs(requestNamespace(c)))
}

func (h *Handler) handleGetJob(c *echo.Context) error {
	status, ok := h.server.GetJob(requestNamespace(c), c.Param("name"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "job not found")
	}
	return c.JSON(http.StatusOK, status)
}

func (h *Handler) handleDeleteJob(c *echo.Context) error {
	if err := requireWrite(c, "deleting jobs requires write authorization"); err != nil {
		return err
	}
	if err := h.server.DeleteJob(c.Request().Context(), requestNamespace(c), c.Param("name")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "job not found")
	}
	return c.NoContent(http.StatusNoContent)
}

func validationResponse(c *echo.Context, err error) error {
	var issues spec.ValidationErrors
	if errors.As(err, &issues) {
		return c.JSON(http.StatusUnprocessableEntity, map[string]any{"error": "job is invalid", "issues": issues})
	}
	return echo.NewHTTPError(http.StatusUnprocessableEntity, err.Error())
}

func (h *Handler) handlePlanJob(c *echo.Context) error {
	var request api.JobRegistrationRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := h.server.CanonicalizeJob(&request.Spec); err != nil {
		return validationResponse(c, err)
	}
	selected := requestNamespace(c)
	if selected != "" && selected != request.Spec.Namespace {
		return echo.NewHTTPError(http.StatusForbidden, "manifest namespace does not match selected namespace")
	}
	if err := h.server.ValidateNamespaceAllocationLimit(request.Spec.Namespace, &request.Spec); err != nil {
		return validationResponse(c, err)
	}
	if err := requireAPIAccessDelegation(c, &request.Spec); err != nil {
		return err
	}
	var currentSpec *spec.JobSpec
	var revision int
	if current, ok := h.server.GetJob(request.Spec.Namespace, request.Spec.Name); ok {
		currentSpec, revision = current.Spec, current.Revision
	}
	return c.JSON(http.StatusOK, plan.Build(currentSpec, revision, &request.Spec))
}

func (h *Handler) handleRegisterJob(c *echo.Context) error {
	if err := requireWrite(c, "applying jobs requires write authorization"); err != nil {
		return err
	}
	var request api.JobRegistrationRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := h.server.CanonicalizeJob(&request.Spec); err != nil {
		return validationResponse(c, err)
	}
	selected := requestNamespace(c)
	if selected != "" && selected != request.Spec.Namespace {
		return echo.NewHTTPError(http.StatusForbidden, "manifest namespace does not match selected namespace")
	}
	if err := requireAPIAccessDelegation(c, &request.Spec); err != nil {
		return err
	}
	if err := h.server.RegisterJob(c.Request().Context(), request.Spec.Namespace, &request.Spec); err != nil {
		return validationResponse(c, err)
	}
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListAllocations(c *echo.Context) error {
	var filter *AllocationListFilter
	job, label := c.QueryParam("job"), c.QueryParam("label")
	if job != "" || label != "" {
		filter = &AllocationListFilter{Job: job, Label: label}
	}
	allocations := h.server.ListAllocations(requestNamespace(c), filter)
	if allocations == nil {
		allocations = api.AllocationListResponse{}
	}
	return c.JSON(http.StatusOK, allocations)
}

func (h *Handler) handleListDiscovery(c *echo.Context) error {
	if err := requireRoot(c, "internal discovery requires the bootstrap cluster credential"); err != nil {
		return err
	}
	var filter *catalog.ListFilter
	job, label := c.QueryParam("job"), c.QueryParam("label")
	if job != "" || label != "" {
		filter = &catalog.ListFilter{Job: job, Label: label}
	}
	entries := h.server.ListServices("", filter)
	if entries == nil {
		entries = api.ServiceListResponse{}
	}
	return c.JSON(http.StatusOK, entries)
}

func (h *Handler) handleRaftJoin(c *echo.Context) error {
	if err := requireRoot(c, "Raft membership changes require the bootstrap cluster credential"); err != nil {
		return err
	}
	var request api.RaftJoinRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.ID == "" || request.RaftAddress == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id and raft_address are required")
	}
	if h.server.joiner == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "cluster join not available")
	}
	if err := h.server.joiner.AddVoter(request.ID, request.RaftAddress); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	caCert, caKey, err := h.server.ClusterCA()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "cluster CA unavailable")
	}
	return c.JSON(http.StatusOK, api.RaftJoinResponse{CACert: caCert, CAKey: caKey})
}

func (h *Handler) handleRaftMemberRemove(c *echo.Context) error {
	if err := requireRoot(c, "Raft membership changes require the bootstrap cluster credential"); err != nil {
		return err
	}
	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id is required")
	}
	if h.server.joiner == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "cluster membership changes not available")
	}
	if err := h.server.joiner.RemoveServer(id); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleRaftLeadershipTransfer(c *echo.Context) error {
	if err := requireRoot(c, "leadership transfer requires the bootstrap cluster credential"); err != nil {
		return err
	}
	if h.server.joiner == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "Raft leadership transfer not available")
	}
	if err := h.server.joiner.LeadershipTransfer(); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleMetrics(c *echo.Context) error {
	promhttp.Handler().ServeHTTP(c.Response(), c.Request())
	return nil
}

func (h *Handler) convertNode(node *Node) *api.NodeResponse {
	return &api.NodeResponse{
		ID: node.ID, Host: node.Host, Port: node.Port, Status: api.NodeStatusResponse(node.Status),
		LastHeartbeat: node.LastHeartbeat, CPU: node.CPU, Memory: node.Memory, OS: node.OS, Arch: node.Arch, Labels: node.Labels,
		Volumes: node.Volumes, Capabilities: node.Capabilities, Version: node.Version,
	}
}

func (h *Handler) handleRestartJob(c *echo.Context) error {
	if err := requireWrite(c, "restarting jobs requires write authorization"); err != nil {
		return err
	}
	if err := h.server.RestartJob(c.Request().Context(), requestNamespace(c), c.Param("name")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListJobRevisions(c *echo.Context) error {
	revisions, err := h.server.ListJobRevisions(c.Request().Context(), requestNamespace(c), c.Param("name"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, revisions)
}

func (h *Handler) handleStopAllocation(c *echo.Context) error {
	if err := requireWrite(c, "stopping allocations requires write authorization"); err != nil {
		return err
	}
	if err := h.server.StopAllocationByID(c.Request().Context(), requestNamespace(c), c.Param("id")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExecAllocation(c *echo.Context) error {
	if err := requireWrite(c, "exec requires write authorization"); err != nil {
		return err
	}
	var request api.ExecRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if len(request.Command) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "command is required")
	}
	result, err := h.server.ExecAllocation(c.Request().Context(), requestNamespace(c), c.Param("id"), request.Task, request.Command)
	if errors.Is(err, ErrTaskSelection) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleCreateExecSession(c *echo.Context) error {
	if err := requireWrite(c, "interactive exec requires write authorization"); err != nil {
		return err
	}
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
	result, err := h.server.CreateExecSession(c.Request().Context(), requestNamespace(c), c.Param("id"), &request)
	if errors.Is(err, ErrTaskSelection) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusCreated, result)
}

func (h *Handler) handleExecSessionInput(c *echo.Context) error {
	if err := requireWrite(c, "interactive exec requires write authorization"); err != nil {
		return err
	}
	var request api.ExecSessionInputRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := h.server.WriteExecSession(c.Request().Context(), requestNamespace(c), c.Param("id"), c.Param("session"), &request); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExecSessionOutput(c *echo.Context) error {
	if err := requireWrite(c, "interactive exec requires write authorization"); err != nil {
		return err
	}
	offset, err := strconv.ParseInt(c.QueryParam("offset"), 10, 64)
	if c.QueryParam("offset") == "" {
		offset, err = 0, nil
	}
	if err != nil || offset < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "offset must be a non-negative integer")
	}
	result, err := h.server.ReadExecSession(c.Request().Context(), requestNamespace(c), c.Param("id"), c.Param("session"), offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleExecSessionResize(c *echo.Context) error {
	if err := requireWrite(c, "interactive exec requires write authorization"); err != nil {
		return err
	}
	var request api.ExecSessionResizeRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if request.Cols == 0 || request.Rows == 0 || request.Cols > 1000 || request.Rows > 1000 {
		return echo.NewHTTPError(http.StatusBadRequest, "terminal dimensions must be between 1 and 1000")
	}
	if err := h.server.ResizeExecSession(c.Request().Context(), requestNamespace(c), c.Param("id"), c.Param("session"), &request); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExecSessionClose(c *echo.Context) error {
	if err := requireWrite(c, "interactive exec requires write authorization"); err != nil {
		return err
	}
	if err := h.server.CloseExecSession(c.Request().Context(), requestNamespace(c), c.Param("id"), c.Param("session")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleAllocationMetrics(c *echo.Context) error {
	metrics, err := h.server.AllocationMetrics(c.Request().Context(), requestNamespace(c), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, metrics)
}

func (h *Handler) handleEvents(c *echo.Context) error {
	ch := h.server.events.subscribe(requestNamespace(c))
	defer h.server.events.unsubscribe(ch)

	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	c.Response().WriteHeader(http.StatusOK)

	ctx := c.Request().Context()
	rc := http.NewResponseController(c.Response())

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(c.Response(), "data: %s\n\n", data); err != nil {
				return err
			}
			_ = rc.Flush()
		}
	}
}
