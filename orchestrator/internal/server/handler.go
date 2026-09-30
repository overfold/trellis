package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/catalog"
	"github.com/overfold/trellis/internal/client"
	"github.com/overfold/trellis/internal/execstream"
	"github.com/overfold/trellis/internal/plan"
	secretstore "github.com/overfold/trellis/internal/secrets"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/tlsutil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler exposes a Server through HTTP routes.
type Handler struct {
	server *Server
}

const (
	maxHeartbeatBodyBytes          = 32 << 20
	maxHeartbeatAllocationStatuses = 320_000
)

type contextKey string

// NamespaceContextKey stores the authenticated namespace or encoded scoped authorization in a request context.
const NamespaceContextKey contextKey = "trellis-namespace"

// AdminContextKey stores cluster-administrator status in a request context.
const AdminContextKey contextKey = "trellis-admin"

// NodeContextKey stores the immutable node ID authenticated by mutual TLS.
const NodeContextKey contextKey = "trellis-node"

// JoinTokenContextKey stores the join token presented to the node enrollment
// endpoint. The middleware does not validate it; enrollment consumes it.
const JoinTokenContextKey contextKey = "trellis-join-token"

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

func requireRoot(c *echo.Context, message string) error {
	if !authorization(c).root {
		return echo.NewHTTPError(http.StatusForbidden, message)
	}
	return nil
}

func authenticatedNodeID(c *echo.Context, message string) (uuid.UUID, error) {
	id, ok := c.Request().Context().Value(NodeContextKey).(uuid.UUID)
	if !ok || id == uuid.Nil {
		return uuid.Nil, echo.NewHTTPError(http.StatusForbidden, message)
	}
	return id, nil
}

func requireExactNode(c *echo.Context, expected uuid.UUID, message string) error {
	id, err := authenticatedNodeID(c, message)
	if err != nil || expected == uuid.Nil || id != expected {
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
	v1.GET("/credentials", h.handleListCredentials)
	v1.DELETE("/credentials/:id", h.handleRevokeCredential)
	v1.GET("/nodes", h.handleListNodes)
	v1.POST("/nodes/enroll", h.handleEnrollNode)
	v1.POST("/nodes/join-tokens", h.handleCreateJoinToken)
	v1.GET("/nodes/join-tokens", h.handleListJoinTokens)
	v1.DELETE("/nodes/join-tokens/:id", h.handleRevokeJoinToken)
	v1.POST("/nodes", h.handleRegisterNode)
	v1.POST("/nodes/:id/heartbeat", h.handleHeartbeat)
	v1.POST("/nodes/:id/drain", h.handleDrainNode)
	v1.DELETE("/nodes/:id/drain", h.handleUndrainNode)
	v1.GET("/namespaces", h.handleListNamespaces)
	v1.GET("/allocations", h.handleListClusterAllocations)
	v1.GET("/events", h.handleClusterEvents)

	// Every namespaced resource names its namespace in the path.
	ns := v1.Group("/namespaces/:namespace")
	ns.GET("/jobs", h.handleListJobs)
	ns.POST("/jobs", h.handleRegisterJob)
	ns.POST("/jobs/plan", h.handlePlanJob)
	ns.GET("/jobs/:name", h.handleGetJob)
	ns.DELETE("/jobs/:name", h.handleDeleteJob)
	ns.POST("/jobs/:name/restart", h.handleRestartJob)
	ns.POST("/jobs/:name/groups/:group/replacement-backoff/reset", h.handleResetReplacementBackoff)
	ns.GET("/jobs/:name/versions", h.handleListJobVersions)
	ns.GET("/allocations", h.handleListAllocations)
	ns.DELETE("/allocations/:id", h.handleStopAllocation)
	ns.GET("/allocations/:id/events", h.handleAllocationEvents)
	ns.GET("/allocations/:id/logs", h.handleAllocationLogs)
	ns.GET("/allocations/:id/exec", h.handleExec)
	ns.GET("/allocations/:id/metrics", h.handleAllocationMetrics)
	ns.GET("/events", h.handleEvents)
	ns.PUT("/secrets/:name", h.handleSetSecret)
	ns.GET("/secrets", h.handleListSecrets)
	ns.GET("/secrets/:name", h.handleGetSecret)
	ns.DELETE("/secrets/:name", h.handleDeleteSecret)
	v1.GET("/internal/discovery", h.handleListDiscovery)
	v1.POST("/raft/join", h.handleRaftJoin)
	v1.DELETE("/raft/members/:id", h.handleRaftMemberRemove)
	v1.POST("/raft/leadership-transfer", h.handleRaftLeadershipTransfer)
	v1.GET("/cluster/settings", h.handleGetClusterSettings)
	v1.PUT("/cluster/settings/job-limits", h.handleUpdateJobLimits)
	v1.PUT("/cluster/settings/reconciliation", h.handleUpdateReconciliationSettings)
	v1.GET("/backup", h.handleBackupCreate)
	v1.POST("/backup/restore", h.handleBackupRestore)
}

func (h *Handler) handleCreateCredential(c *echo.Context) error {
	if err := requireRoot(c, "credential creation requires the administrator credential"); err != nil {
		return err
	}
	var request api.CredentialCreateRequest
	if err := decodeJSON(c, &request, maxSmallRequestBytes); err != nil {
		return err
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
	if request.TTLSeconds < 0 || request.TTLSeconds > maxTTLSeconds {
		return echo.NewHTTPError(http.StatusBadRequest, "ttl_seconds must be between 0 and "+strconv.FormatInt(maxTTLSeconds, 10))
	}
	token, credential, err := h.server.CreateCredential(c.Request().Context(), scope, access, request.Namespace, time.Duration(request.TTLSeconds)*time.Second)
	if errors.Is(err, ErrInvalidCredentialRequest) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusCreated, api.CredentialCreateResponse{Token: token, CredentialResponse: credentialResponse(credential)})
}

// maxTTLSeconds bounds requested lifetimes so their durations cannot overflow.
const maxTTLSeconds = int64(100 * 365 * 24 * 60 * 60)

func credentialResponse(credential auth.OperatorCredential) api.CredentialResponse {
	response := api.CredentialResponse{
		ID:        credential.ID,
		Scope:     string(credential.Principal.Scope),
		Access:    string(credential.Principal.Access),
		Namespace: credential.Principal.Namespace,
		CreatedAt: credential.Principal.CreatedAt,
	}
	if !credential.Principal.ExpiresAt.IsZero() {
		expiresAt := credential.Principal.ExpiresAt
		response.ExpiresAt = &expiresAt
	}
	return response
}

func (h *Handler) handleListCredentials(c *echo.Context) error {
	if err := requireRoot(c, "listing credentials requires the administrator credential"); err != nil {
		return err
	}
	credentials, err := h.server.ListCredentials(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to list credentials")
	}
	response := make(api.CredentialListResponse, 0, len(credentials))
	for _, credential := range credentials {
		response = append(response, credentialResponse(credential))
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}

func (h *Handler) handleRevokeCredential(c *echo.Context) error {
	if err := requireRoot(c, "revoking credentials requires the administrator credential"); err != nil {
		return err
	}
	err := h.server.RevokeCredential(c.Request().Context(), c.Param("id"))
	if errors.Is(err, auth.ErrCredentialNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "credential not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to revoke credential")
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateJoinToken(c *echo.Context) error {
	if err := requireRoot(c, "creating join tokens requires the administrator credential"); err != nil {
		return err
	}
	var request api.JoinTokenCreateRequest
	if err := decodeJSON(c, &request, maxSmallRequestBytes); err != nil {
		return err
	}
	if request.TTLSeconds < 0 || request.TTLSeconds > maxTTLSeconds {
		return echo.NewHTTPError(http.StatusBadRequest, "ttl_seconds is out of range")
	}
	token, record, err := h.server.CreateJoinToken(c.Request().Context(), time.Duration(request.TTLSeconds)*time.Second, request.MaxUses)
	if errors.Is(err, ErrInvalidJoinTokenRequest) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to create join token")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusCreated, api.JoinTokenCreateResponse{Token: token, JoinTokenResponse: record.API()})
}

func (h *Handler) handleListJoinTokens(c *echo.Context) error {
	if err := requireRoot(c, "listing join tokens requires the administrator credential"); err != nil {
		return err
	}
	tokens, err := h.server.ListJoinTokens(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to list join tokens")
	}
	response := make(api.JoinTokenListResponse, 0, len(tokens))
	for _, token := range tokens {
		response = append(response, token.API())
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}

func (h *Handler) handleRevokeJoinToken(c *echo.Context) error {
	if err := requireRoot(c, "revoking join tokens requires the administrator credential"); err != nil {
		return err
	}
	err := h.server.RevokeJoinToken(c.Request().Context(), c.Param("id"))
	if errors.Is(err, ErrJoinTokenNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "join token not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to revoke join token")
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetClusterSettings(c *echo.Context) error {
	if err := requireClusterRead(c, "cluster settings require cluster/read authorization"); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, h.server.ClusterSettings().API())
}

func (h *Handler) handleUpdateJobLimits(c *echo.Context) error {
	if err := requireRoot(c, "changing cluster settings requires the administrator credential"); err != nil {
		return err
	}
	var limits spec.Limits
	if err := decodeJSON(c, &limits, maxSmallRequestBytes); err != nil {
		return err
	}
	settings, err := h.server.UpdateJobLimits(c.Request().Context(), limits)
	return clusterSettingsResponse(c, settings, err)
}

func (h *Handler) handleUpdateReconciliationSettings(c *echo.Context) error {
	if err := requireRoot(c, "changing cluster settings requires the administrator credential"); err != nil {
		return err
	}
	var reconciliation api.ReconciliationSettings
	if err := decodeJSON(c, &reconciliation, maxSmallRequestBytes); err != nil {
		return err
	}
	settings, err := h.server.UpdateReconciliationSettings(c.Request().Context(), ReconciliationSettingsFromAPI(reconciliation))
	return clusterSettingsResponse(c, settings, err)
}

func clusterSettingsResponse(c *echo.Context, settings ClusterSettings, err error) error {
	switch {
	case errors.Is(err, ErrInvalidClusterSettings):
		return echo.NewHTTPError(http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrClusterSettingsConflict):
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	case err != nil:
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	return c.JSON(http.StatusOK, settings.API())
}

func (h *Handler) handleBackupCreate(c *echo.Context) error {
	if err := requireRoot(c, "backup operations require the administrator credential"); err != nil {
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
	if err := requireRoot(c, "backup operations require the administrator credential"); err != nil {
		return err
	}
	var backup api.BackupSnapshot
	if err := decodeJSON(c, &backup, maxBackupRequestBytes); err != nil {
		return err
	}
	if err := h.server.Restore(c.Request().Context(), &backup); err != nil {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

// Secrets are namespace-scoped and write-only: no endpoint returns a secret
// value, whatever the caller's scope. Values reach tasks only through
// leader-to-agent delivery.

func (h *Handler) handleSetSecret(c *echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	ns, err := namespaceWrite(c, "setting secrets requires write authorization")
	if err != nil {
		return err
	}
	if !spec.ValidIdentifier(c.Param("name")) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid secret name")
	}
	var request api.SecretWriteRequest
	if err := decodeJSON(c, &request, maxSecretRequestBytes); err != nil {
		return err
	}
	decodedSize := base64.StdEncoding.DecodedLen(len(request.ValueBase64))
	if strings.HasSuffix(request.ValueBase64, "=") {
		decodedSize--
	}
	if strings.HasSuffix(request.ValueBase64, "==") {
		decodedSize--
	}
	if len(request.ValueBase64) > base64.StdEncoding.EncodedLen(secretstore.MaxValueSize) || decodedSize > secretstore.MaxValueSize {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "secret exceeds 65536 bytes")
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
	c.Response().Header().Set("Cache-Control", "no-store")
	ns, err := namespaceParam(c)
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
	c.Response().Header().Set("Cache-Control", "no-store")
	ns, err := namespaceParam(c)
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
	c.Response().Header().Set("Cache-Control", "no-store")
	ns, err := namespaceWrite(c, "deleting secrets requires write authorization")
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
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	events, ok := h.server.AllocationEvents(ns, c.Param("id"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "allocation not found")
	}
	if events == nil {
		events = api.AllocationEventListResponse{}
	}
	return c.JSON(http.StatusOK, events)
}

func (h *Handler) handleAllocationLogs(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	tail, err := strconv.Atoi(c.QueryParam("tail"))
	if c.QueryParam("tail") == "" {
		tail, err = 100, nil
	}
	if err != nil || tail < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "tail must be a non-negative integer")
	}
	logs, err := h.server.AllocationTaskLogsForNamespace(c.Request().Context(), ns, c.Param("id"), c.QueryParam("task"), c.QueryParam("follow") == "true", tail)
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
		if errors.Is(err, ErrNodeNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "node not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListNodes(c *echo.Context) error {
	if err := requireClusterRead(c, "nodes are cluster-scoped"); err != nil {
		return err
	}
	nodes := h.server.ListNodes()
	// Membership only annotates the listing; node commands must keep working
	// while the Raft configuration is briefly unreadable.
	voters, _ := h.server.MemberVoters()
	result := make(api.NodeListResponse, 0, len(nodes))
	for _, node := range nodes {
		response := h.convertNode(&node)
		if voter, member := voters[node.ID.String()]; member {
			response.ControlPlane = api.ControlPlaneNonvoter
			if voter {
				response.ControlPlane = api.ControlPlaneVoter
			}
		}
		result = append(result, *response)
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleRegisterNode(c *echo.Context) error {
	var request api.NodeRegistrationRequest
	if err := decodeJSON(c, &request, maxDefaultRequestBytes); err != nil {
		return err
	}
	if err := requireExactNode(c, request.ID, "node registration identity does not match certificate"); err != nil {
		return err
	}
	cpuCapacity, memoryCapacity := request.CPUCapacity, request.MemoryCapacity
	if cpuCapacity == 0 {
		cpuCapacity = request.CPU
	}
	if memoryCapacity == 0 {
		memoryCapacity = request.Memory
	}
	cpuAllocatable, memoryAllocatable := request.CPUAllocatable, request.MemoryAllocatable
	if cpuAllocatable == 0 {
		cpuAllocatable = request.CPU
	}
	if memoryAllocatable == 0 {
		memoryAllocatable = request.Memory
	}
	if err := h.server.RegisterNode(c.Request().Context(), &NodeRegistration{
		ID: request.ID, Host: request.Host, Port: request.Port,
		CPUCapacity: cpuCapacity, MemoryCapacity: memoryCapacity,
		CPUAllocatable: cpuAllocatable, MemoryAllocatable: memoryAllocatable,
		OS: request.OS, Arch: request.Arch, Labels: request.Labels, Volumes: request.Volumes, Capabilities: request.Capabilities,
		WireGuardPublicKey: request.WireGuardPublicKey, WireGuardEndpoint: request.WireGuardEndpoint,
		WireGuardPortBase: request.WireGuardPortBase, WireGuardPortCount: request.WireGuardPortCount,
	}); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "unable to register node")
	}
	return c.JSON(http.StatusCreated, api.NodeRegistrationResponse{ID: request.ID})
}

func (h *Handler) handleHeartbeat(c *echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := requireExactNode(c, id, "heartbeat identity does not match certificate"); err != nil {
		return err
	}
	var request api.HeartbeatRequest
	if err := decodeJSON(c, &request, maxHeartbeatBodyBytes); err != nil {
		return err
	}
	if len(request.Allocations) > maxHeartbeatAllocationStatuses {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "heartbeat contains too many allocation status reports")
	}
	resources := nodeResourceObservation{
		CPUCapacity: request.CPUCapacity, MemoryCapacity: request.MemoryCapacity,
		CPUAllocatable: request.CPUAllocatable, MemoryAllocatable: request.MemoryAllocatable,
		CPUUsage: request.CPUUsage, MemoryUsed: request.MemoryUsed,
		MemoryAvailable: request.MemoryAvailable, MetricsAt: request.MetricsAt,
	}
	if err := h.server.Heartbeat(c.Request().Context(), id, request.Allocations, request.Version, request.Volumes, request.Capabilities, resources); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "unable to process heartbeat")
	}
	h.server.RecordRaftProgress(id, request.RaftAppliedIndex)
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, h.server.ListJobs(ns))
}

func (h *Handler) handleGetJob(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	status, ok := h.server.GetJob(ns, c.Param("name"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "job not found")
	}
	return c.JSON(http.StatusOK, status)
}

func (h *Handler) handleDeleteJob(c *echo.Context) error {
	ns, err := namespaceWrite(c, "deleting jobs requires write authorization")
	if err != nil {
		return err
	}
	if err := h.server.DeleteJob(c.Request().Context(), ns, c.Param("name")); err != nil {
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

// decodeJobRequest decodes and canonicalizes a job submission addressed to
// the {namespace} path parameter, whose spec must name the same namespace. A
// nil request with a nil error means the validation response was written.
func (h *Handler) decodeJobRequest(c *echo.Context, ns string) (*api.JobRegistrationRequest, error) {
	var request api.JobRegistrationRequest
	if err := decodeJSON(c, &request, maxJobRequestBytes); err != nil {
		return nil, err
	}
	if err := h.server.CanonicalizeJob(&request.Spec); err != nil {
		return nil, validationResponse(c, err)
	}
	if request.Spec.Namespace != ns {
		return nil, echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("spec.namespace %q does not match request namespace %q", request.Spec.Namespace, ns))
	}
	return &request, nil
}

func (h *Handler) handlePlanJob(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	request, err := h.decodeJobRequest(c, ns)
	if err != nil || request == nil {
		return err
	}
	if err := h.server.ValidateNamespaceAllocationLimit(request.Spec.Namespace, &request.Spec); err != nil {
		return validationResponse(c, err)
	}
	if err := requireAPIAccessDelegation(c, &request.Spec); err != nil {
		return err
	}
	var currentSpec *spec.JobSpec
	var version, revision int
	if current, ok := h.server.GetJob(request.Spec.Namespace, request.Spec.Name); ok {
		currentSpec, version, revision = current.Spec, current.Version, current.Revision
	}
	return c.JSON(http.StatusOK, plan.Build(currentSpec, version, revision, &request.Spec))
}

func (h *Handler) handleRegisterJob(c *echo.Context) error {
	ns, err := namespaceWrite(c, "applying jobs requires write authorization")
	if err != nil {
		return err
	}
	request, err := h.decodeJobRequest(c, ns)
	if err != nil || request == nil {
		return err
	}
	if err := requireAPIAccessDelegation(c, &request.Spec); err != nil {
		return err
	}
	result, err := h.server.RegisterJob(c.Request().Context(), ns, &request.Spec, request.ExpectedVersion)
	if errors.Is(err, ErrJobVersionConflict) {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}
	if err != nil {
		return validationResponse(c, err)
	}
	return c.JSON(http.StatusAccepted, result)
}

func (h *Handler) handleListAllocations(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	return h.listAllocations(c, ns)
}

// handleListClusterAllocations lists allocations across every namespace and
// therefore requires cluster scope.
func (h *Handler) handleListClusterAllocations(c *echo.Context) error {
	if err := requireClusterRead(c, "listing allocations across namespaces requires cluster scope"); err != nil {
		return err
	}
	return h.listAllocations(c, "")
}

func (h *Handler) listAllocations(c *echo.Context, ns string) error {
	var filter *AllocationListFilter
	job, label := c.QueryParam("job"), c.QueryParam("label")
	if job != "" || label != "" {
		filter = &AllocationListFilter{Job: job, Label: label}
	}
	allocations := h.server.ListAllocations(ns, filter)
	if allocations == nil {
		allocations = api.AllocationListResponse{}
	}
	return c.JSON(http.StatusOK, allocations)
}

func (h *Handler) handleListDiscovery(c *echo.Context) error {
	nodeID, err := authenticatedNodeID(c, "internal discovery requires an authenticated node")
	if err != nil {
		return err
	}
	var filter *catalog.ListFilter
	job, label := c.QueryParam("job"), c.QueryParam("label")
	if job != "" || label != "" {
		filter = &catalog.ListFilter{Job: job, Label: label}
	}
	entries := h.server.ListServicesForNode(nodeID, filter)
	if entries == nil {
		entries = api.ServiceListResponse{}
	}
	return c.JSON(http.StatusOK, entries)
}

func (h *Handler) handleRaftJoin(c *echo.Context) error {
	var request api.RaftJoinRequest
	if err := decodeJSON(c, &request, maxSmallRequestBytes); err != nil {
		return err
	}
	if request.RaftAddress == "" || request.ServerAddress == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "raft_address and server_address are required")
	}
	nodeID, ok := c.Request().Context().Value(NodeContextKey).(uuid.UUID)
	if !ok || nodeID == uuid.Nil {
		return echo.NewHTTPError(http.StatusForbidden, "Raft join requires an authenticated node")
	}
	if c.Request().TLS == nil || len(c.Request().TLS.PeerCertificates) == 0 {
		return echo.NewHTTPError(http.StatusForbidden, "Raft join requires an authenticated node certificate")
	}
	certificate := c.Request().TLS.PeerCertificates[0]
	certificateNodeID, err := tlsutil.NodeID(certificate)
	if err != nil || certificateNodeID != nodeID {
		return echo.NewHTTPError(http.StatusForbidden, "Raft join identity does not match certificate")
	}
	nodeID = certificateNodeID
	// In managed signing mode every member holds the CA key and could mint a
	// certificate for a new UUID. Only identities bound by join-token
	// enrollment (or the bootstrap node) may join; external mode lets the
	// operator's CA decide and binds on first join.
	if _, caKey, err := h.server.ClusterCA(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "load managed signing material")
	} else if caKey != "" && !h.server.AuthorizeNodeCertificate(c.Request().Context(), nodeID, certificate) {
		return echo.NewHTTPError(http.StatusForbidden, "Raft join requires an identity enrolled with a join token")
	}
	if err := h.server.BindNodeCertificate(c.Request().Context(), nodeID, certificate); err != nil {
		return echo.NewHTTPError(http.StatusForbidden, err.Error())
	}
	for _, address := range []string{request.RaftAddress, request.ServerAddress} {
		host, _, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "advertised addresses must be host:port")
		}
		if err := certificate.VerifyHostname(host); err != nil {
			return echo.NewHTTPError(http.StatusForbidden, "advertised address is not bound to the authenticated node certificate")
		}
	}
	if h.server.joiner == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "cluster join not available")
	}
	if err := h.server.RecordNodeServerAddress(c.Request().Context(), nodeID, request.ServerAddress); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	members, err := h.server.JoinMember(c.Request().Context(), nodeID, request.RaftAddress)
	if errors.Is(err, ErrNodeRemoved) {
		return echo.NewHTTPError(http.StatusForbidden, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	_, caKey, err := h.server.ClusterCA()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "load managed signing material")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, api.RaftJoinResponse{CAKey: caKey, Members: members})
}

func (h *Handler) handleEnrollNode(c *echo.Context) error {
	joinToken, _ := c.Request().Context().Value(JoinTokenContextKey).(string)
	if joinToken == "" {
		return echo.NewHTTPError(http.StatusUnauthorized, "node enrollment requires a join token")
	}
	var request api.NodeEnrollmentRequest
	if err := decodeJSON(c, &request, maxSmallRequestBytes); err != nil {
		return err
	}
	response, err := h.server.EnrollNode(c.Request().Context(), joinToken, request.ServerAdvertise, request.AgentAdvertise, request.RaftAdvertise)
	if errors.Is(err, ErrInvalidJoinToken) {
		return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusCreated, response)
}

func (h *Handler) handleRaftMemberRemove(c *echo.Context) error {
	if err := requireRoot(c, "Raft membership changes require the administrator credential"); err != nil {
		return err
	}
	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id is required")
	}
	if h.server.joiner == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "cluster membership changes not available")
	}
	if err := h.server.RemoveMember(c.Request().Context(), id); err != nil {
		if errors.Is(err, ErrInvalidNodeID) {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		if errors.Is(err, ErrMembershipUnsafe) {
			return echo.NewHTTPError(http.StatusConflict, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleRaftLeadershipTransfer(c *echo.Context) error {
	if err := requireRoot(c, "leadership transfer requires the administrator credential"); err != nil {
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

func (h *Handler) convertNode(node *NodeView) *api.NodeResponse {
	var lastHeartbeat *time.Time
	if !node.LastHeartbeat.IsZero() {
		heartbeat := node.LastHeartbeat
		lastHeartbeat = &heartbeat
	}
	return &api.NodeResponse{
		ID: node.ID, Host: node.Host, Port: node.Port, Status: api.NodeStatusResponse(node.Status),
		LastHeartbeat: lastHeartbeat, CPU: node.CPUAllocatable, Memory: node.MemoryAllocatable,
		CPUCapacity: node.CPUCapacity, MemoryCapacity: node.MemoryCapacity,
		CPUAllocatable: node.CPUAllocatable, MemoryAllocatable: node.MemoryAllocatable,
		CPUUsage: node.CPUUsage, MemoryUsed: node.MemoryUsed, MemoryAvailable: node.MemoryAvailable, MetricsAt: node.MetricsAt,
		OS: node.OS, Arch: node.Arch, Labels: node.Labels,
		Volumes: node.Volumes, Capabilities: node.Capabilities, Version: node.Version,
	}
}

func (h *Handler) handleRestartJob(c *echo.Context) error {
	ns, err := namespaceWrite(c, "restarting jobs requires write authorization")
	if err != nil {
		return err
	}
	if err := h.server.RestartJob(c.Request().Context(), ns, c.Param("name")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleResetReplacementBackoff(c *echo.Context) error {
	ns, err := namespaceWrite(c, "resetting replacement backoff requires write authorization")
	if err != nil {
		return err
	}
	name, group := c.Param("name"), c.Param("group")
	if !spec.ValidIdentifier(name) || !spec.ValidIdentifier(group) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid job or task group name")
	}
	if err := h.server.ResetReplacementBackoff(c.Request().Context(), ns, name, group); err != nil {
		if errors.Is(err, ErrTaskGroupNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "task group not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListJobVersions(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	versions, err := h.server.ListJobVersions(c.Request().Context(), ns, c.Param("name"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, versions)
}

func (h *Handler) handleStopAllocation(c *echo.Context) error {
	ns, err := namespaceWrite(c, "stopping allocations requires write authorization")
	if err != nil {
		return err
	}
	if err := h.server.StopAllocationByID(c.Request().Context(), ns, c.Param("id")); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

// handleExec opens an exec stream. Request, authorization, lookup, and
// admission errors are ordinary HTTP responses; once the connection is
// upgraded the leader relays frames between the client and the agent.
func (h *Handler) handleExec(c *echo.Context) error {
	ns, err := namespaceWrite(c, "exec requires write authorization")
	if err != nil {
		return err
	}
	if !execstream.IsUpgradeRequest(c.Request()) {
		return echo.NewHTTPError(http.StatusBadRequest, "exec requires an HTTP/1.1 upgrade to "+execstream.Protocol)
	}
	request, err := execstream.DecodeRequest(c.QueryParams())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	stream, err := h.server.OpenExec(c.Request().Context(), ns, c.Param("id"), request)
	if err != nil {
		switch {
		case errors.Is(err, errExecRelayLimit):
			return echo.NewHTTPError(http.StatusTooManyRequests, err.Error())
		case errors.Is(err, errExecNoTerm):
			return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
		}
		return h.agentRequestError(err, noRunningTaskMessage(c.Param("id"), request.Task))
	}
	defer stream.Close()
	conn, err := execstream.Accept(c.Response())
	if err != nil {
		h.server.log.Warn("accept exec stream", "allocation", c.Param("id"), "error", err)
		return nil
	}
	stream.Relay(conn)
	return nil
}

func (h *Handler) handleAllocationMetrics(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	metrics, err := h.server.AllocationMetrics(c.Request().Context(), ns, c.Param("id"))
	if err != nil {
		return h.agentRequestError(err, fmt.Sprintf("allocation %s is not running on its node", c.Param("id")))
	}
	return c.JSON(http.StatusOK, metrics)
}

// agentRequestError maps a failed exec or allocation metrics request to its
// public status. Control-plane lookup failures keep their meaning, and the
// agent's rejections of the request itself pass through. When notRunning is
// set, an agent 404 means the control plane knows the allocation but its
// node has no running target, so it becomes a 409 with that message;
// otherwise an agent 404 passes through. Transport failures and other agent failures are logged and
// reported without their details.
func (h *Handler) agentRequestError(err error, notRunning string) error {
	var agentErr *client.HTTPError
	switch {
	case errors.Is(err, ErrAllocationNotFound):
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	case errors.Is(err, ErrTaskSelection):
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	case errors.As(err, &agentErr):
		switch agentErr.Status {
		case http.StatusNotFound:
			if notRunning != "" {
				return echo.NewHTTPError(http.StatusConflict, notRunning)
			}
			return echo.NewHTTPError(http.StatusNotFound, agentErr.Message())
		case http.StatusBadRequest, http.StatusConflict, http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
			return echo.NewHTTPError(agentErr.Status, agentErr.Message())
		case http.StatusServiceUnavailable:
			return echo.NewHTTPError(http.StatusServiceUnavailable, "node agent unavailable: "+agentErr.Message())
		}
		h.server.log.Warn("agent request failed", "error", err)
		return echo.NewHTTPError(http.StatusBadGateway, "node agent failed to handle the request")
	default:
		h.server.log.Warn("agent request failed", "error", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "node agent unavailable")
	}
}

func noRunningTaskMessage(id, task string) string {
	if task == "" {
		return fmt.Sprintf("allocation %s has no running task", id)
	}
	return fmt.Sprintf("allocation %s task %q is not running", id, task)
}

func (h *Handler) handleEvents(c *echo.Context) error {
	ns, err := namespaceParam(c)
	if err != nil {
		return err
	}
	return h.streamEvents(c, ns)
}

// handleClusterEvents streams events from every namespace and therefore
// requires cluster scope.
func (h *Handler) handleClusterEvents(c *echo.Context) error {
	if err := requireClusterRead(c, "streaming events across namespaces requires cluster scope"); err != nil {
		return err
	}
	return h.streamEvents(c, "")
}

func (h *Handler) streamEvents(c *echo.Context, ns string) error {
	ch, ok := h.server.events.subscribe(ns)
	if !ok {
		c.Response().Header().Set("Retry-After", "1")
		return echo.NewHTTPError(http.StatusServiceUnavailable, "event subscriber limit reached")
	}
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
