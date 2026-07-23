package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type createRuntimePoolRequest struct {
	Name                 string `json:"name"`
	MaxInflight          int32  `json:"max_inflight"`
	AffinityGraceSeconds *int32 `json:"affinity_grace_seconds,omitempty"`
	LeaseSeconds         *int32 `json:"lease_seconds,omitempty"`
}

func (h *Handler) CreateRuntimePool(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	createdBy, err := util.ParseUUID(requestUserID(r))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "member identity is required")
		return
	}
	var req createRuntimePoolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid runtime pool payload")
		return
	}
	affinity := int32(service.DefaultWorkflowAffinityGraceSeconds)
	if req.AffinityGraceSeconds != nil {
		affinity = *req.AffinityGraceSeconds
	}
	lease := int32(service.DefaultWorkflowLeaseSeconds)
	if req.LeaseSeconds != nil {
		lease = *req.LeaseSeconds
	}
	pool, err := h.WorkflowService.CreateRuntimePool(
		r.Context(),
		workspaceID,
		createdBy,
		req.Name,
		req.MaxInflight,
		affinity,
		lease,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, pool)
}

func (h *Handler) ListRuntimePools(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	pools, err := h.Queries.ListRuntimePools(r.Context(), workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list runtime pools")
		return
	}
	type poolWithMembers struct {
		db.RuntimePool
		Runtimes []db.RuntimePoolRuntime `json:"runtimes"`
	}
	out := make([]poolWithMembers, 0, len(pools))
	for _, pool := range pools {
		members, err := h.Queries.ListRuntimePoolRuntimes(r.Context(), db.ListRuntimePoolRuntimesParams{
			PoolID:      pool.ID,
			WorkspaceID: workspaceID,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list runtime pool members")
			return
		}
		out = append(out, poolWithMembers{RuntimePool: pool, Runtimes: members})
	}
	writeJSON(w, http.StatusOK, out)
}

type addRuntimePoolMemberRequest struct {
	RuntimeID string `json:"runtime_id"`
	Priority  int32  `json:"priority"`
}

func (h *Handler) AddRuntimePoolMember(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	poolID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "poolId"), "pool_id")
	if !ok {
		return
	}
	var req addRuntimePoolMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid runtime pool member payload")
		return
	}
	runtimeID, err := util.ParseUUID(req.RuntimeID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "runtime_id must be a UUID")
		return
	}
	member, err := h.WorkflowService.AddRuntimeToPool(r.Context(), workspaceID, poolID, runtimeID, req.Priority)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, member)
}

type bindAgentPoolRequest struct {
	AgentID string `json:"agent_id"`
}

func (h *Handler) BindAgentRuntimePool(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	poolID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "poolId"), "pool_id")
	if !ok {
		return
	}
	var req bindAgentPoolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid agent runtime pool payload")
		return
	}
	agentID, err := util.ParseUUID(req.AgentID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "agent_id must be a UUID")
		return
	}
	binding, err := h.WorkflowService.BindAgentToPool(r.Context(), workspaceID, agentID, poolID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, binding)
}

type workflowNodeRequest struct {
	Key               string                         `json:"key"`
	IssueID           string                         `json:"issue_id"`
	PassageKey        string                         `json:"passage_key"`
	NodeKey           string                         `json:"node_key"`
	ExecutorKind      string                         `json:"executor_kind"`
	AgentID           string                         `json:"agent_id,omitempty"`
	RuntimePoolID     string                         `json:"runtime_pool_id,omitempty"`
	Priority          int32                          `json:"priority"`
	PreferredDaemonID string                         `json:"preferred_daemon_id,omitempty"`
	InputDigest       string                         `json:"input_digest,omitempty"`
	LawDigest         string                         `json:"law_digest,omitempty"`
	OutputContract    json.RawMessage                `json:"output_contract"`
	MaxAttempts       int32                          `json:"max_attempts,omitempty"`
	DependsOn         []string                       `json:"depends_on,omitempty"`
	Resources         []service.WorkflowResourceSpec `json:"resources,omitempty"`
}

type createWorkflowRunRequest struct {
	ProjectID           string                                   `json:"project_id"`
	AnchorIssueID       string                                   `json:"anchor_issue_id"`
	GraphKey            string                                   `json:"graph_key"`
	GraphVersion        string                                   `json:"graph_version"`
	IntegrationPoolID   string                                   `json:"integration_pool_id,omitempty"`
	WIPLimit            int32                                    `json:"wip_limit"`
	HumanGateLimit      int32                                    `json:"human_gate_limit"`
	InputDigest         string                                   `json:"input_digest,omitempty"`
	LawDigest           string                                   `json:"law_digest,omitempty"`
	Metadata            json.RawMessage                          `json:"metadata"`
	Nodes               []workflowNodeRequest                    `json:"nodes"`
	ImportedCheckpoints []service.WorkflowImportedCheckpointSpec `json:"imported_checkpoints,omitempty"`
}

func optionalUUID(value string) (pgtype.UUID, error) {
	if strings.TrimSpace(value) == "" {
		return pgtype.UUID{}, nil
	}
	return util.ParseUUID(value)
}

func (h *Handler) CreateWorkflowRun(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	createdBy, err := util.ParseUUID(requestUserID(r))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "member identity is required")
		return
	}
	var req createWorkflowRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow run payload")
		return
	}
	projectID, err := util.ParseUUID(req.ProjectID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "project_id must be a UUID")
		return
	}
	anchorID, err := util.ParseUUID(req.AnchorIssueID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "anchor_issue_id must be a UUID")
		return
	}
	integrationPoolID, err := optionalUUID(req.IntegrationPoolID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "integration_pool_id must be a UUID")
		return
	}
	nodes := make([]service.WorkflowNodeSpec, 0, len(req.Nodes))
	for _, raw := range req.Nodes {
		issueID, err := util.ParseUUID(raw.IssueID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "every node issue_id must be a UUID")
			return
		}
		agentID, err := optionalUUID(raw.AgentID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "node agent_id must be a UUID")
			return
		}
		poolID, err := optionalUUID(raw.RuntimePoolID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "node runtime_pool_id must be a UUID")
			return
		}
		nodes = append(nodes, service.WorkflowNodeSpec{
			Key:               raw.Key,
			IssueID:           issueID,
			PassageKey:        raw.PassageKey,
			NodeKey:           raw.NodeKey,
			ExecutorKind:      raw.ExecutorKind,
			AgentID:           agentID,
			RuntimePoolID:     poolID,
			Priority:          raw.Priority,
			PreferredDaemonID: raw.PreferredDaemonID,
			InputDigest:       raw.InputDigest,
			LawDigest:         raw.LawDigest,
			OutputContract:    raw.OutputContract,
			MaxAttempts:       raw.MaxAttempts,
			DependsOn:         raw.DependsOn,
			Resources:         raw.Resources,
		})
	}
	run, err := h.WorkflowService.CreateWorkflowRun(r.Context(), service.WorkflowGraphSpec{
		WorkspaceID:         workspaceID,
		ProjectID:           projectID,
		AnchorIssueID:       anchorID,
		GraphKey:            req.GraphKey,
		GraphVersion:        req.GraphVersion,
		IntegrationPoolID:   integrationPoolID,
		WIPLimit:            req.WIPLimit,
		HumanGateLimit:      req.HumanGateLimit,
		InputDigest:         req.InputDigest,
		LawDigest:           req.LawDigest,
		Metadata:            req.Metadata,
		CreatedBy:           createdBy,
		Nodes:               nodes,
		ImportedCheckpoints: req.ImportedCheckpoints,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (h *Handler) ListWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runs, err := h.Queries.ListWorkflowRuns(r.Context(), workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workflow runs")
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) GetWorkflowRun(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	run, err := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{
		ID:          runID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "workflow run not found")
		return
	}
	nodes, err := h.Queries.ListWorkflowNodes(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workflow nodes")
		return
	}
	type inspectedNode struct {
		db.WorkflowNode
		Dependencies []db.WorkflowNodeDependency `json:"dependencies"`
		Resources    []db.WorkflowNodeResource   `json:"resources"`
		Attempts     []db.WorkflowNodeAttempt    `json:"attempts"`
		Result       *db.WorkflowNodeResult      `json:"result,omitempty"`
	}
	inspected := make([]inspectedNode, 0, len(nodes))
	for _, node := range nodes {
		dependencies, _ := h.Queries.ListWorkflowNodeDependencies(r.Context(), node.ID)
		resources, _ := h.Queries.ListWorkflowNodeResources(r.Context(), node.ID)
		attempts, _ := h.Queries.ListWorkflowNodeAttempts(r.Context(), node.ID)
		var result *db.WorkflowNodeResult
		if accepted, err := h.Queries.GetWorkflowNodeResult(r.Context(), node.ID); err == nil {
			result = &accepted
		}
		inspected = append(inspected, inspectedNode{
			WorkflowNode: node,
			Dependencies: dependencies,
			Resources:    resources,
			Attempts:     attempts,
			Result:       result,
		})
	}
	metrics, err := h.Queries.GetWorkflowRunMetrics(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load workflow metrics")
		return
	}
	usageByModel, err := h.Queries.ListWorkflowRunUsageByModel(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load workflow usage")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run":            run,
		"nodes":          inspected,
		"metrics":        metrics,
		"usage_by_model": usageByModel,
	})
}

type workflowGateCompleteRequest struct {
	CommentID string `json:"comment_id"`
	Verdict   string `json:"verdict"`
}

func (h *Handler) CompleteWorkflowGate(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	if _, err := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{
		ID: runID, WorkspaceID: workspaceID,
	}); err != nil {
		writeError(w, http.StatusNotFound, "workflow run not found")
		return
	}
	nodeID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "nodeId"), "node_id")
	if !ok {
		return
	}
	var req workflowGateCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow gate payload")
		return
	}
	commentID, err := util.ParseUUID(req.CommentID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "comment_id must be a UUID")
		return
	}
	node, err := h.WorkflowService.CompleteHumanGate(
		r.Context(),
		workspaceID,
		runID,
		nodeID,
		commentID,
		req.Verdict,
		requestUserID(r),
	)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (h *Handler) PauseWorkflowRun(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	run, err := h.WorkflowService.PauseRun(
		r.Context(),
		workspaceID,
		runID,
		requestUserID(r),
	)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) ResumeWorkflowRun(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	run, err := h.WorkflowService.ResumeRun(
		r.Context(),
		workspaceID,
		runID,
		requestUserID(r),
	)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) CancelWorkflowRun(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	run, err := h.WorkflowService.CancelRun(
		r.Context(),
		workspaceID,
		runID,
		requestUserID(r),
	)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

type workflowRetryRequest struct {
	InputDigest string `json:"input_digest,omitempty"`
	LawDigest   string `json:"law_digest,omitempty"`
}

func (h *Handler) RetryWorkflowNode(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	if _, err := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{
		ID: runID, WorkspaceID: workspaceID,
	}); err != nil {
		writeError(w, http.StatusNotFound, "workflow run not found")
		return
	}
	nodeID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "nodeId"), "node_id")
	if !ok {
		return
	}
	var req workflowRetryRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	node, err := h.WorkflowService.RetryNode(r.Context(), runID, nodeID, req.InputDigest, req.LawDigest, requestUserID(r))
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (h *Handler) CancelWorkflowNode(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	runID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "run_id")
	if !ok {
		return
	}
	if _, err := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{
		ID: runID, WorkspaceID: workspaceID,
	}); err != nil {
		writeError(w, http.StatusNotFound, "workflow run not found")
		return
	}
	nodeID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "nodeId"), "node_id")
	if !ok {
		return
	}
	node, err := h.WorkflowService.CancelNode(r.Context(), runID, nodeID, requestUserID(r))
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, node)
}
