package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	DefaultWorkflowAffinityGraceSeconds = 60
	DefaultWorkflowLeaseSeconds         = 90
	MaxWorkflowBundleSize               = 100 << 20
)

var (
	ErrNoWorkflowNodeReady  = errors.New("no compatible workflow node is ready")
	ErrWorkflowResourceBusy = errors.New("workflow node resource is already claimed")
)

type WorkflowService struct {
	Queries   *db.Queries
	TxStarter TxStarter
}

func NewWorkflowService(q *db.Queries, tx TxStarter) *WorkflowService {
	return &WorkflowService{Queries: q, TxStarter: tx}
}

type WorkflowResourceSpec struct {
	Key  string `json:"key"`
	Mode string `json:"mode,omitempty"`
}

type WorkflowNodeSpec struct {
	Key               string                 `json:"key"`
	IssueID           pgtype.UUID            `json:"issue_id"`
	PassageKey        string                 `json:"passage_key"`
	NodeKey           string                 `json:"node_key"`
	ExecutorKind      string                 `json:"executor_kind"`
	AgentID           pgtype.UUID            `json:"agent_id"`
	RuntimePoolID     pgtype.UUID            `json:"runtime_pool_id"`
	Priority          int32                  `json:"priority"`
	PreferredDaemonID string                 `json:"preferred_daemon_id,omitempty"`
	InputDigest       string                 `json:"input_digest,omitempty"`
	LawDigest         string                 `json:"law_digest,omitempty"`
	OutputContract    json.RawMessage        `json:"output_contract"`
	MaxAttempts       int32                  `json:"max_attempts,omitempty"`
	DependsOn         []string               `json:"depends_on,omitempty"`
	Resources         []WorkflowResourceSpec `json:"resources,omitempty"`
}

type WorkflowGraphSpec struct {
	WorkspaceID         pgtype.UUID                      `json:"workspace_id"`
	ProjectID           pgtype.UUID                      `json:"project_id"`
	AnchorIssueID       pgtype.UUID                      `json:"anchor_issue_id"`
	GraphKey            string                           `json:"graph_key"`
	GraphVersion        string                           `json:"graph_version"`
	IntegrationPoolID   pgtype.UUID                      `json:"integration_pool_id"`
	WIPLimit            int32                            `json:"wip_limit"`
	HumanGateLimit      int32                            `json:"human_gate_limit"`
	InputDigest         string                           `json:"input_digest,omitempty"`
	LawDigest           string                           `json:"law_digest,omitempty"`
	Metadata            json.RawMessage                  `json:"metadata"`
	CreatedBy           pgtype.UUID                      `json:"created_by"`
	Nodes               []WorkflowNodeSpec               `json:"nodes"`
	ImportedCheckpoints []WorkflowImportedCheckpointSpec `json:"imported_checkpoints,omitempty"`
}

type WorkflowImportedCheckpointSpec struct {
	NodeKey         string          `json:"node_key"`
	CanonicalCommit string          `json:"canonical_commit"`
	ArtifactDigest  string          `json:"artifact_digest"`
	Manifest        json.RawMessage `json:"manifest"`
}

type deterministicWorkflowContract struct {
	Operation      string   `json:"operation"`
	Command        []string `json:"command"`
	AllowedPaths   []string `json:"allowed_paths"`
	TimeoutSeconds int32    `json:"timeout_seconds,omitempty"`
}

type WorkflowArtifactSubmission struct {
	TaskID         pgtype.UUID
	AttemptID      pgtype.UUID
	ClaimEpoch     int64
	BaseCommit     string
	ResultCommit   string
	ArtifactKey    string
	ArtifactDigest string
	ArtifactSize   int64
	Manifest       json.RawMessage
}

func newWorkflowUUID() pgtype.UUID {
	id := uuid.New()
	return pgtype.UUID{Bytes: id, Valid: true}
}

func optionalText(value string) pgtype.Text {
	value = strings.TrimSpace(value)
	return pgtype.Text{String: value, Valid: value != ""}
}

func jsonObjectOrEmpty(value json.RawMessage) []byte {
	if len(value) == 0 {
		return []byte(`{}`)
	}
	return value
}

func sameUUID(left, right pgtype.UUID) bool {
	return left.Valid && right.Valid && left.Bytes == right.Bytes
}

func (s *WorkflowService) runInTx(ctx context.Context, fn func(*db.Queries) error) error {
	if s.TxStarter == nil {
		return fn(s.Queries)
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin workflow transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := fn(s.Queries.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func resolveWorkflowSuccessorInputDigests(
	ctx context.Context,
	qtx *db.Queries,
	successors []db.WorkflowNode,
) error {
	for _, successor := range successors {
		material, err := qtx.GetWorkflowNodeResolvedInputMaterial(ctx, successor.ID)
		if err != nil {
			return fmt.Errorf("load workflow successor input material: %w", err)
		}
		encoded, err := json.Marshal(map[string]any{
			"dependencies": material,
			"law_digest":   successor.LawDigest.String,
			"generation":   successor.Generation,
		})
		if err != nil {
			return fmt.Errorf("encode workflow successor input material: %w", err)
		}
		digest := sha256.Sum256(encoded)
		if _, err := qtx.SetWorkflowNodeResolvedInputDigest(ctx, db.SetWorkflowNodeResolvedInputDigestParams{
			InputDigest: optionalText(fmt.Sprintf("sha256:%x", digest)),
			NodeID:      successor.ID,
		}); err != nil {
			return fmt.Errorf("set workflow successor input digest: %w", err)
		}
	}
	return nil
}

func (s *WorkflowService) CreateRuntimePool(
	ctx context.Context,
	workspaceID, createdBy pgtype.UUID,
	name string,
	maxInflight, affinityGraceSeconds, leaseSeconds int32,
) (db.RuntimePool, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return db.RuntimePool{}, errors.New("runtime pool name is required")
	}
	if maxInflight < 1 {
		return db.RuntimePool{}, errors.New("runtime pool max_inflight must be positive")
	}
	if affinityGraceSeconds < 0 {
		return db.RuntimePool{}, errors.New("runtime pool affinity_grace_seconds must not be negative")
	}
	if leaseSeconds < 30 {
		return db.RuntimePool{}, errors.New("runtime pool lease_seconds must be at least 30")
	}
	return s.Queries.CreateRuntimePool(ctx, db.CreateRuntimePoolParams{
		WorkspaceID:          workspaceID,
		Name:                 name,
		Enabled:              true,
		MaxInflight:          maxInflight,
		AffinityGraceSeconds: affinityGraceSeconds,
		LeaseSeconds:         leaseSeconds,
		CreatedBy:            createdBy,
	})
}

func (s *WorkflowService) AddRuntimeToPool(
	ctx context.Context,
	workspaceID, poolID, runtimeID pgtype.UUID,
	priority int32,
) (db.RuntimePoolRuntime, error) {
	pool, err := s.Queries.GetRuntimePoolInWorkspace(ctx, db.GetRuntimePoolInWorkspaceParams{
		ID:          poolID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		return db.RuntimePoolRuntime{}, fmt.Errorf("load runtime pool: %w", err)
	}
	runtime, err := s.Queries.GetAgentRuntime(ctx, runtimeID)
	if err != nil {
		return db.RuntimePoolRuntime{}, fmt.Errorf("load runtime: %w", err)
	}
	if !sameUUID(pool.WorkspaceID, runtime.WorkspaceID) {
		return db.RuntimePoolRuntime{}, errors.New("runtime and pool belong to different workspaces")
	}
	return s.Queries.AddRuntimeToPool(ctx, db.AddRuntimeToPoolParams{
		PoolID:    poolID,
		RuntimeID: runtimeID,
		Priority:  priority,
		Enabled:   true,
	})
}

func (s *WorkflowService) BindAgentToPool(
	ctx context.Context,
	workspaceID, agentID, poolID pgtype.UUID,
) (db.AgentRuntimePool, error) {
	pool, err := s.Queries.GetRuntimePoolInWorkspace(ctx, db.GetRuntimePoolInWorkspaceParams{
		ID:          poolID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		return db.AgentRuntimePool{}, fmt.Errorf("load runtime pool: %w", err)
	}
	agent, err := s.Queries.GetAgent(ctx, agentID)
	if err != nil {
		return db.AgentRuntimePool{}, fmt.Errorf("load agent: %w", err)
	}
	if !sameUUID(pool.WorkspaceID, agent.WorkspaceID) {
		return db.AgentRuntimePool{}, errors.New("agent and pool belong to different workspaces")
	}
	return s.Queries.BindAgentToRuntimePool(ctx, db.BindAgentToRuntimePoolParams{
		AgentID: agentID,
		PoolID:  poolID,
		Enabled: true,
	})
}

func validateWorkflowGraph(spec WorkflowGraphSpec) error {
	spec.GraphKey = strings.TrimSpace(spec.GraphKey)
	spec.GraphVersion = strings.TrimSpace(spec.GraphVersion)
	if !spec.WorkspaceID.Valid || !spec.ProjectID.Valid || !spec.AnchorIssueID.Valid || !spec.CreatedBy.Valid {
		return errors.New("workspace_id, project_id, anchor_issue_id, and created_by are required")
	}
	if spec.GraphKey == "" || spec.GraphVersion == "" {
		return errors.New("graph_key and graph_version are required")
	}
	if spec.WIPLimit < 1 || spec.HumanGateLimit < 1 {
		return errors.New("wip_limit and human_gate_limit must be positive")
	}
	if len(spec.Nodes) == 0 {
		return errors.New("workflow graph must contain at least one node")
	}

	byKey := make(map[string]WorkflowNodeSpec, len(spec.Nodes))
	inDegree := make(map[string]int, len(spec.Nodes))
	successors := make(map[string][]string, len(spec.Nodes))
	for _, node := range spec.Nodes {
		key := strings.TrimSpace(node.Key)
		if key == "" {
			return errors.New("every workflow node requires a key")
		}
		if _, exists := byKey[key]; exists {
			return fmt.Errorf("duplicate workflow node key %q", key)
		}
		if strings.TrimSpace(node.PassageKey) == "" || strings.TrimSpace(node.NodeKey) == "" {
			return fmt.Errorf("workflow node %q requires passage_key and node_key", key)
		}
		switch node.ExecutorKind {
		case "agent":
			if !node.AgentID.Valid || !node.RuntimePoolID.Valid {
				return fmt.Errorf("agent workflow node %q requires agent_id and runtime_pool_id", key)
			}
		case "human_gate", "deterministic":
			if node.AgentID.Valid || node.RuntimePoolID.Valid {
				return fmt.Errorf("%s workflow node %q must not declare agent_id or runtime_pool_id", node.ExecutorKind, key)
			}
			if node.ExecutorKind == "deterministic" {
				var contract deterministicWorkflowContract
				if err := json.Unmarshal(jsonObjectOrEmpty(node.OutputContract), &contract); err != nil {
					return fmt.Errorf("deterministic workflow node %q has invalid output_contract: %w", key, err)
				}
				if contract.Operation != "repository_command_v1" || len(contract.Command) == 0 {
					return fmt.Errorf("deterministic workflow node %q requires repository_command_v1 and a command argv", key)
				}
				for _, argument := range contract.Command {
					if strings.TrimSpace(argument) == "" {
						return fmt.Errorf("deterministic workflow node %q command contains an empty argument", key)
					}
				}
				if len(contract.AllowedPaths) == 0 {
					return fmt.Errorf("deterministic workflow node %q requires allowed_paths", key)
				}
				if contract.TimeoutSeconds < 0 || contract.TimeoutSeconds > 300 {
					return fmt.Errorf("deterministic workflow node %q timeout_seconds must be between 1 and 300", key)
				}
			}
		default:
			return fmt.Errorf("workflow node %q has unsupported executor_kind %q", key, node.ExecutorKind)
		}
		seenResources := make(map[string]struct{}, len(node.Resources))
		for _, resource := range node.Resources {
			resourceKey := strings.TrimSpace(resource.Key)
			if resourceKey == "" {
				return fmt.Errorf("workflow node %q has an empty resource key", key)
			}
			if resource.Mode != "" && resource.Mode != "exclusive" {
				return fmt.Errorf("workflow node %q resource %q has unsupported mode %q", key, resourceKey, resource.Mode)
			}
			if _, duplicate := seenResources[resourceKey]; duplicate {
				return fmt.Errorf("workflow node %q repeats resource %q", key, resourceKey)
			}
			seenResources[resourceKey] = struct{}{}
		}
		byKey[key] = node
		inDegree[key] = 0
	}
	for key, node := range byKey {
		seenDeps := make(map[string]struct{}, len(node.DependsOn))
		for _, rawDependency := range node.DependsOn {
			dependency := strings.TrimSpace(rawDependency)
			if dependency == key {
				return fmt.Errorf("workflow node %q cannot depend on itself", key)
			}
			if _, exists := byKey[dependency]; !exists {
				return fmt.Errorf("workflow node %q depends on unknown node %q", key, dependency)
			}
			if _, duplicate := seenDeps[dependency]; duplicate {
				return fmt.Errorf("workflow node %q repeats dependency %q", key, dependency)
			}
			seenDeps[dependency] = struct{}{}
			inDegree[key]++
			successors[dependency] = append(successors[dependency], key)
		}
	}
	queue := make([]string, 0, len(byKey))
	for key, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, key)
		}
	}
	visited := 0
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		visited++
		for _, successor := range successors[key] {
			inDegree[successor]--
			if inDegree[successor] == 0 {
				queue = append(queue, successor)
			}
		}
	}
	if visited != len(byKey) {
		return errors.New("workflow graph contains a dependency cycle")
	}
	imported := make(map[string]struct{}, len(spec.ImportedCheckpoints))
	for _, checkpoint := range spec.ImportedCheckpoints {
		key := strings.TrimSpace(checkpoint.NodeKey)
		if _, exists := byKey[key]; !exists {
			return fmt.Errorf("imported checkpoint references unknown workflow node %q", key)
		}
		if _, duplicate := imported[key]; duplicate {
			return fmt.Errorf("duplicate imported checkpoint for workflow node %q", key)
		}
		if strings.TrimSpace(checkpoint.CanonicalCommit) == "" ||
			strings.TrimSpace(checkpoint.ArtifactDigest) == "" {
			return fmt.Errorf("imported checkpoint %q requires canonical_commit and artifact_digest", key)
		}
		imported[key] = struct{}{}
	}
	for key := range imported {
		for _, dependency := range byKey[key].DependsOn {
			if _, present := imported[dependency]; !present {
				return fmt.Errorf("imported checkpoint %q is not dependency-closed; %q is missing", key, dependency)
			}
		}
	}
	return nil
}

func (s *WorkflowService) CreateWorkflowRun(ctx context.Context, spec WorkflowGraphSpec) (db.WorkflowRun, error) {
	if err := validateWorkflowGraph(spec); err != nil {
		return db.WorkflowRun{}, err
	}
	if _, err := s.Queries.GetProjectInWorkspace(ctx, db.GetProjectInWorkspaceParams{
		ID:          spec.ProjectID,
		WorkspaceID: spec.WorkspaceID,
	}); err != nil {
		return db.WorkflowRun{}, fmt.Errorf("load workflow project: %w", err)
	}
	anchor, err := s.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID:          spec.AnchorIssueID,
		WorkspaceID: spec.WorkspaceID,
	})
	if err != nil {
		return db.WorkflowRun{}, fmt.Errorf("load workflow anchor issue: %w", err)
	}
	if !sameUUID(anchor.ProjectID, spec.ProjectID) {
		return db.WorkflowRun{}, errors.New("workflow anchor issue does not belong to the selected project")
	}
	if spec.IntegrationPoolID.Valid {
		if _, err := s.Queries.GetRuntimePoolInWorkspace(ctx, db.GetRuntimePoolInWorkspaceParams{
			ID:          spec.IntegrationPoolID,
			WorkspaceID: spec.WorkspaceID,
		}); err != nil {
			return db.WorkflowRun{}, fmt.Errorf("load integration runtime pool: %w", err)
		}
	}

	for _, node := range spec.Nodes {
		issue, err := s.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
			ID:          node.IssueID,
			WorkspaceID: spec.WorkspaceID,
		})
		if err != nil {
			return db.WorkflowRun{}, fmt.Errorf("load issue for node %q: %w", node.Key, err)
		}
		if !sameUUID(issue.ProjectID, spec.ProjectID) {
			return db.WorkflowRun{}, fmt.Errorf("issue for node %q does not belong to the workflow project", node.Key)
		}
		if node.ExecutorKind == "agent" {
			agent, err := s.Queries.GetAgent(ctx, node.AgentID)
			if err != nil {
				return db.WorkflowRun{}, fmt.Errorf("load agent for node %q: %w", node.Key, err)
			}
			if !sameUUID(agent.WorkspaceID, spec.WorkspaceID) {
				return db.WorkflowRun{}, fmt.Errorf("agent for node %q belongs to another workspace", node.Key)
			}
			pool, err := s.Queries.GetRuntimePoolInWorkspace(ctx, db.GetRuntimePoolInWorkspaceParams{
				ID:          node.RuntimePoolID,
				WorkspaceID: spec.WorkspaceID,
			})
			if err != nil {
				return db.WorkflowRun{}, fmt.Errorf("load runtime pool for node %q: %w", node.Key, err)
			}
			if !pool.Enabled {
				return db.WorkflowRun{}, fmt.Errorf("runtime pool for node %q is disabled", node.Key)
			}
			bindings, err := s.Queries.ListAgentRuntimePools(ctx, db.ListAgentRuntimePoolsParams{
				AgentID:     node.AgentID,
				WorkspaceID: spec.WorkspaceID,
			})
			if err != nil {
				return db.WorkflowRun{}, fmt.Errorf("list runtime pool bindings for node %q: %w", node.Key, err)
			}
			bound := false
			for _, binding := range bindings {
				if binding.Enabled && sameUUID(binding.PoolID, node.RuntimePoolID) {
					bound = true
					break
				}
			}
			if !bound {
				return db.WorkflowRun{}, fmt.Errorf("agent for node %q is not enabled in its runtime pool", node.Key)
			}
		}
	}

	var run db.WorkflowRun
	err = s.runInTx(ctx, func(qtx *db.Queries) error {
		created, err := qtx.CreateWorkflowRun(ctx, db.CreateWorkflowRunParams{
			WorkspaceID:       spec.WorkspaceID,
			ProjectID:         spec.ProjectID,
			AnchorIssueID:     spec.AnchorIssueID,
			GraphKey:          strings.TrimSpace(spec.GraphKey),
			GraphVersion:      strings.TrimSpace(spec.GraphVersion),
			Status:            "running",
			IntegrationPoolID: spec.IntegrationPoolID,
			WipLimit:          spec.WIPLimit,
			HumanGateLimit:    spec.HumanGateLimit,
			InputDigest:       optionalText(spec.InputDigest),
			LawDigest:         optionalText(spec.LawDigest),
			Metadata:          jsonObjectOrEmpty(spec.Metadata),
			CreatedBy:         spec.CreatedBy,
		})
		if err != nil {
			return fmt.Errorf("create workflow run: %w", err)
		}
		run = created

		nodeIDs := make(map[string]pgtype.UUID, len(spec.Nodes))
		now := time.Now()
		for _, nodeSpec := range spec.Nodes {
			state := "pending"
			var readyAt pgtype.Timestamptz
			var stealableAt pgtype.Timestamptz
			if len(nodeSpec.DependsOn) == 0 {
				if nodeSpec.ExecutorKind == "human_gate" {
					state = "waiting_human"
				} else if nodeSpec.ExecutorKind == "deterministic" {
					state = "ready"
					readyAt = pgtype.Timestamptz{Time: now, Valid: true}
				} else {
					state = "ready"
					readyAt = pgtype.Timestamptz{Time: now, Valid: true}
					pool, err := qtx.GetRuntimePool(ctx, nodeSpec.RuntimePoolID)
					if err != nil {
						return fmt.Errorf("reload runtime pool for root node %q: %w", nodeSpec.Key, err)
					}
					stealableAt = pgtype.Timestamptz{
						Time:  now.Add(time.Duration(pool.AffinityGraceSeconds) * time.Second),
						Valid: true,
					}
				}
			}
			maxAttempts := nodeSpec.MaxAttempts
			if maxAttempts < 1 {
				maxAttempts = 3
			}
			outputContract := jsonObjectOrEmpty(nodeSpec.OutputContract)
			node, err := qtx.CreateWorkflowNode(ctx, db.CreateWorkflowNodeParams{
				RunID:             created.ID,
				IssueID:           nodeSpec.IssueID,
				PassageKey:        strings.TrimSpace(nodeSpec.PassageKey),
				NodeKey:           strings.TrimSpace(nodeSpec.NodeKey),
				Generation:        1,
				ExecutorKind:      nodeSpec.ExecutorKind,
				AgentID:           nodeSpec.AgentID,
				RuntimePoolID:     nodeSpec.RuntimePoolID,
				State:             state,
				Priority:          nodeSpec.Priority,
				PreferredDaemonID: optionalText(nodeSpec.PreferredDaemonID),
				StealableAt:       stealableAt,
				InputDigest:       optionalText(nodeSpec.InputDigest),
				LawDigest:         optionalText(nodeSpec.LawDigest),
				OutputContract:    outputContract,
				MaxAttempts:       maxAttempts,
				ReadyAt:           readyAt,
			})
			if err != nil {
				return fmt.Errorf("create workflow node %q: %w", nodeSpec.Key, err)
			}
			nodeIDs[nodeSpec.Key] = node.ID
			for _, resourceSpec := range nodeSpec.Resources {
				mode := resourceSpec.Mode
				if mode == "" {
					mode = "exclusive"
				}
				if _, err := qtx.CreateWorkflowNodeResource(ctx, db.CreateWorkflowNodeResourceParams{
					NodeID:      node.ID,
					ResourceKey: strings.TrimSpace(resourceSpec.Key),
					Mode:        mode,
				}); err != nil {
					return fmt.Errorf("create resource for workflow node %q: %w", nodeSpec.Key, err)
				}
			}
		}
		for _, nodeSpec := range spec.Nodes {
			nodeID := nodeIDs[nodeSpec.Key]
			for _, dependency := range nodeSpec.DependsOn {
				if _, err := qtx.CreateWorkflowNodeDependency(ctx, db.CreateWorkflowNodeDependencyParams{
					NodeID:          nodeID,
					DependsOnNodeID: nodeIDs[dependency],
				}); err != nil {
					return fmt.Errorf("create dependency for workflow node %q: %w", nodeSpec.Key, err)
				}
			}
		}
		for _, checkpoint := range spec.ImportedCheckpoints {
			nodeSpec := byKeyForWorkflowSpec(spec.Nodes, checkpoint.NodeKey)
			active, err := qtx.HasActiveTaskForIssue(ctx, nodeSpec.IssueID)
			if err != nil {
				return fmt.Errorf("check active task before importing workflow checkpoint %q: %w", checkpoint.NodeKey, err)
			}
			if active {
				return fmt.Errorf("cannot import workflow checkpoint %q while its issue has an active task", checkpoint.NodeKey)
			}
			nodeID := nodeIDs[checkpoint.NodeKey]
			attemptID := newWorkflowUUID()
			manifest := jsonObjectOrEmpty(checkpoint.Manifest)
			if _, err := qtx.CreateImportedWorkflowNodeAttempt(ctx, db.CreateImportedWorkflowNodeAttemptParams{
				ID:              attemptID,
				NodeID:          nodeID,
				ClaimEpoch:      1,
				CanonicalCommit: optionalText(checkpoint.CanonicalCommit),
				ArtifactDigest:  optionalText(checkpoint.ArtifactDigest),
				Manifest:        manifest,
			}); err != nil {
				return fmt.Errorf("create imported workflow attempt %q: %w", checkpoint.NodeKey, err)
			}
			if _, err := qtx.CompleteImportedWorkflowNode(ctx, db.CompleteImportedWorkflowNodeParams{
				ClaimEpoch: 1,
				AttemptID:  attemptID,
				NodeID:     nodeID,
			}); err != nil {
				return fmt.Errorf("complete imported workflow node %q: %w", checkpoint.NodeKey, err)
			}
			if _, err := qtx.CreateImportedWorkflowNodeResult(ctx, db.CreateImportedWorkflowNodeResultParams{
				AttemptID:       attemptID,
				ClaimEpoch:      1,
				CanonicalCommit: strings.TrimSpace(checkpoint.CanonicalCommit),
				ArtifactDigest:  strings.TrimSpace(checkpoint.ArtifactDigest),
				Manifest:        manifest,
				NodeID:          nodeID,
			}); err != nil {
				return fmt.Errorf("create imported workflow result %q: %w", checkpoint.NodeKey, err)
			}
		}
		for _, checkpoint := range spec.ImportedCheckpoints {
			successors, err := qtx.ReleaseReadyWorkflowSuccessors(ctx, nodeIDs[checkpoint.NodeKey])
			if err != nil {
				return fmt.Errorf("release successors after imported checkpoint %q: %w", checkpoint.NodeKey, err)
			}
			if err := resolveWorkflowSuccessorInputDigests(ctx, qtx, successors); err != nil {
				return fmt.Errorf("resolve successors after imported checkpoint %q: %w", checkpoint.NodeKey, err)
			}
		}
		return nil
	})
	if err != nil {
		return db.WorkflowRun{}, err
	}
	return run, nil
}

func byKeyForWorkflowSpec(nodes []WorkflowNodeSpec, key string) WorkflowNodeSpec {
	for _, node := range nodes {
		if node.Key == key {
			return node
		}
	}
	return WorkflowNodeSpec{}
}

func (s *WorkflowService) MaterializeReadyAgentTasks(
	ctx context.Context,
	daemonID string,
	runtimeIDs []pgtype.UUID,
	maxTasks int,
) ([]db.AgentTaskQueue, error) {
	daemonID = strings.TrimSpace(daemonID)
	if daemonID == "" || len(runtimeIDs) == 0 || maxTasks < 1 {
		return nil, nil
	}
	out := make([]db.AgentTaskQueue, 0, maxTasks)
	for len(out) < maxTasks {
		var task db.AgentTaskQueue
		err := s.runInTx(ctx, func(qtx *db.Queries) error {
			if err := qtx.AcquireWorkflowClaimLock(ctx); err != nil {
				return fmt.Errorf("acquire workflow claim lock: %w", err)
			}
			candidate, err := qtx.SelectWorkflowNodeClaimCandidate(ctx, db.SelectWorkflowNodeClaimCandidateParams{
				RuntimeIds: runtimeIDs,
				DaemonID:   optionalText(daemonID),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoWorkflowNodeReady
			}
			if err != nil {
				return fmt.Errorf("select workflow node claim candidate: %w", err)
			}
			attemptID := newWorkflowUUID()
			node, err := qtx.ClaimWorkflowNode(ctx, db.ClaimWorkflowNodeParams{
				AttemptID: attemptID,
				DaemonID:  optionalText(daemonID),
				ID:        candidate.ID,
			})
			if err != nil {
				return fmt.Errorf("claim workflow node: %w", err)
			}
			preferredDaemonAtClaim := candidate.PreferredDaemonID
			if !preferredDaemonAtClaim.Valid {
				preferredDaemonAtClaim = optionalText(daemonID)
			}
			if _, err := qtx.CreateWorkflowNodeAttempt(ctx, db.CreateWorkflowNodeAttemptParams{
				ID:                     attemptID,
				NodeID:                 node.ID,
				ClaimEpoch:             node.ClaimEpoch,
				RuntimeID:              candidate.SelectedRuntimeID,
				DaemonID:               daemonID,
				PreferredDaemonAtClaim: preferredDaemonAtClaim,
				AffinityStolen: pgtype.Bool{
					Bool:  candidate.PreferredDaemonID.Valid && candidate.PreferredDaemonID.String != daemonID,
					Valid: true,
				},
				LeaseSeconds: float64(candidate.LeaseSeconds),
			}); err != nil {
				return fmt.Errorf("create workflow attempt: %w", err)
			}
			expectedClaims, err := qtx.CountWorkflowNodeExclusiveResources(ctx, node.ID)
			if err != nil {
				return fmt.Errorf("count workflow resources: %w", err)
			}
			if _, err := qtx.ClaimWorkflowNodeResources(ctx, db.ClaimWorkflowNodeResourcesParams{
				NodeID:    node.ID,
				AttemptID: attemptID,
			}); err != nil {
				return fmt.Errorf("claim workflow resources: %w", err)
			}
			actualClaims, err := qtx.CountWorkflowAttemptResourceClaims(ctx, db.CountWorkflowAttemptResourceClaimsParams{
				NodeID:    node.ID,
				AttemptID: attemptID,
			})
			if err != nil {
				return fmt.Errorf("count acquired workflow resources: %w", err)
			}
			if actualClaims != expectedClaims {
				return ErrWorkflowResourceBusy
			}
			createdTask, err := qtx.CreateWorkflowAgentTask(ctx, db.CreateWorkflowAgentTaskParams{
				RuntimeID: candidate.SelectedRuntimeID,
				AttemptID: attemptID,
				NodeID:    node.ID,
			})
			if err != nil {
				return fmt.Errorf("create workflow agent task: %w", err)
			}
			if _, err := qtx.AttachTaskToWorkflowAttempt(ctx, db.AttachTaskToWorkflowAttemptParams{
				TaskID:    createdTask.ID,
				AttemptID: attemptID,
				NodeID:    node.ID,
			}); err != nil {
				return fmt.Errorf("attach task to workflow attempt: %w", err)
			}
			task = createdTask
			return nil
		})
		if errors.Is(err, ErrNoWorkflowNodeReady) {
			break
		}
		if errors.Is(err, ErrWorkflowResourceBusy) {
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, task)
	}
	return out, nil
}

func (s *WorkflowService) MaterializeReadyDeterministicNodes(
	ctx context.Context,
	daemonID string,
	runtimeIDs []pgtype.UUID,
	maxNodes int,
) (int, error) {
	daemonID = strings.TrimSpace(daemonID)
	if daemonID == "" || len(runtimeIDs) == 0 || maxNodes < 1 {
		return 0, nil
	}
	materialized := 0
	for materialized < maxNodes {
		err := s.runInTx(ctx, func(qtx *db.Queries) error {
			if err := qtx.AcquireWorkflowClaimLock(ctx); err != nil {
				return fmt.Errorf("acquire deterministic workflow claim lock: %w", err)
			}
			candidate, err := qtx.SelectDeterministicWorkflowNodeCandidate(ctx, db.SelectDeterministicWorkflowNodeCandidateParams{
				RuntimeIds: runtimeIDs,
				DaemonID:   optionalText(daemonID),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoWorkflowNodeReady
			}
			if err != nil {
				return fmt.Errorf("select deterministic workflow node: %w", err)
			}
			attemptID := newWorkflowUUID()
			node, err := qtx.ClaimDeterministicWorkflowNode(ctx, db.ClaimDeterministicWorkflowNodeParams{
				AttemptID: attemptID,
				ID:        candidate.ID,
			})
			if err != nil {
				return fmt.Errorf("claim deterministic workflow node: %w", err)
			}
			expectedClaims, err := qtx.CountWorkflowNodeExclusiveResources(ctx, node.ID)
			if err != nil {
				return fmt.Errorf("count deterministic workflow resources: %w", err)
			}
			if _, err := qtx.ClaimWorkflowNodeResources(ctx, db.ClaimWorkflowNodeResourcesParams{
				NodeID:    node.ID,
				AttemptID: attemptID,
			}); err != nil {
				return fmt.Errorf("claim deterministic workflow resources: %w", err)
			}
			actualClaims, err := qtx.CountWorkflowAttemptResourceClaims(ctx, db.CountWorkflowAttemptResourceClaimsParams{
				NodeID:    node.ID,
				AttemptID: attemptID,
			})
			if err != nil {
				return fmt.Errorf("count deterministic workflow resource claims: %w", err)
			}
			if actualClaims != expectedClaims {
				return ErrWorkflowResourceBusy
			}
			digest := sha256.Sum256(node.OutputContract)
			artifactDigest := fmt.Sprintf("sha256:%x", digest)
			manifest, _ := json.Marshal(map[string]any{
				"executor_kind":   "deterministic",
				"operation":       "repository_command_v1",
				"node_id":         util.UUIDToString(node.ID),
				"attempt_id":      util.UUIDToString(attemptID),
				"claim_epoch":     node.ClaimEpoch,
				"contract_digest": artifactDigest,
			})
			if _, err := qtx.CreateDeterministicWorkflowNodeAttempt(ctx, db.CreateDeterministicWorkflowNodeAttemptParams{
				ID:             attemptID,
				NodeID:         node.ID,
				ClaimEpoch:     node.ClaimEpoch,
				RuntimeID:      candidate.SelectedRuntimeID,
				DaemonID:       daemonID,
				LeaseSeconds:   float64(candidate.LeaseSeconds),
				ArtifactDigest: optionalText(artifactDigest),
				Manifest:       manifest,
			}); err != nil {
				return fmt.Errorf("create deterministic workflow attempt: %w", err)
			}
			payload, _ := json.Marshal(map[string]any{
				"attempt_id":  util.UUIDToString(attemptID),
				"claim_epoch": node.ClaimEpoch,
				"operation":   "repository_command_v1",
			})
			if _, err := qtx.InsertWorkflowOutbox(ctx, db.InsertWorkflowOutboxParams{
				RunID:     node.RunID,
				NodeID:    node.ID,
				EventType: "workflow.deterministic_ready",
				Payload:   payload,
			}); err != nil {
				return fmt.Errorf("enqueue deterministic workflow integration: %w", err)
			}
			return nil
		})
		if errors.Is(err, ErrNoWorkflowNodeReady) {
			break
		}
		if errors.Is(err, ErrWorkflowResourceBusy) {
			continue
		}
		if err != nil {
			return materialized, err
		}
		materialized++
	}
	return materialized, nil
}

func (s *WorkflowService) ClaimIntegrationJobs(
	ctx context.Context,
	daemonID string,
	runtimeIDs []pgtype.UUID,
	maxJobs int,
) ([]db.ClaimWorkflowIntegrationJobRow, error) {
	if strings.TrimSpace(daemonID) == "" || len(runtimeIDs) == 0 || maxJobs < 1 {
		return nil, nil
	}
	if _, err := s.Queries.RequeueExpiredWorkflowIntegrationEvents(ctx); err != nil {
		return nil, fmt.Errorf("requeue expired workflow integration jobs: %w", err)
	}
	jobs := make([]db.ClaimWorkflowIntegrationJobRow, 0, maxJobs)
	for len(jobs) < maxJobs {
		var job db.ClaimWorkflowIntegrationJobRow
		err := s.runInTx(ctx, func(qtx *db.Queries) error {
			if err := qtx.AcquireWorkflowClaimLock(ctx); err != nil {
				return fmt.Errorf("acquire workflow integration claim lock: %w", err)
			}
			claimed, err := qtx.ClaimWorkflowIntegrationJob(ctx, db.ClaimWorkflowIntegrationJobParams{
				RuntimeIds: runtimeIDs,
				DaemonID:   optionalText(daemonID),
			})
			if err != nil {
				return err
			}
			job = claimed
			return nil
		})
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return jobs, fmt.Errorf("claim workflow integration job: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *WorkflowService) SubmitArtifact(ctx context.Context, submission WorkflowArtifactSubmission) (db.WorkflowNodeAttempt, error) {
	if submission.ArtifactSize < 1 || submission.ArtifactSize > MaxWorkflowBundleSize {
		return db.WorkflowNodeAttempt{}, fmt.Errorf("workflow artifact size must be between 1 and %d bytes", MaxWorkflowBundleSize)
	}
	if strings.TrimSpace(submission.BaseCommit) == "" ||
		strings.TrimSpace(submission.ResultCommit) == "" ||
		strings.TrimSpace(submission.ArtifactKey) == "" ||
		strings.TrimSpace(submission.ArtifactDigest) == "" {
		return db.WorkflowNodeAttempt{}, errors.New("base_commit, result_commit, artifact_key, and artifact_digest are required")
	}
	var attempt db.WorkflowNodeAttempt
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		current, err := qtx.GetWorkflowAttemptByTask(ctx, submission.TaskID)
		if err != nil {
			return fmt.Errorf("load workflow attempt: %w", err)
		}
		if !sameUUID(current.ID, submission.AttemptID) || current.ClaimEpoch != submission.ClaimEpoch {
			return errors.New("stale workflow attempt or claim epoch")
		}
		updated, err := qtx.SubmitWorkflowAttemptArtifact(ctx, db.SubmitWorkflowAttemptArtifactParams{
			BaseCommit:     optionalText(submission.BaseCommit),
			ResultCommit:   optionalText(submission.ResultCommit),
			ArtifactKey:    optionalText(submission.ArtifactKey),
			ArtifactDigest: optionalText(submission.ArtifactDigest),
			ArtifactSize:   pgtype.Int8{Int64: submission.ArtifactSize, Valid: true},
			Manifest:       jsonObjectOrEmpty(submission.Manifest),
			AttemptID:      submission.AttemptID,
			ClaimEpoch:     submission.ClaimEpoch,
		})
		if err != nil {
			return fmt.Errorf("submit workflow artifact: %w", err)
		}
		if _, err := qtx.MarkWorkflowNodeSubmitted(ctx, db.MarkWorkflowNodeSubmittedParams{
			AttemptID:  submission.AttemptID,
			ClaimEpoch: submission.ClaimEpoch,
		}); err != nil {
			return fmt.Errorf("mark workflow node submitted: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{
			"attempt_id":      util.UUIDToString(submission.AttemptID),
			"claim_epoch":     submission.ClaimEpoch,
			"artifact_key":    submission.ArtifactKey,
			"artifact_digest": submission.ArtifactDigest,
		})
		node, err := qtx.GetWorkflowNodeForTask(ctx, submission.TaskID)
		if err != nil {
			return fmt.Errorf("load submitted workflow node: %w", err)
		}
		if _, err := qtx.InsertWorkflowOutbox(ctx, db.InsertWorkflowOutboxParams{
			RunID:     node.RunID,
			NodeID:    node.ID,
			EventType: "workflow.artifact_submitted",
			Payload:   payload,
		}); err != nil {
			return fmt.Errorf("enqueue workflow artifact integration: %w", err)
		}
		attempt = updated
		return nil
	})
	return attempt, err
}

func (s *WorkflowService) AcceptIntegratedResult(
	ctx context.Context,
	eventID, nodeID, attemptID pgtype.UUID,
	claimEpoch int64,
	canonicalCommit string,
) (db.WorkflowNodeResult, error) {
	canonicalCommit = strings.TrimSpace(canonicalCommit)
	if canonicalCommit == "" {
		return db.WorkflowNodeResult{}, errors.New("canonical_commit is required")
	}
	var result db.WorkflowNodeResult
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		existing, existingErr := qtx.GetWorkflowNodeResult(ctx, nodeID)
		if existingErr == nil {
			if existing.CanonicalCommit != canonicalCommit ||
				!sameUUID(existing.AttemptID, attemptID) ||
				existing.ClaimEpoch != claimEpoch {
				return errors.New("workflow node already has a different accepted result")
			}
			if _, err := qtx.CompleteWorkflowOutboxEvent(ctx, eventID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("complete replayed workflow integration event: %w", err)
			}
			result = existing
			return nil
		}
		if !errors.Is(existingErr, pgx.ErrNoRows) {
			return fmt.Errorf("load accepted workflow result: %w", existingErr)
		}
		if _, err := qtx.MarkWorkflowNodeIntegrating(ctx, db.MarkWorkflowNodeIntegratingParams{
			NodeID:     nodeID,
			AttemptID:  attemptID,
			ClaimEpoch: claimEpoch,
		}); err != nil {
			return fmt.Errorf("fence workflow integration: %w", err)
		}
		accepted, err := qtx.AcceptWorkflowNodeResult(ctx, db.AcceptWorkflowNodeResultParams{
			CanonicalCommit: canonicalCommit,
			NodeID:          nodeID,
			AttemptID:       attemptID,
			ClaimEpoch:      claimEpoch,
		})
		if err != nil {
			return fmt.Errorf("accept workflow result: %w", err)
		}
		if _, err := qtx.MarkWorkflowAttemptIntegrated(ctx, db.MarkWorkflowAttemptIntegratedParams{
			AttemptID:  attemptID,
			ClaimEpoch: claimEpoch,
		}); err != nil {
			return fmt.Errorf("mark workflow attempt integrated: %w", err)
		}
		node, err := qtx.CompleteWorkflowNode(ctx, db.CompleteWorkflowNodeParams{
			NodeID:     nodeID,
			AttemptID:  attemptID,
			ClaimEpoch: claimEpoch,
		})
		if err != nil {
			return fmt.Errorf("complete workflow node: %w", err)
		}
		if _, err := qtx.ReleaseWorkflowAttemptResources(ctx, attemptID); err != nil {
			return fmt.Errorf("release workflow resources: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{
			"attempt_id":       util.UUIDToString(attemptID),
			"claim_epoch":      claimEpoch,
			"canonical_commit": canonicalCommit,
		})
		if _, err := qtx.InsertWorkflowOutbox(ctx, db.InsertWorkflowOutboxParams{
			RunID:     node.RunID,
			NodeID:    node.ID,
			EventType: "workflow.node_accepted",
			Payload:   payload,
		}); err != nil {
			return fmt.Errorf("enqueue successor release: %w", err)
		}
		if _, err := qtx.CompleteWorkflowOutboxEvent(ctx, eventID); err != nil {
			return fmt.Errorf("complete workflow integration event: %w", err)
		}
		result = accepted
		return nil
	})
	return result, err
}

func (s *WorkflowService) FailIntegration(
	ctx context.Context,
	eventID, nodeID, attemptID pgtype.UUID,
	claimEpoch int64,
	message string,
	retryable bool,
) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "workflow integration failed"
	}
	return s.runInTx(ctx, func(qtx *db.Queries) error {
		if retryable {
			if _, err := qtx.RetryWorkflowOutboxEvent(ctx, db.RetryWorkflowOutboxEventParams{
				RetryAfterSeconds: 15,
				LastError:         pgtype.Text{String: message, Valid: true},
				ID:                eventID,
			}); err != nil {
				return fmt.Errorf("retry workflow integration event: %w", err)
			}
			return nil
		}
		if _, err := qtx.FailWorkflowOutboxEvent(ctx, db.FailWorkflowOutboxEventParams{
			LastError: pgtype.Text{String: message, Valid: true},
			ID:        eventID,
		}); err != nil {
			return fmt.Errorf("fail workflow integration event: %w", err)
		}
		if _, err := qtx.BlockWorkflowNodeIntegration(ctx, db.BlockWorkflowNodeIntegrationParams{
			NodeID:     nodeID,
			AttemptID:  attemptID,
			ClaimEpoch: claimEpoch,
		}); err != nil {
			return fmt.Errorf("block workflow node after integration failure: %w", err)
		}
		if _, err := qtx.ReleaseWorkflowAttemptResources(ctx, attemptID); err != nil {
			return fmt.Errorf("release blocked workflow resources: %w", err)
		}
		return nil
	})
}

func (s *WorkflowService) CompleteHumanGate(
	ctx context.Context,
	runID, nodeID pgtype.UUID,
) (db.WorkflowNode, error) {
	var gate db.WorkflowNode
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		completed, err := qtx.CompleteWorkflowHumanGate(ctx, db.CompleteWorkflowHumanGateParams{
			NodeID: nodeID,
			RunID:  runID,
		})
		if err != nil {
			return fmt.Errorf("complete workflow human gate: %w", err)
		}
		if _, err := qtx.InsertWorkflowOutbox(ctx, db.InsertWorkflowOutboxParams{
			RunID:     runID,
			NodeID:    nodeID,
			EventType: "workflow.node_accepted",
			Payload:   []byte(`{"executor_kind":"human_gate"}`),
		}); err != nil {
			return fmt.Errorf("enqueue gate successor release: %w", err)
		}
		gate = completed
		return nil
	})
	return gate, err
}

func (s *WorkflowService) RetryNode(
	ctx context.Context,
	runID, nodeID pgtype.UUID,
	inputDigest, lawDigest, actorID string,
) (db.WorkflowNode, error) {
	var node db.WorkflowNode
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		retried, err := qtx.RetryWorkflowNode(ctx, db.RetryWorkflowNodeParams{
			InputDigest: optionalText(inputDigest),
			LawDigest:   optionalText(lawDigest),
			NodeID:      nodeID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("retry workflow node: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{
			"command":    "retry",
			"actor_id":   actorID,
			"generation": retried.Generation,
		})
		if _, err := qtx.InsertWorkflowAuditEvent(ctx, db.InsertWorkflowAuditEventParams{
			RunID:     runID,
			NodeID:    nodeID,
			EventType: "workflow.operator_retry",
			Payload:   payload,
		}); err != nil {
			return fmt.Errorf("audit workflow retry: %w", err)
		}
		node = retried
		return nil
	})
	return node, err
}

func (s *WorkflowService) CancelNode(
	ctx context.Context,
	runID, nodeID pgtype.UUID,
	actorID string,
) (db.WorkflowNode, error) {
	var cancelled db.WorkflowNode
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		current, err := qtx.GetWorkflowNode(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("load workflow node: %w", err)
		}
		if !sameUUID(current.RunID, runID) {
			return errors.New("workflow node does not belong to the run")
		}
		node, err := qtx.CancelWorkflowNode(ctx, db.CancelWorkflowNodeParams{
			NodeID: nodeID,
			RunID:  runID,
		})
		if err != nil {
			return fmt.Errorf("cancel workflow node: %w", err)
		}
		if current.CurrentAttemptID.Valid {
			message := pgtype.Text{String: "cancelled by workflow operator", Valid: true}
			if _, err := qtx.CancelWorkflowNodeAttempt(ctx, db.CancelWorkflowNodeAttemptParams{
				Error:     message,
				AttemptID: current.CurrentAttemptID,
			}); err != nil {
				return fmt.Errorf("cancel workflow attempt: %w", err)
			}
			if _, err := qtx.CancelWorkflowTaskForAttempt(ctx, db.CancelWorkflowTaskForAttemptParams{
				Error:     message,
				AttemptID: current.CurrentAttemptID,
			}); err != nil {
				return fmt.Errorf("cancel workflow task: %w", err)
			}
			if _, err := qtx.ReleaseWorkflowAttemptResources(ctx, current.CurrentAttemptID); err != nil {
				return fmt.Errorf("release cancelled workflow resources: %w", err)
			}
		}
		if _, err := qtx.CancelWorkflowOutboxForNode(ctx, db.CancelWorkflowOutboxForNodeParams{
			Error:  pgtype.Text{String: "cancelled by workflow operator", Valid: true},
			NodeID: nodeID,
		}); err != nil {
			return fmt.Errorf("cancel workflow outbox: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{"command": "cancel", "actor_id": actorID})
		if _, err := qtx.InsertWorkflowAuditEvent(ctx, db.InsertWorkflowAuditEventParams{
			RunID:     runID,
			NodeID:    nodeID,
			EventType: "workflow.operator_cancel",
			Payload:   payload,
		}); err != nil {
			return fmt.Errorf("audit workflow cancellation: %w", err)
		}
		cancelled = node
		return nil
	})
	return cancelled, err
}

func (s *WorkflowService) ProcessOutbox(ctx context.Context, maxEvents int32) (int, error) {
	if maxEvents < 1 {
		return 0, nil
	}
	processed := 0
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		events, err := qtx.ClaimWorkflowReleaseEvents(ctx, maxEvents)
		if err != nil {
			return fmt.Errorf("claim workflow outbox: %w", err)
		}
		for _, event := range events {
			switch event.EventType {
			case "workflow.node_accepted":
				if !event.NodeID.Valid {
					return fmt.Errorf("workflow outbox event %s has no node", util.UUIDToString(event.ID))
				}
				successors, err := qtx.ReleaseReadyWorkflowSuccessors(ctx, event.NodeID)
				if err != nil {
					return fmt.Errorf("release workflow successors: %w", err)
				}
				if err := resolveWorkflowSuccessorInputDigests(ctx, qtx, successors); err != nil {
					return fmt.Errorf("resolve workflow successor inputs: %w", err)
				}
			default:
				if _, err := qtx.RetryWorkflowOutboxEvent(ctx, db.RetryWorkflowOutboxEventParams{
					RetryAfterSeconds: 30,
					LastError:         pgtype.Text{String: "unsupported workflow event type", Valid: true},
					ID:                event.ID,
				}); err != nil {
					return err
				}
				continue
			}
			if _, err := qtx.CompleteWorkflowOutboxEvent(ctx, event.ID); err != nil {
				return fmt.Errorf("complete workflow outbox event: %w", err)
			}
			processed++
		}
		return nil
	})
	return processed, err
}

func (s *WorkflowService) ExpireLeases(ctx context.Context, maxAttempts int32) (int, error) {
	if maxAttempts < 1 {
		return 0, nil
	}
	expiredCount := 0
	err := s.runInTx(ctx, func(qtx *db.Queries) error {
		attempts, err := qtx.ListExpiredWorkflowAttempts(ctx, maxAttempts)
		if err != nil {
			return fmt.Errorf("list expired workflow attempts: %w", err)
		}
		for _, attempt := range attempts {
			if _, err := qtx.FailWorkflowAttempt(ctx, db.FailWorkflowAttemptParams{
				Status:     "expired",
				Error:      pgtype.Text{String: "workflow lease expired", Valid: true},
				AttemptID:  attempt.ID,
				ClaimEpoch: attempt.ClaimEpoch,
			}); err != nil {
				return fmt.Errorf("expire workflow attempt: %w", err)
			}
			if _, err := qtx.RequeueWorkflowNodeAfterAttempt(ctx, db.RequeueWorkflowNodeAfterAttemptParams{
				NodeID:     attempt.NodeID,
				AttemptID:  attempt.ID,
				ClaimEpoch: attempt.ClaimEpoch,
			}); err != nil {
				return fmt.Errorf("requeue expired workflow node: %w", err)
			}
			if _, err := qtx.ReleaseWorkflowAttemptResources(ctx, attempt.ID); err != nil {
				return fmt.Errorf("release expired workflow resources: %w", err)
			}
			if _, err := qtx.ExpireWorkflowTask(ctx, attempt.ID); err != nil {
				return fmt.Errorf("expire workflow task: %w", err)
			}
			expiredCount++
		}
		return nil
	})
	return expiredCount, err
}
