package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func workflowTestUUID(seed byte) pgtype.UUID {
	var value [16]byte
	for index := range value {
		value[index] = seed
	}
	return pgtype.UUID{Bytes: value, Valid: true}
}

func validWorkflowGraphSpec() WorkflowGraphSpec {
	workspaceID := workflowTestUUID(1)
	projectID := workflowTestUUID(2)
	issueID := workflowTestUUID(3)
	agentID := workflowTestUUID(4)
	poolID := workflowTestUUID(5)
	return WorkflowGraphSpec{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		AnchorIssueID:     issueID,
		GraphKey:          "pub-passage-production",
		GraphVersion:      "1",
		IntegrationPoolID: workflowTestUUID(6),
		WIPLimit:          4,
		HumanGateLimit:    5,
		CreatedBy:         workflowTestUUID(7),
		Nodes: []WorkflowNodeSpec{
			{
				Key:           "passage:07",
				IssueID:       issueID,
				PassageKey:    "passage",
				NodeKey:       "07",
				ExecutorKind:  "agent",
				AgentID:       agentID,
				RuntimePoolID: poolID,
				OutputContract: json.RawMessage(
					`{"allowed_paths":["pipeline-output/passage/07.md"]}`,
				),
				Resources: []WorkflowResourceSpec{{Key: "book/book/passage/passage/07"}},
			},
			{
				Key:           "passage:08",
				IssueID:       issueID,
				PassageKey:    "passage",
				NodeKey:       "08",
				ExecutorKind:  "agent",
				AgentID:       agentID,
				RuntimePoolID: poolID,
				OutputContract: json.RawMessage(
					`{"allowed_paths":["pipeline-output/passage/08.md"]}`,
				),
				Resources: []WorkflowResourceSpec{{Key: "book/book/passage/passage/08"}},
			},
			{
				Key:          "passage:join",
				IssueID:      issueID,
				PassageKey:   "passage",
				NodeKey:      "join",
				ExecutorKind: "deterministic",
				OutputContract: json.RawMessage(
					`{"operation":"repository_command_v1","command":["uv","run","tools/join.py"],"allowed_paths":["pipeline-output/passage/issue-log.md"]}`,
				),
				DependsOn: []string{"passage:07", "passage:08"},
				Resources: []WorkflowResourceSpec{{Key: "book/book/passage/passage"}},
			},
		},
	}
}

func TestValidateWorkflowGraphAllowsParallelBranchesAndDeterministicJoin(t *testing.T) {
	if err := validateWorkflowGraph(validWorkflowGraphSpec()); err != nil {
		t.Fatalf("valid graph rejected: %v", err)
	}
}

func TestValidateWorkflowGraphRejectsCycle(t *testing.T) {
	spec := validWorkflowGraphSpec()
	spec.Nodes[0].DependsOn = []string{"passage:join"}
	err := validateWorkflowGraph(spec)
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestValidateWorkflowGraphRejectsUnsafeDeterministicContract(t *testing.T) {
	spec := validWorkflowGraphSpec()
	spec.Nodes[2].OutputContract = json.RawMessage(
		`{"operation":"repository_command_v1","command":[],"allowed_paths":[]}`,
	)
	err := validateWorkflowGraph(spec)
	if err == nil || !strings.Contains(err.Error(), "command argv") {
		t.Fatalf("deterministic contract error = %v", err)
	}
}

func TestValidateWorkflowGraphRequiresDependencyClosedImport(t *testing.T) {
	spec := validWorkflowGraphSpec()
	spec.ImportedCheckpoints = []WorkflowImportedCheckpointSpec{
		{
			NodeKey:         "passage:join",
			CanonicalCommit: "abc",
			ArtifactDigest:  "sha256:123",
		},
	}
	err := validateWorkflowGraph(spec)
	if err == nil || !strings.Contains(err.Error(), "dependency-closed") {
		t.Fatalf("import closure error = %v", err)
	}

	spec.ImportedCheckpoints = []WorkflowImportedCheckpointSpec{
		{NodeKey: "passage:07", CanonicalCommit: "abc", ArtifactDigest: "sha256:07"},
		{NodeKey: "passage:08", CanonicalCommit: "abc", ArtifactDigest: "sha256:08"},
		{NodeKey: "passage:join", CanonicalCommit: "abc", ArtifactDigest: "sha256:join"},
	}
	if err := validateWorkflowGraph(spec); err != nil {
		t.Fatalf("dependency-closed import rejected: %v", err)
	}
}
