package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	"github.com/multica-ai/multica/server/internal/cli"
)

// dagCmd drives the fork's pooled DAG workflow runs (ADR-0032). It was
// `multica workflow` until upstream claimed that name for Project Workflows
// (issue statuses and handoffs); the API paths under /workflow-runs are
// unchanged.
var dagCmd = &cobra.Command{
	Use:   "dag",
	Short: "Inspect and control pooled DAG workflow runs",
}

var dagListCmd = &cobra.Command{
	Use:   "list",
	Short: "List workflow runs in the workspace",
	RunE:  runDagList,
}

var dagGetCmd = &cobra.Command{
	Use:   "get <run-id>",
	Short: "Inspect a workflow run, nodes, attempts, resources, and results",
	Args:  exactArgs(1),
	RunE:  runDagGet,
}

var dagCreateCmd = &cobra.Command{
	Use:   "create --file <graph.json>",
	Short: "Create a versioned durable workflow run from a graph specification",
	Args:  cobra.NoArgs,
	RunE:  runDagCreate,
}

var dagGateCompleteCmd = &cobra.Command{
	Use:   "gate-complete <run-id> <node-id>",
	Short: "Record a human gate verdict and release ready successors",
	Args:  exactArgs(2),
	RunE:  runDagGateComplete,
}

var dagPauseCmd = &cobra.Command{
	Use:   "pause <run-id>",
	Short: "Pause new claims for a workflow run",
	Args:  exactArgs(1),
	RunE:  runDagPause,
}

var dagResumeCmd = &cobra.Command{
	Use:   "resume <run-id>",
	Short: "Resume claims for a paused workflow run",
	Args:  exactArgs(1),
	RunE:  runDagResume,
}

var dagCancelRunCmd = &cobra.Command{
	Use:   "cancel-run <run-id>",
	Short: "Fence and cancel an entire workflow run",
	Args:  exactArgs(1),
	RunE:  runDagCancelRun,
}

var dagRetryCmd = &cobra.Command{
	Use:   "retry <run-id> <node-id>",
	Short: "Audit and retry a blocked or failed workflow node",
	Args:  exactArgs(2),
	RunE:  runDagRetry,
}

var dagCancelCmd = &cobra.Command{
	Use:   "cancel <run-id> <node-id>",
	Short: "Audit and cancel a workflow node",
	Args:  exactArgs(2),
	RunE:  runDagCancel,
}

var runtimePoolCmd = &cobra.Command{
	Use:   "pool",
	Short: "Inspect runtime pools",
}

var runtimePoolListCmd = &cobra.Command{
	Use:   "list",
	Short: "List runtime pools and their runtime members",
	RunE:  runRuntimePoolList,
}

var runtimePoolCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a runtime pool",
	Args:  exactArgs(1),
	RunE:  runRuntimePoolCreate,
}

var runtimePoolAddRuntimeCmd = &cobra.Command{
	Use:   "add-runtime <pool-id> <runtime-id>",
	Short: "Add an eligible runtime to a pool",
	Args:  exactArgs(2),
	RunE:  runRuntimePoolAddRuntime,
}

var runtimePoolPruneStaleRuntimeCmd = &cobra.Command{
	Use:   "prune-stale-runtime <pool-id> <runtime-id>",
	Short: "Remove a pool membership whose runtime no longer exists",
	Args:  exactArgs(2),
	RunE:  runRuntimePoolPruneStaleRuntime,
}

var runtimePoolBindAgentCmd = &cobra.Command{
	Use:   "bind-agent <pool-id> <agent-id>",
	Short: "Opt an agent into graph dispatch through a runtime pool",
	Args:  exactArgs(2),
	RunE:  runRuntimePoolBindAgent,
}

func init() {
	dagCmd.AddCommand(dagListCmd)
	dagCmd.AddCommand(dagGetCmd)
	dagCmd.AddCommand(dagCreateCmd)
	dagCmd.AddCommand(dagGateCompleteCmd)
	dagCmd.AddCommand(dagPauseCmd)
	dagCmd.AddCommand(dagResumeCmd)
	dagCmd.AddCommand(dagCancelRunCmd)
	dagCmd.AddCommand(dagRetryCmd)
	dagCmd.AddCommand(dagCancelCmd)
	dagListCmd.Flags().String("output", "table", "Output format: table or json")
	dagGetCmd.Flags().String("output", "json", "Output format: json")
	dagCreateCmd.Flags().String("file", "", "Path to a workflow graph JSON document")
	_ = dagCreateCmd.MarkFlagRequired("file")
	dagGateCompleteCmd.Flags().String("comment-id", "", "Durable issue comment containing the accepted gate control")
	dagGateCompleteCmd.Flags().String("verdict", "", "Accepted gate verdict: approved or auto_promoted")
	_ = dagGateCompleteCmd.MarkFlagRequired("comment-id")
	_ = dagGateCompleteCmd.MarkFlagRequired("verdict")
	dagRetryCmd.Flags().String("input-digest", "", "Replacement input digest for the new generation")
	dagRetryCmd.Flags().String("law-digest", "", "Replacement editorial-law digest for the new generation")

	runtimePoolCmd.AddCommand(runtimePoolListCmd)
	runtimePoolCmd.AddCommand(runtimePoolCreateCmd)
	runtimePoolCmd.AddCommand(runtimePoolAddRuntimeCmd)
	runtimePoolCmd.AddCommand(runtimePoolPruneStaleRuntimeCmd)
	runtimePoolCmd.AddCommand(runtimePoolBindAgentCmd)
	runtimePoolListCmd.Flags().String("output", "table", "Output format: table or json")
	runtimePoolCreateCmd.Flags().Int32("max-inflight", 1, "Maximum active nodes in the pool")
	runtimePoolCreateCmd.Flags().Int32("affinity-grace-seconds", 60, "Passage affinity period before stealing")
	runtimePoolCreateCmd.Flags().Int32("lease-seconds", 90, "Node claim lease duration")
	runtimePoolAddRuntimeCmd.Flags().Int32("priority", 0, "Runtime selection priority")
	runtimeCmd.AddCommand(runtimePoolCmd)
}

func dagAPIClient(cmd *cobra.Command) (*cli.APIClient, error) {
	client, err := newAPIClient(cmd)
	if err != nil {
		return nil, err
	}
	if client.WorkspaceID == "" {
		return nil, fmt.Errorf("workspace ID is required")
	}
	return client, nil
}

func runDagList(cmd *cobra.Command, _ []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var runs []map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) + "/workflow-runs"
	if err := client.GetJSON(ctx, path, &runs); err != nil {
		return fmt.Errorf("list workflow runs: %w", err)
	}
	output, _ := cmd.Flags().GetString("output")
	if output == "json" {
		return cli.PrintJSON(os.Stdout, runs)
	}
	rows := make([][]string, 0, len(runs))
	for _, run := range runs {
		rows = append(rows, []string{
			strVal(run, "id"),
			strVal(run, "graph_key"),
			strVal(run, "graph_version"),
			strVal(run, "status"),
			strVal(run, "wip_limit"),
			strVal(run, "human_gate_limit"),
		})
	}
	cli.PrintTable(os.Stdout, []string{"ID", "GRAPH", "VERSION", "STATUS", "WIP", "GATE_LIMIT"}, rows)
	return nil
}

func runDagGet(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var run map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/workflow-runs/" + url.PathEscape(args[0])
	if err := client.GetJSON(ctx, path, &run); err != nil {
		return fmt.Errorf("get workflow run: %w", err)
	}
	return cli.PrintJSON(os.Stdout, run)
}

func runDagCreate(cmd *cobra.Command, _ []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	path, _ := cmd.Flags().GetString("file")
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read workflow graph: %w", err)
	}
	var body map[string]any
	if err := json.Unmarshal(content, &body); err != nil {
		return fmt.Errorf("parse workflow graph: %w", err)
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var run map[string]any
	apiPath := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) + "/workflow-runs"
	if err := client.PostJSON(ctx, apiPath, body, &run); err != nil {
		return fmt.Errorf("create workflow run: %w", err)
	}
	return cli.PrintJSON(os.Stdout, run)
}

func dagNodeCommandPath(client *cli.APIClient, runID, nodeID, command string) string {
	return "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/workflow-runs/" + url.PathEscape(runID) +
		"/nodes/" + url.PathEscape(nodeID) + "/" + command
}

func dagRunCommandPath(client *cli.APIClient, runID, command string) string {
	return "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/workflow-runs/" + url.PathEscape(runID) + "/" + command
}

func runDagGateComplete(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	commentID, _ := cmd.Flags().GetString("comment-id")
	verdict, _ := cmd.Flags().GetString("verdict")
	body := map[string]any{"comment_id": commentID, "verdict": verdict}
	if err := client.PostJSON(ctx, dagNodeCommandPath(client, args[0], args[1], "gate-complete"), body, &node); err != nil {
		return fmt.Errorf("complete workflow gate: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runDagRunCommand(cmd *cobra.Command, runID, command string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var run map[string]any
	if err := client.PostJSON(
		ctx,
		dagRunCommandPath(client, runID, command),
		map[string]any{},
		&run,
	); err != nil {
		return fmt.Errorf("%s workflow run: %w", command, err)
	}
	return cli.PrintJSON(os.Stdout, run)
}

func runDagPause(cmd *cobra.Command, args []string) error {
	return runDagRunCommand(cmd, args[0], "pause")
}

func runDagResume(cmd *cobra.Command, args []string) error {
	return runDagRunCommand(cmd, args[0], "resume")
}

func runDagCancelRun(cmd *cobra.Command, args []string) error {
	return runDagRunCommand(cmd, args[0], "cancel")
}

func runDagRetry(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	inputDigest, _ := cmd.Flags().GetString("input-digest")
	lawDigest, _ := cmd.Flags().GetString("law-digest")
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	body := map[string]any{"input_digest": inputDigest, "law_digest": lawDigest}
	if err := client.PostJSON(ctx, dagNodeCommandPath(client, args[0], args[1], "retry"), body, &node); err != nil {
		return fmt.Errorf("retry workflow node: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runDagCancel(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	if err := client.PostJSON(ctx, dagNodeCommandPath(client, args[0], args[1], "cancel"), map[string]any{}, &node); err != nil {
		return fmt.Errorf("cancel workflow node: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runRuntimePoolList(cmd *cobra.Command, _ []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var pools []map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) + "/runtime-pools"
	if err := client.GetJSON(ctx, path, &pools); err != nil {
		return fmt.Errorf("list runtime pools: %w", err)
	}
	output, _ := cmd.Flags().GetString("output")
	if output == "json" {
		return cli.PrintJSON(os.Stdout, pools)
	}
	rows := make([][]string, 0, len(pools))
	for _, pool := range pools {
		memberCount := "0"
		if runtimes, ok := pool["runtimes"].([]any); ok {
			memberCount = fmt.Sprintf("%d", len(runtimes))
		}
		rows = append(rows, []string{
			strVal(pool, "id"),
			strVal(pool, "name"),
			strVal(pool, "enabled"),
			strVal(pool, "max_inflight"),
			strVal(pool, "affinity_grace_seconds"),
			strVal(pool, "lease_seconds"),
			memberCount,
		})
	}
	cli.PrintTable(
		os.Stdout,
		[]string{"ID", "NAME", "ENABLED", "MAX", "AFFINITY_S", "LEASE_S", "RUNTIMES"},
		rows,
	)
	return nil
}

func runRuntimePoolCreate(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	maxInflight, _ := cmd.Flags().GetInt32("max-inflight")
	affinity, _ := cmd.Flags().GetInt32("affinity-grace-seconds")
	lease, _ := cmd.Flags().GetInt32("lease-seconds")
	body := map[string]any{
		"name":                   args[0],
		"max_inflight":           maxInflight,
		"affinity_grace_seconds": affinity,
		"lease_seconds":          lease,
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var pool map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) + "/runtime-pools"
	if err := client.PostJSON(ctx, path, body, &pool); err != nil {
		return fmt.Errorf("create runtime pool: %w", err)
	}
	return cli.PrintJSON(os.Stdout, pool)
}

func runRuntimePoolAddRuntime(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	priority, _ := cmd.Flags().GetInt32("priority")
	body := map[string]any{"runtime_id": args[1], "priority": priority}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var member map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/runtime-pools/" + url.PathEscape(args[0]) + "/runtimes"
	if err := client.PostJSON(ctx, path, body, &member); err != nil {
		return fmt.Errorf("add runtime to pool: %w", err)
	}
	return cli.PrintJSON(os.Stdout, member)
}

func runRuntimePoolPruneStaleRuntime(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var result map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/runtime-pools/" + url.PathEscape(args[0]) + "/runtimes/" + url.PathEscape(args[1])
	if err := client.DeleteJSONResponse(ctx, path, &result); err != nil {
		return fmt.Errorf("prune stale runtime from pool: %w", err)
	}
	return cli.PrintJSON(os.Stdout, result)
}

func runRuntimePoolBindAgent(cmd *cobra.Command, args []string) error {
	client, err := dagAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var binding map[string]any
	path := "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/runtime-pools/" + url.PathEscape(args[0]) + "/agents"
	if err := client.PostJSON(ctx, path, map[string]any{"agent_id": args[1]}, &binding); err != nil {
		return fmt.Errorf("bind agent to runtime pool: %w", err)
	}
	return cli.PrintJSON(os.Stdout, binding)
}
