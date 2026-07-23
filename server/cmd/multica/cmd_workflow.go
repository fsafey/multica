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

var workflowCmd = &cobra.Command{
	Use:   "workflow",
	Short: "Inspect durable workflow runs",
}

var workflowListCmd = &cobra.Command{
	Use:   "list",
	Short: "List workflow runs in the workspace",
	RunE:  runWorkflowList,
}

var workflowGetCmd = &cobra.Command{
	Use:   "get <run-id>",
	Short: "Inspect a workflow run, nodes, attempts, resources, and results",
	Args:  exactArgs(1),
	RunE:  runWorkflowGet,
}

var workflowCreateCmd = &cobra.Command{
	Use:   "create --file <graph.json>",
	Short: "Create a versioned durable workflow run from a graph specification",
	Args:  cobra.NoArgs,
	RunE:  runWorkflowCreate,
}

var workflowGateCompleteCmd = &cobra.Command{
	Use:   "gate-complete <run-id> <node-id>",
	Short: "Record a human gate verdict and release ready successors",
	Args:  exactArgs(2),
	RunE:  runWorkflowGateComplete,
}

var workflowRetryCmd = &cobra.Command{
	Use:   "retry <run-id> <node-id>",
	Short: "Audit and retry a blocked or failed workflow node",
	Args:  exactArgs(2),
	RunE:  runWorkflowRetry,
}

var workflowCancelCmd = &cobra.Command{
	Use:   "cancel <run-id> <node-id>",
	Short: "Audit and cancel a workflow node",
	Args:  exactArgs(2),
	RunE:  runWorkflowCancel,
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

var runtimePoolBindAgentCmd = &cobra.Command{
	Use:   "bind-agent <pool-id> <agent-id>",
	Short: "Opt an agent into graph dispatch through a runtime pool",
	Args:  exactArgs(2),
	RunE:  runRuntimePoolBindAgent,
}

func init() {
	workflowCmd.AddCommand(workflowListCmd)
	workflowCmd.AddCommand(workflowGetCmd)
	workflowCmd.AddCommand(workflowCreateCmd)
	workflowCmd.AddCommand(workflowGateCompleteCmd)
	workflowCmd.AddCommand(workflowRetryCmd)
	workflowCmd.AddCommand(workflowCancelCmd)
	workflowListCmd.Flags().String("output", "table", "Output format: table or json")
	workflowGetCmd.Flags().String("output", "json", "Output format: json")
	workflowCreateCmd.Flags().String("file", "", "Path to a workflow graph JSON document")
	_ = workflowCreateCmd.MarkFlagRequired("file")
	workflowRetryCmd.Flags().String("input-digest", "", "Replacement input digest for the new generation")
	workflowRetryCmd.Flags().String("law-digest", "", "Replacement editorial-law digest for the new generation")

	runtimePoolCmd.AddCommand(runtimePoolListCmd)
	runtimePoolCmd.AddCommand(runtimePoolCreateCmd)
	runtimePoolCmd.AddCommand(runtimePoolAddRuntimeCmd)
	runtimePoolCmd.AddCommand(runtimePoolBindAgentCmd)
	runtimePoolListCmd.Flags().String("output", "table", "Output format: table or json")
	runtimePoolCreateCmd.Flags().Int32("max-inflight", 1, "Maximum active nodes in the pool")
	runtimePoolCreateCmd.Flags().Int32("affinity-grace-seconds", 60, "Passage affinity period before stealing")
	runtimePoolCreateCmd.Flags().Int32("lease-seconds", 90, "Node claim lease duration")
	runtimePoolAddRuntimeCmd.Flags().Int32("priority", 0, "Runtime selection priority")
	runtimeCmd.AddCommand(runtimePoolCmd)
}

func workflowAPIClient(cmd *cobra.Command) (*cli.APIClient, error) {
	client, err := newAPIClient(cmd)
	if err != nil {
		return nil, err
	}
	if client.WorkspaceID == "" {
		return nil, fmt.Errorf("workspace ID is required")
	}
	return client, nil
}

func runWorkflowList(cmd *cobra.Command, _ []string) error {
	client, err := workflowAPIClient(cmd)
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

func runWorkflowGet(cmd *cobra.Command, args []string) error {
	client, err := workflowAPIClient(cmd)
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

func runWorkflowCreate(cmd *cobra.Command, _ []string) error {
	client, err := workflowAPIClient(cmd)
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

func workflowNodeCommandPath(client *cli.APIClient, runID, nodeID, command string) string {
	return "/api/workspaces/" + url.PathEscape(client.WorkspaceID) +
		"/workflow-runs/" + url.PathEscape(runID) +
		"/nodes/" + url.PathEscape(nodeID) + "/" + command
}

func runWorkflowGateComplete(cmd *cobra.Command, args []string) error {
	client, err := workflowAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	if err := client.PostJSON(ctx, workflowNodeCommandPath(client, args[0], args[1], "gate-complete"), map[string]any{}, &node); err != nil {
		return fmt.Errorf("complete workflow gate: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runWorkflowRetry(cmd *cobra.Command, args []string) error {
	client, err := workflowAPIClient(cmd)
	if err != nil {
		return err
	}
	inputDigest, _ := cmd.Flags().GetString("input-digest")
	lawDigest, _ := cmd.Flags().GetString("law-digest")
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	body := map[string]any{"input_digest": inputDigest, "law_digest": lawDigest}
	if err := client.PostJSON(ctx, workflowNodeCommandPath(client, args[0], args[1], "retry"), body, &node); err != nil {
		return fmt.Errorf("retry workflow node: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runWorkflowCancel(cmd *cobra.Command, args []string) error {
	client, err := workflowAPIClient(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := cli.APIContext(context.Background())
	defer cancel()
	var node map[string]any
	if err := client.PostJSON(ctx, workflowNodeCommandPath(client, args[0], args[1], "cancel"), map[string]any{}, &node); err != nil {
		return fmt.Errorf("cancel workflow node: %w", err)
	}
	return cli.PrintJSON(os.Stdout, node)
}

func runRuntimePoolList(cmd *cobra.Command, _ []string) error {
	client, err := workflowAPIClient(cmd)
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
	client, err := workflowAPIClient(cmd)
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
	client, err := workflowAPIClient(cmd)
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

func runRuntimePoolBindAgent(cmd *cobra.Command, args []string) error {
	client, err := workflowAPIClient(cmd)
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
