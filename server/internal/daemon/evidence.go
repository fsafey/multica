package daemon

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/multica-ai/multica/server/pkg/executionevidence"
)

type executionSnapshotParams struct {
	Task                  Task
	Provider              string
	InvocationModel       string
	InvocationModelSource string
	ProviderCLIVersion    string
	MulticaCLIVersion     string
	MulticaGitCommit      string
	Instructions          string
	Skills                []SkillData
	ThinkingLevel         string
	CustomArguments       []string
	CustomEnvironment     map[string]string
	MCPConfiguration      json.RawMessage
	SessionRequested      bool
	RequestedSessionID    string
	WorkdirRequested      bool
	WorkdirReused         bool
}

func resolveInvocationModel(provider, model, source string, extraArgs, customArgs []string) (string, string) {
	if provider == "codex" {
		// An explicit thread model is sent over JSON-RPC and wins over app-server
		// config. When it is absent, Codex -c/--config model assignments select
		// the provider model before the thread starts.
		if model != "" {
			return model, source
		}
		if override, ok := codexModelOverride(extraArgs); ok {
			model, source = override, "runtime_arguments"
		}
		if override, ok := codexModelOverride(customArgs); ok {
			model, source = override, "custom_arguments"
		}
		return model, source
	}

	// These backends append filtered runtime arguments and then agent custom
	// arguments after the daemon-managed --model flag, so their last surviving
	// --model value is the effective CLI selection. Providers that reserve or
	// reject --model are deliberately excluded.
	switch provider {
	case "claude", "codebuddy", "cursor", "copilot", "opencode", "pi", "deveco":
		if override, ok := cliModelOverride(extraArgs); ok {
			model, source = override, "runtime_arguments"
		}
		if override, ok := cliModelOverride(customArgs); ok {
			model, source = override, "custom_arguments"
		}
	}
	return model, source
}

func cliModelOverride(args []string) (string, bool) {
	model := ""
	found := false
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch {
		case arg == "--model" && index+1 < len(args):
			candidate := trimArgumentValue(args[index+1])
			if candidate != "" && !strings.HasPrefix(candidate, "-") {
				model, found = candidate, true
			}
			index++
		case strings.HasPrefix(arg, "--model="):
			if candidate := trimArgumentValue(strings.TrimPrefix(arg, "--model=")); candidate != "" {
				model, found = candidate, true
			}
		}
	}
	return model, found
}

func codexModelOverride(args []string) (string, bool) {
	model := ""
	found := false
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		assignment := ""
		switch {
		case (arg == "-c" || arg == "--config") && index+1 < len(args):
			assignment = args[index+1]
			index++
		case strings.HasPrefix(arg, "-c="):
			assignment = strings.TrimPrefix(arg, "-c=")
		case strings.HasPrefix(arg, "--config="):
			assignment = strings.TrimPrefix(arg, "--config=")
		}
		key, value, ok := strings.Cut(assignment, "=")
		if ok && strings.TrimSpace(key) == "model" {
			if candidate := trimArgumentValue(value); candidate != "" {
				model, found = candidate, true
			}
		}
	}
	return model, found
}

func trimArgumentValue(value string) string {
	return strings.Trim(strings.TrimSpace(value), "\"'")
}

func buildExecutionSnapshot(params executionSnapshotParams) (executionevidence.Snapshot, error) {
	mountedSkills := make([]executionevidence.MountedSkill, 0, len(params.Skills))
	for _, skill := range params.Skills {
		hash := skill.Hash
		if hash == "" {
			hash = executionevidence.HashText(skill.Content)
		}
		mountedSkills = append(mountedSkills, executionevidence.MountedSkill{
			Name:        skill.Name,
			ContentHash: hash,
		})
	}
	sort.Slice(mountedSkills, func(i, j int) bool {
		if mountedSkills[i].Name == mountedSkills[j].Name {
			return mountedSkills[i].ContentHash < mountedSkills[j].ContentHash
		}
		return mountedSkills[i].Name < mountedSkills[j].Name
	})

	mcpNames, mcpHash, err := executionevidence.MCPMetadata(params.MCPConfiguration)
	if err != nil {
		return executionevidence.Snapshot{}, err
	}

	return executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 params.Task.ID,
		Provider:               params.Provider,
		InvocationModel:        params.InvocationModel,
		InvocationModelSource:  params.InvocationModelSource,
		ProviderCLIVersion:     params.ProviderCLIVersion,
		MulticaCLIVersion:      params.MulticaCLIVersion,
		MulticaGitCommit:       params.MulticaGitCommit,
		AgentID:                params.Task.AgentID,
		RuntimeID:              params.Task.RuntimeID,
		WorkspaceID:            params.Task.WorkspaceID,
		ProjectID:              params.Task.ProjectID,
		Instructions:           params.Instructions,
		WorkspaceContext:       params.Task.WorkspaceContext,
		MountedSkills:          mountedSkills,
		ThinkingLevel:          params.ThinkingLevel,
		CustomArguments:        executionevidence.SanitizeArguments(params.CustomArguments),
		CustomEnvironmentNames: executionevidence.EnvironmentNames(params.CustomEnvironment),
		MCPServerNames:         mcpNames,
		MCPConfigurationHash:   mcpHash,
		SessionResume: executionevidence.SessionResumeDecision{
			Requested:      params.SessionRequested,
			Selected:       params.Task.PriorSessionID != "",
			PriorSessionID: params.RequestedSessionID,
		},
		WorkdirReuse: executionevidence.WorkdirReuseDecision{
			Requested: params.WorkdirRequested,
			Selected:  params.WorkdirReused,
		},
	}, nil
}
