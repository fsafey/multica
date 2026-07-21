// Package executionevidence defines the provider-neutral, immutable snapshot
// recorded immediately before a Multica daemon launches an agent provider.
package executionevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/multica-ai/multica/server/pkg/redact"
)

const (
	CurrentSchemaVersion = 1
	RedactedValue        = "[REDACTED]"
	RedactedPath         = "[REDACTED PATH]"
)

type MountedSkill struct {
	Name        string `json:"name"`
	ContentHash string `json:"content_hash"`
}

type SessionResumeDecision struct {
	Requested      bool   `json:"requested"`
	Selected       bool   `json:"selected"`
	PriorSessionID string `json:"prior_session_id,omitempty"`
}

type WorkdirReuseDecision struct {
	Requested bool `json:"requested"`
	Selected  bool `json:"selected"`
}

// Snapshot contains only claim-time configuration and daemon decisions. It
// deliberately excludes provider credentials, custom environment values, raw
// MCP configuration, and the absolute workdir path.
type Snapshot struct {
	SchemaVersion          int                   `json:"schema_version"`
	TaskID                 string                `json:"task_id"`
	Provider               string                `json:"provider"`
	InvocationModel        string                `json:"invocation_model,omitempty"`
	InvocationModelSource  string                `json:"invocation_model_source"`
	ProviderCLIVersion     string                `json:"provider_cli_version,omitempty"`
	MulticaCLIVersion      string                `json:"multica_cli_version"`
	MulticaGitCommit       string                `json:"multica_git_commit"`
	AgentID                string                `json:"agent_id"`
	RuntimeID              string                `json:"runtime_id"`
	WorkspaceID            string                `json:"workspace_id"`
	ProjectID              string                `json:"project_id,omitempty"`
	Instructions           string                `json:"instructions"`
	WorkspaceContext       string                `json:"workspace_context"`
	MountedSkills          []MountedSkill        `json:"mounted_skills"`
	ThinkingLevel          string                `json:"thinking_level,omitempty"`
	CustomArguments        []string              `json:"custom_arguments"`
	CustomEnvironmentNames []string              `json:"custom_environment_names"`
	MCPServerNames         []string              `json:"mcp_server_names"`
	MCPConfigurationHash   string                `json:"mcp_configuration_hash,omitempty"`
	SessionResume          SessionResumeDecision `json:"session_resume"`
	WorkdirReuse           WorkdirReuseDecision  `json:"workdir_reuse"`
}

var fullGitCommit = regexp.MustCompile(`^[0-9a-f]{40}$`)

// CompletenessIssues lists claim-time facts that were unavailable rather than
// treating provider defaults or unstamped development binaries as resolved
// evidence. The snapshot is still persisted so the missing fact is explicit.
func (s Snapshot) CompletenessIssues() []string {
	issues := []string{}
	for name, value := range map[string]string{
		"task_id":                 s.TaskID,
		"provider":                s.Provider,
		"invocation_model":        s.InvocationModel,
		"invocation_model_source": s.InvocationModelSource,
		"provider_cli_version":    s.ProviderCLIVersion,
		"multica_cli_version":     s.MulticaCLIVersion,
		"agent_id":                s.AgentID,
		"runtime_id":              s.RuntimeID,
		"workspace_id":            s.WorkspaceID,
	} {
		if strings.TrimSpace(value) == "" || strings.EqualFold(strings.TrimSpace(value), "unknown") {
			issues = append(issues, name)
		}
	}
	if !fullGitCommit.MatchString(strings.TrimSpace(s.MulticaGitCommit)) {
		issues = append(issues, "multica_git_commit")
	}
	sort.Strings(issues)
	return issues
}

func (s Snapshot) CanonicalPayload() ([]byte, error) {
	if s.SchemaVersion <= 0 || s.SchemaVersion > CurrentSchemaVersion {
		return nil, fmt.Errorf("unsupported execution evidence schema version %d", s.SchemaVersion)
	}
	payload, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return CanonicalizePayload(payload)
}

func (s Snapshot) Digest() (string, error) {
	payload, err := s.CanonicalPayload()
	if err != nil {
		return "", err
	}
	return HashBytes(payload), nil
}

// CanonicalizePayload normalizes an arbitrary JSON payload without projecting
// it through the current Snapshot struct. Integrity checks therefore retain
// fields added by future evidence schema versions.
func CanonicalizePayload(payload []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode execution evidence payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode execution evidence payload: multiple JSON values")
		}
		return nil, fmt.Errorf("decode execution evidence payload: %w", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode execution evidence payload: %w", err)
	}
	return canonical, nil
}

func HashBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func HashText(value string) string {
	return HashBytes([]byte(value))
}

func EnvironmentNames(values map[string]string) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SanitizeArguments preserves operational flags while removing known secret
// values and private filesystem paths. It is intentionally conservative: a
// flag whose name implies credentials causes its following value to be masked.
func SanitizeArguments(args []string) []string {
	out := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := args[index]
		name, value, hasValue := strings.Cut(arg, "=")
		if normalizeArgumentName(name) == "config" {
			if hasValue {
				out = append(out, name+"="+sanitizeConfigArgument(value))
			} else {
				out = append(out, arg)
				if index+1 < len(args) && !strings.HasPrefix(args[index+1], "-") {
					out = append(out, sanitizeConfigArgument(args[index+1]))
					index++
				}
			}
			continue
		}
		if sensitiveArgumentName(name) {
			if hasValue {
				out = append(out, name+"="+RedactedValue)
			} else {
				out = append(out, arg)
				if index+1 < len(args) && !strings.HasPrefix(args[index+1], "-") {
					out = append(out, RedactedValue)
					index++
				}
			}
			continue
		}
		if pathArgumentName(name) {
			if hasValue {
				out = append(out, name+"="+RedactedPath)
			} else {
				out = append(out, arg)
				if index+1 < len(args) && !strings.HasPrefix(args[index+1], "-") {
					out = append(out, RedactedPath)
					index++
				}
			}
			continue
		}
		if hasValue && filepath.IsAbs(value) {
			out = append(out, name+"="+RedactedPath)
			continue
		}
		if hasValue {
			out = append(out, name+"="+redact.Text(sanitizeURLArgument(value)))
			continue
		}
		if filepath.IsAbs(arg) {
			out = append(out, RedactedPath)
			continue
		}
		out = append(out, redact.Text(sanitizeURLArgument(arg)))
	}
	return out
}

func sanitizeURLArgument(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return value
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

func sanitizeConfigArgument(value string) string {
	if filepath.IsAbs(value) || (!strings.Contains(value, "=") && strings.ContainsAny(value, `/\`)) {
		return RedactedPath
	}
	return redact.Text(sanitizeURLArgument(value))
}

func sensitiveArgumentName(value string) bool {
	name := normalizeArgumentName(value)
	for _, part := range []string{"api-key", "apikey", "access-key", "private-key", "secret", "token", "password", "credential", "authorization", "auth-header"} {
		if strings.Contains(name, part) {
			return true
		}
	}
	return name == "env" || name == "e" || name == "header" || name == "h"
}

func pathArgumentName(value string) bool {
	switch normalizeArgumentName(value) {
	case "add-dir", "directory", "workdir", "work-dir", "cwd", "path", "config-file", "env-file":
		return true
	default:
		return false
	}
}

func normalizeArgumentName(value string) string {
	name := strings.ToLower(strings.TrimLeft(strings.TrimSpace(value), "-"))
	return strings.ReplaceAll(name, "_", "-")
}

// MCPMetadata returns stable server names and a canonical hash of the
// credential-redacted configuration. Raw configuration and credentials are
// never returned or incorporated into the digest.
func MCPMetadata(raw json.RawMessage) ([]string, string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return []string{}, "", nil
	}

	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, "", fmt.Errorf("decode MCP configuration: %w", err)
	}
	// Hash the effective configuration after recursively removing credential
	// values. This preserves structural and non-secret configuration drift
	// without turning the digest into an oracle over API keys or headers.
	canonical, err := json.Marshal(redactMCPValue(document, ""))
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize MCP configuration: %w", err)
	}

	names := []string{}
	for _, key := range []string{"mcpServers", "mcp_servers", "servers"} {
		servers, ok := document[key].(map[string]any)
		if !ok {
			continue
		}
		for name := range servers {
			names = append(names, name)
		}
		break
	}
	sort.Strings(names)
	return names, HashBytes(canonical), nil
}

func redactMCPValue(value any, parentKey string) any {
	switch item := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			if sensitiveMCPKey(key) || sensitiveMCPContainer(parentKey) {
				out[key] = RedactedValue
				continue
			}
			out[key] = redactMCPValue(child, key)
		}
		return out
	case []any:
		if strings.EqualFold(strings.TrimSpace(parentKey), "args") || strings.EqualFold(strings.TrimSpace(parentKey), "arguments") {
			args := make([]string, len(item))
			for i, child := range item {
				text, ok := child.(string)
				if !ok {
					return RedactedValue
				}
				args[i] = text
			}
			return SanitizeArguments(args)
		}
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = redactMCPValue(child, parentKey)
		}
		return out
	case string:
		if sensitiveMCPContainer(parentKey) {
			return RedactedValue
		}
		return redact.Text(item)
	default:
		return item
	}
}

func sensitiveMCPContainer(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "env", "headers", "environment":
		return true
	default:
		return false
	}
}

func sensitiveMCPKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), " ", "_"))
	for _, part := range []string{
		"secret", "token", "password", "credential", "authorization", "api_key", "apikey",
		"private_key", "privatekey", "access_key", "accesskey", "cookie", "auth",
	} {
		if strings.Contains(normalized, part) {
			return true
		}
	}
	return false
}

type SequenceGap struct {
	After  int32 `json:"after"`
	Before int32 `json:"before"`
}

type SequenceIntegrity struct {
	Valid              bool          `json:"valid"`
	StrictlyIncreasing bool          `json:"strictly_increasing"`
	MessageCount       int           `json:"message_count"`
	FirstSequence      int32         `json:"first_sequence,omitempty"`
	LastSequence       int32         `json:"last_sequence,omitempty"`
	DuplicateCount     int           `json:"duplicate_count"`
	OutOfOrderCount    int           `json:"out_of_order_count"`
	Gaps               []SequenceGap `json:"gaps"`
}

func CheckSequenceIntegrity(sequences []int32) SequenceIntegrity {
	result := SequenceIntegrity{
		Valid:              true,
		StrictlyIncreasing: true,
		MessageCount:       len(sequences),
		Gaps:               []SequenceGap{},
	}
	if len(sequences) == 0 {
		return result
	}
	result.FirstSequence = sequences[0]
	result.LastSequence = sequences[len(sequences)-1]
	if sequences[0] != 1 {
		result.Gaps = append(result.Gaps, SequenceGap{After: 0, Before: sequences[0]})
	}
	for i := 1; i < len(sequences); i++ {
		previous, current := sequences[i-1], sequences[i]
		switch {
		case current == previous:
			result.DuplicateCount++
			result.StrictlyIncreasing = false
		case current < previous:
			result.OutOfOrderCount++
			result.StrictlyIncreasing = false
		case current > previous+1:
			result.Gaps = append(result.Gaps, SequenceGap{After: previous, Before: current})
		}
	}
	result.Valid = result.StrictlyIncreasing && len(result.Gaps) == 0
	return result
}
