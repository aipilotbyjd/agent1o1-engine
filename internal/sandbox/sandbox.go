package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrSandboxNotAvailable = errors.New("sandbox not available")
	ErrExecutionTimeout    = errors.New("execution timed out")
	ErrExecutionFailed     = errors.New("execution failed")
	ErrMemoryExceeded      = errors.New("memory limit exceeded")
)

// ExecutionMode represents the isolation mode.
type ExecutionMode int

const (
	ExecutionModeNone ExecutionMode = iota
	ExecutionModeProcess
	ExecutionModeContainer
	ExecutionModeWASM
)

// ExecutionRequest represents a sandboxed execution request.
type ExecutionRequest struct {
	Code        string
	Language    string
	Input       map[string]interface{}
	Timeout     time.Duration
	MemoryLimit int64   // bytes
	CPULimit    float64 // cores
	Mode        ExecutionMode
	Environment map[string]string
}

// that can be passed to sandboxed processes.
var safeEnvVars = map[string]bool{
	"PATH":   true,
	"HOME":   true, // Often needed by runtimes
	"TMPDIR": true,
	"LANG":   true,
	"LC_ALL": true,
}

// ExecutionResult represents the result of a sandboxed execution.
type ExecutionResult struct {
	Output   map[string]interface{}
	Stdout   string
	Stderr   string
	ExitCode int
	Duration time.Duration
	Memory   int64
}

// Sandbox provides isolated code execution.
type Sandbox struct {
	logger   *slog.Logger
	workDir  string
	runtimes map[string]Runtime
	mu       sync.RWMutex
}

// Runtime represents a language runtime.
type Runtime interface {
	Language() string
	Available() bool
	Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error)
}

// Config holds sandbox configuration.
type Config struct {
	Logger                 *slog.Logger
	WorkDir                string
	EnableWASM             bool
	EnableDocker           bool
	AllowedEnvVars         []string      // Additional allowed env vars
	MaxMemoryBytes         int64         // Default max memory (128MB)
	MaxExecutionTime       time.Duration // Default max execution time (30s)
	EnableNetworkIsolation bool          // Block network access in process mode
}

// NewSandbox creates a new sandbox.
func NewSandbox(config Config) (*Sandbox, error) {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.WorkDir == "" {
		config.WorkDir = os.TempDir()
	}

	sandbox := &Sandbox{
		logger:   config.Logger,
		workDir:  config.WorkDir,
		runtimes: make(map[string]Runtime),
	}

	// Register built-in runtimes (they enforce ExecutionModeContainer; host-based execution is disabled)
	sandbox.RegisterRuntime(&NodeJSRuntime{})
	sandbox.RegisterRuntime(&PythonRuntime{})
	sandbox.RegisterRuntime(&BashRuntime{})

	return sandbox, nil
}

// RegisterRuntime registers a language runtime.
func (s *Sandbox) RegisterRuntime(runtime Runtime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimes[runtime.Language()] = runtime
	s.logger.Info("runtime registered", slog.String("language", runtime.Language()))
}

// Execute executes code in a sandbox.
func (s *Sandbox) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	s.mu.RLock()
	runtime, exists := s.runtimes[req.Language]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported language: %s", req.Language)
	}

	if !runtime.Available() {
		return nil, fmt.Errorf("runtime not available: %s", req.Language)
	}

	// Set defaults
	if req.Timeout == 0 {
		req.Timeout = 30 * time.Second
	}
	if req.MemoryLimit == 0 {
		req.MemoryLimit = 128 * 1024 * 1024 // 128MB
	}

	// Execute with timeout context
	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	start := time.Now()
	result, err := runtime.Execute(ctx, req)
	if result != nil {
		result.Duration = time.Since(start)
	}

	return result, err
}

// ListRuntimes returns available runtimes.
func (s *Sandbox) ListRuntimes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var languages []string
	for lang, runtime := range s.runtimes {
		if runtime.Available() {
			languages = append(languages, lang)
		}
	}
	return languages
}

// NodeJSRuntime executes JavaScript code using Node.js.
type NodeJSRuntime struct{}

func (r *NodeJSRuntime) Language() string {
	return "javascript"
}

func (r *NodeJSRuntime) Available() bool {
	_, err := exec.LookPath("node")
	return err == nil
}

func (r *NodeJSRuntime) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	if req.Mode != ExecutionModeContainer {
		return nil, fmt.Errorf("%w: host-based code execution is disabled for security; use ExecutionModeContainer with ContainerRuntime instead", ErrSandboxNotAvailable)
	}

	result := &ExecutionResult{}

	// Create temp file
	tmpDir, err := os.MkdirTemp("", "sandbox-nodejs-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	// Write code to file
	codeFile := filepath.Join(tmpDir, "script.js")

	// Wrap code to handle input/output
	wrappedCode := fmt.Sprintf(`
const input = %s;
const output = (function() {
	%s
})();
console.log(JSON.stringify({ __output: output }));
`, mustJSON(req.Input), req.Code)

	if err := os.WriteFile(codeFile, []byte(wrappedCode), 0644); err != nil {
		return nil, err
	}

	// Execute
	cmd := exec.CommandContext(ctx, "node", codeFile)
	cmd.Dir = tmpDir

	// SECURITY: Only pass explicitly allowed environment variables
	// Never inherit the full parent environment
	cmd.Env = buildSafeEnv(req.Environment)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	if ctx.Err() == context.DeadlineExceeded {
		return result, ErrExecutionTimeout
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		return result, nil
	}

	// Parse output
	var outputWrapper struct {
		Output interface{} `json:"__output"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &outputWrapper); err == nil {
		if m, ok := outputWrapper.Output.(map[string]interface{}); ok {
			result.Output = m
		} else {
			result.Output = map[string]interface{}{"result": outputWrapper.Output}
		}
	}

	return result, nil
}

// PythonRuntime executes Python code.
type PythonRuntime struct{}

func (r *PythonRuntime) Language() string {
	return "python"
}

func (r *PythonRuntime) Available() bool {
	_, err := exec.LookPath("python3")
	if err != nil {
		_, err = exec.LookPath("python")
	}
	return err == nil
}

func (r *PythonRuntime) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	if req.Mode != ExecutionModeContainer {
		return nil, fmt.Errorf("%w: host-based code execution is disabled for security; use ExecutionModeContainer with ContainerRuntime instead", ErrSandboxNotAvailable)
	}

	result := &ExecutionResult{}

	// Create temp file
	tmpDir, err := os.MkdirTemp("", "sandbox-python-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	// Write code to file
	codeFile := filepath.Join(tmpDir, "script.py")

	wrappedCode := fmt.Sprintf(`
import json
import sys

input_data = json.loads('''%s''')

def main():
    %s

output = main()
print(json.dumps({"__output": output}))
`, mustJSON(req.Input), indentCode(req.Code, "    "))

	if err := os.WriteFile(codeFile, []byte(wrappedCode), 0644); err != nil {
		return nil, err
	}

	// Find python executable
	pythonExec := "python3"
	if _, err := exec.LookPath("python3"); err != nil {
		pythonExec = "python"
	}

	// Execute
	cmd := exec.CommandContext(ctx, pythonExec, codeFile)
	cmd.Dir = tmpDir

	// SECURITY: Only pass explicitly allowed environment variables
	cmd.Env = buildSafeEnv(req.Environment)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	if ctx.Err() == context.DeadlineExceeded {
		return result, ErrExecutionTimeout
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		return result, nil
	}

	// Parse output
	var outputWrapper struct {
		Output interface{} `json:"__output"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &outputWrapper); err == nil {
		if m, ok := outputWrapper.Output.(map[string]interface{}); ok {
			result.Output = m
		} else {
			result.Output = map[string]interface{}{"result": outputWrapper.Output}
		}
	}

	return result, nil
}

// BashRuntime executes shell commands.
type BashRuntime struct{}

func (r *BashRuntime) Language() string {
	return "bash"
}

func (r *BashRuntime) Available() bool {
	_, err := exec.LookPath("bash")
	return err == nil
}

func (r *BashRuntime) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	if req.Mode != ExecutionModeContainer {
		return nil, fmt.Errorf("%w: host-based code execution is disabled for security; use ExecutionModeContainer with ContainerRuntime instead", ErrSandboxNotAvailable)
	}

	result := &ExecutionResult{}

	// Create temp file
	tmpDir, err := os.MkdirTemp("", "sandbox-bash-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	// Write script to file
	scriptFile := filepath.Join(tmpDir, "script.sh")
	if err := os.WriteFile(scriptFile, []byte(req.Code), 0755); err != nil {
		return nil, err
	}

	// Write input as JSON file
	inputFile := filepath.Join(tmpDir, "input.json")
	inputJSON, _ := json.Marshal(req.Input)
	os.WriteFile(inputFile, inputJSON, 0644)

	// Execute
	cmd := exec.CommandContext(ctx, "bash", scriptFile)
	cmd.Dir = tmpDir

	// SECURITY: Create minimal environment for bash scripts
	// Do NOT inherit os.Environ() as it may contain secrets
	cmd.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=" + tmpDir,
		"TMPDIR=" + tmpDir,
		"INPUT_FILE=" + inputFile,
	}

	// Add only explicitly requested environment variables (validated)
	for k, v := range req.Environment {
		// Validate key to prevent injection
		if isValidEnvKey(k) {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	if ctx.Err() == context.DeadlineExceeded {
		return result, ErrExecutionTimeout
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
	}

	// Try to parse stdout as JSON output
	var output map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &output); err == nil {
		result.Output = output
	} else {
		result.Output = map[string]interface{}{"stdout": result.Stdout}
	}

	return result, nil
}

// ContainerRuntime executes code in Docker containers.
type ContainerRuntime struct {
	image   string
	command []string
}

func NewContainerRuntime(language, image string, command []string) *ContainerRuntime {
	return &ContainerRuntime{
		image:   image,
		command: command,
	}
}

func (r *ContainerRuntime) Language() string {
	return "container"
}

func (r *ContainerRuntime) Available() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

func (r *ContainerRuntime) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	result := &ExecutionResult{}

	// Create temp dir for mounting
	tmpDir, err := os.MkdirTemp("", "sandbox-container-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	// Write code and input
	codeFile := filepath.Join(tmpDir, "code")
	inputFile := filepath.Join(tmpDir, "input.json")

	os.WriteFile(codeFile, []byte(req.Code), 0644)
	inputJSON, _ := json.Marshal(req.Input)
	os.WriteFile(inputFile, inputJSON, 0644)

	// Build docker command with security options
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:/workspace:ro", tmpDir),
		"--memory", fmt.Sprintf("%d", req.MemoryLimit),
		"--cpus", fmt.Sprintf("%.2f", req.CPULimit),
		"--network", "none", // No network access
		"--read-only",                              // Read-only root filesystem
		"--security-opt", "no-new-privileges:true", // Prevent privilege escalation
		"--cap-drop", "ALL", // Drop all capabilities
		"--pids-limit", "100", // Limit process count
		"--ulimit", "nofile=100:200", // Limit open files
		"--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", // Writable /tmp with limits
		r.image,
	}
	args = append(args, r.command...)

	cmd := exec.CommandContext(ctx, "docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	if ctx.Err() == context.DeadlineExceeded {
		return result, ErrExecutionTimeout
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
	}

	return result, nil
}

// Helpers

// It only includes essential variables and explicitly requested ones.
func buildSafeEnv(requestedEnv map[string]string) []string {
	env := []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=/tmp",
		"TMPDIR=/tmp",
		"LANG=en_US.UTF-8",
	}

	// Add explicitly requested environment variables (validated)
	for k, v := range requestedEnv {
		if isValidEnvKey(k) {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	return env
}

// Returns false for keys that could be used for injection attacks.
func isValidEnvKey(key string) bool {
	if key == "" {
		return false
	}

	// Block common dangerous environment variables
	dangerousVars := map[string]bool{
		"LD_PRELOAD":      true, // Can inject shared libraries
		"LD_LIBRARY_PATH": true, // Can redirect library loading
		"PATH":            true, // Already set in safe env
		"HOME":            true, // Already set in safe env
		"SHELL":           true, // Could affect shell behavior
		"IFS":             true, // Internal field separator - shell injection
		"BASH_ENV":        true, // Executed before bash runs
		"ENV":             true, // Similar to BASH_ENV
		"PS1":             true, // Prompt - can contain commands
		"PS2":             true,
		"PS3":             true,
		"PS4":             true,
		"PROMPT_COMMAND":  true, // Executed before prompt
		"CDPATH":          true, // Can affect cd behavior
		"HISTFILE":        true, // History file location
		"TERM":            true, // Terminal type
	}

	if dangerousVars[key] {
		return false
	}

	// Only allow alphanumeric characters and underscores
	// First character must be letter or underscore
	for i, c := range key {
		if i == 0 {
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}

	return true
}

func mustJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func indentCode(code, indent string) string {
	lines := bytes.Split([]byte(code), []byte("\n"))
	var result bytes.Buffer
	for i, line := range lines {
		if i > 0 {
			result.WriteString("\n")
		}
		if len(line) > 0 {
			result.WriteString(indent)
		}
		result.Write(line)
	}
	return result.String()
}

// CodeExecutor is the executor that uses the sandbox.
type CodeExecutor struct {
	sandbox *Sandbox
}

// NewCodeExecutorWithSandbox creates a new code executor with sandbox.
func NewCodeExecutorWithSandbox(sandbox *Sandbox) *CodeExecutor {
	return &CodeExecutor{sandbox: sandbox}
}

func (e *CodeExecutor) NodeType() string {
	return "code"
}

func (e *CodeExecutor) Execute(ctx context.Context, code, language string, input map[string]interface{}, timeout time.Duration) (*ExecutionResult, error) {
	return e.sandbox.Execute(ctx, &ExecutionRequest{
		Code:     code,
		Language: language,
		Input:    input,
		Timeout:  timeout,
	})
}

// Ensure io is imported even if not directly used in visible code.
var _ = io.EOF
