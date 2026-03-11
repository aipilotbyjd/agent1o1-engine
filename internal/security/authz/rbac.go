package authz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrAccessDenied = errors.New("access denied")
	ErrRoleNotFound = errors.New("role not found")
)

// Permission represents a permission.
type Permission struct {
	Resource string // e.g., "workflows", "executions"
	Action   string // e.g., "read", "write", "delete", "execute"
}

func (p Permission) String() string {
	return p.Resource + ":" + p.Action
}

// Role represents a role with permissions.
type Role struct {
	Name        string
	Description string
	Permissions []Permission
}

// Subject represents a subject for authorization.
type Subject struct {
	UserID      string
	WorkspaceID string
	Roles       []string
	ExtraAttrs  map[string]string
}

// RBACAuthorizer implements role-based access control.
type RBACAuthorizer struct {
	roles   map[string]*Role
	rolesMu sync.RWMutex

	// Role inheritance
	inheritance map[string][]string
}

// NewRBACAuthorizer creates a new RBAC authorizer.
func NewRBACAuthorizer() *RBACAuthorizer {
	authorizer := &RBACAuthorizer{
		roles:       make(map[string]*Role),
		inheritance: make(map[string][]string),
	}

	// Register default roles
	authorizer.registerDefaultRoles()

	return authorizer
}

func (a *RBACAuthorizer) registerDefaultRoles() {
	// Owner role - full access
	a.RegisterRole(&Role{
		Name:        "owner",
		Description: "Full access to workspace",
		Permissions: []Permission{
			{Resource: "*", Action: "*"},
		},
	})

	// Admin role
	a.RegisterRole(&Role{
		Name:        "admin",
		Description: "Admin access",
		Permissions: []Permission{
			{Resource: "workflows", Action: "*"},
			{Resource: "executions", Action: "*"},
			{Resource: "credentials", Action: "*"},
			{Resource: "variables", Action: "*"},
			{Resource: "users", Action: "read"},
			{Resource: "settings", Action: "read"},
		},
	})

	// Editor role
	a.RegisterRole(&Role{
		Name:        "editor",
		Description: "Edit workflows and view executions",
		Permissions: []Permission{
			{Resource: "workflows", Action: "read"},
			{Resource: "workflows", Action: "write"},
			{Resource: "executions", Action: "read"},
			{Resource: "executions", Action: "execute"},
			{Resource: "credentials", Action: "use"},
			{Resource: "variables", Action: "read"},
		},
	})

	// Viewer role
	a.RegisterRole(&Role{
		Name:        "viewer",
		Description: "View only access",
		Permissions: []Permission{
			{Resource: "workflows", Action: "read"},
			{Resource: "executions", Action: "read"},
		},
	})

	// Executor role - for service accounts
	a.RegisterRole(&Role{
		Name:        "executor",
		Description: "Execute workflows only",
		Permissions: []Permission{
			{Resource: "workflows", Action: "read"},
			{Resource: "executions", Action: "execute"},
			{Resource: "executions", Action: "read"},
		},
	})

	// Set up inheritance
	a.inheritance["owner"] = []string{}
	a.inheritance["admin"] = []string{"editor"}
	a.inheritance["editor"] = []string{"viewer"}
}

// RegisterRole registers a role.
func (a *RBACAuthorizer) RegisterRole(role *Role) {
	a.rolesMu.Lock()
	defer a.rolesMu.Unlock()
	a.roles[role.Name] = role
}

// Authorize checks if subject has permission.
func (a *RBACAuthorizer) Authorize(ctx context.Context, subject *Subject, resource, action string) error {
	if subject == nil || len(subject.Roles) == 0 {
		return ErrAccessDenied
	}

	// Collect all permissions from all roles (including inherited)
	permissions := a.collectPermissions(subject.Roles)

	// Check if any permission matches
	for _, perm := range permissions {
		if a.matchPermission(perm, resource, action) {
			return nil
		}
	}

	return ErrAccessDenied
}

func (a *RBACAuthorizer) collectPermissions(roles []string) []Permission {
	a.rolesMu.RLock()
	defer a.rolesMu.RUnlock()

	visited := make(map[string]bool)
	var permissions []Permission

	var collect func(roleName string)
	collect = func(roleName string) {
		if visited[roleName] {
			return
		}
		visited[roleName] = true

		role, exists := a.roles[roleName]
		if !exists {
			return
		}

		permissions = append(permissions, role.Permissions...)

		// Collect from inherited roles
		for _, inherited := range a.inheritance[roleName] {
			collect(inherited)
		}
	}

	for _, role := range roles {
		collect(role)
	}

	return permissions
}

func (a *RBACAuthorizer) matchPermission(perm Permission, resource, action string) bool {
	// Check resource match
	if perm.Resource != "*" && perm.Resource != resource {
		return false
	}

	// Check action match
	if perm.Action != "*" && perm.Action != action {
		return false
	}

	return true
}

// GetRole returns a role by name.
func (a *RBACAuthorizer) GetRole(name string) (*Role, error) {
	a.rolesMu.RLock()
	defer a.rolesMu.RUnlock()

	role, exists := a.roles[name]
	if !exists {
		return nil, ErrRoleNotFound
	}
	return role, nil
}

// ListRoles returns all roles.
func (a *RBACAuthorizer) ListRoles() []*Role {
	a.rolesMu.RLock()
	defer a.rolesMu.RUnlock()

	roles := make([]*Role, 0, len(a.roles))
	for _, role := range a.roles {
		roles = append(roles, role)
	}
	return roles
}

// ABACAuthorizer implements attribute-based access control.
type ABACAuthorizer struct {
	policies []*Policy
	mu       sync.RWMutex
}

// Policy represents an ABAC policy.
type Policy struct {
	Name        string
	Description string
	Effect      Effect
	Subjects    []SubjectMatcher
	Resources   []ResourceMatcher
	Actions     []string
	Conditions  []Condition
}

// Effect is the policy effect.
type Effect string

const (
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

// SubjectMatcher matches subjects.
type SubjectMatcher struct {
	Type  string // "user", "role", "group"
	Value string
}

// ResourceMatcher matches resources.
type ResourceMatcher struct {
	Type  string // "workflow", "execution", "credential"
	ID    string // Specific ID or "*"
	Owner string // Owner constraint
}

// Condition represents a policy condition.
type Condition struct {
	Attribute string
	Operator  string // "eq", "ne", "in", "not_in", "gt", "lt"
	Value     interface{}
}

// NewABACAuthorizer creates a new ABAC authorizer.
func NewABACAuthorizer() *ABACAuthorizer {
	return &ABACAuthorizer{
		policies: make([]*Policy, 0),
	}
}

// AddPolicy adds a policy.
func (a *ABACAuthorizer) AddPolicy(policy *Policy) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.policies = append(a.policies, policy)
}

// Authorize checks if request is authorized.
func (a *ABACAuthorizer) Authorize(ctx context.Context, req *AuthzRequest) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, policy := range a.policies {
		match := a.evaluatePolicy(policy, req)
		if match {
			if policy.Effect == EffectDeny {
				return ErrAccessDenied
			}
			return nil // Allow
		}
	}

	return ErrAccessDenied // Default deny
}

// AuthzRequest represents an authorization request.
type AuthzRequest struct {
	Subject    *Subject
	Resource   string
	ResourceID string
	Action     string
	Context    map[string]interface{}
}

func (a *ABACAuthorizer) evaluatePolicy(policy *Policy, req *AuthzRequest) bool {
	// Check subjects
	if len(policy.Subjects) > 0 {
		matched := false
		for _, matcher := range policy.Subjects {
			if a.matchSubject(matcher, req.Subject) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check resources
	if len(policy.Resources) > 0 {
		matched := false
		for _, matcher := range policy.Resources {
			if a.matchResource(matcher, req) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check actions
	if len(policy.Actions) > 0 {
		matched := false
		for _, action := range policy.Actions {
			if action == "*" || action == req.Action {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check conditions
	for _, cond := range policy.Conditions {
		if !a.evaluateCondition(cond, req) {
			return false
		}
	}

	return true
}

func (a *ABACAuthorizer) matchSubject(matcher SubjectMatcher, subject *Subject) bool {
	switch matcher.Type {
	case "user":
		return matcher.Value == "*" || matcher.Value == subject.UserID
	case "role":
		for _, role := range subject.Roles {
			if matcher.Value == "*" || matcher.Value == role {
				return true
			}
		}
	}
	return false
}

func (a *ABACAuthorizer) matchResource(matcher ResourceMatcher, req *AuthzRequest) bool {
	if matcher.Type != "" && matcher.Type != req.Resource {
		return false
	}
	if matcher.ID != "" && matcher.ID != "*" && matcher.ID != req.ResourceID {
		return false
	}
	return true
}

func (a *ABACAuthorizer) evaluateCondition(cond Condition, req *AuthzRequest) bool {
	value, exists := req.Context[cond.Attribute]
	if !exists {
		return false
	}

	switch cond.Operator {
	case "eq":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", cond.Value)
	case "ne":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", cond.Value)
	case "in":
		if slice, ok := cond.Value.([]string); ok {
			for _, item := range slice {
				if item == fmt.Sprintf("%v", value) {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

// ResourceKey generates a resource key for permission checking.
func ResourceKey(resourceType, resourceID string) string {
	if resourceID == "" {
		return resourceType
	}
	return resourceType + "/" + resourceID
}

// ParseResourceKey parses a resource key.
func ParseResourceKey(key string) (resourceType, resourceID string) {
	parts := strings.SplitN(key, "/", 2)
	resourceType = parts[0]
	if len(parts) > 1 {
		resourceID = parts[1]
	}
	return
}
