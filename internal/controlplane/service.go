package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrClusterNotFound   = errors.New("cluster not found")
	ErrNamespaceExists   = errors.New("namespace already exists")
	ErrServiceNotFound   = errors.New("service not found")
	ErrConfigKeyNotFound = errors.New("config key not found")
)

type RateLimitConfig struct {
	RequestsPerSecond int           `json:"requests_per_second"`
	BurstSize         int           `json:"burst_size"`
	WindowDuration    time.Duration `json:"window_duration"`
}

type FeatureFlags struct {
	EnableBetaFeatures     bool `json:"enable_beta_features"`
	EnableMetrics          bool `json:"enable_metrics"`
	EnableTracing          bool `json:"enable_tracing"`
	EnableCrossClusterSync bool `json:"enable_cross_cluster_sync"`
}

type RetentionPolicy struct {
	DefaultRetentionDays int           `json:"default_retention_days"`
	MaxRetentionDays     int           `json:"max_retention_days"`
	CleanupInterval      time.Duration `json:"cleanup_interval"`
}

type DynamicConfig struct {
	RateLimits        *RateLimitConfig           `json:"rate_limits,omitempty"`
	FeatureFlags      *FeatureFlags              `json:"feature_flags,omitempty"`
	RetentionPolicies *RetentionPolicy           `json:"retention_policies,omitempty"`
	Custom            map[string]json.RawMessage `json:"custom,omitempty"`
}

type PeerCluster struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
}

type ClusterSyncConfig struct {
	PeerClusters     []PeerCluster `json:"peer_clusters"`
	SyncInterval     time.Duration `json:"sync_interval"`
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`
	MaxRetries       int           `json:"max_retries"`
}

type HeartbeatRequest struct {
	ClusterID   string            `json:"cluster_id"`
	ClusterName string            `json:"cluster_name"`
	Region      string            `json:"region"`
	Status      ClusterStatus     `json:"status"`
	Timestamp   time.Time         `json:"timestamp"`
	Metadata    map[string]string `json:"metadata"`
}

type HeartbeatResponse struct {
	ClusterID string         `json:"cluster_id"`
	Status    ClusterStatus  `json:"status"`
	Timestamp time.Time      `json:"timestamp"`
	Clusters  []*ClusterInfo `json:"clusters,omitempty"`
}

type ClusterSyncClient interface {
	SendHeartbeat(ctx context.Context, endpoint string, req *HeartbeatRequest) (*HeartbeatResponse, error)
}

// ClusterInfo represents a cluster in the federation.
type ClusterInfo struct {
	ID            string
	Name          string
	Region        string
	Endpoint      string
	Status        ClusterStatus
	LastHeartbeat time.Time
	Metadata      map[string]string
}

// ClusterStatus represents cluster health status.
type ClusterStatus int

const (
	ClusterStatusUnknown ClusterStatus = iota
	ClusterStatusHealthy
	ClusterStatusDegraded
	ClusterStatusUnhealthy
	ClusterStatusOffline
)

// NamespaceConfig represents namespace configuration.
type NamespaceConfig struct {
	ID                   string
	Name                 string
	Description          string
	OwnerEmail           string
	RetentionDays        int
	HistorySizeLimitMB   int
	WorkflowExecutionTTL time.Duration
	AllowedClusters      []string
	DefaultCluster       string
	SearchAttributes     map[string]SearchAttributeType
	ArchivalConfig       *ArchivalConfig
}

// SearchAttributeType defines the type of a search attribute.
type SearchAttributeType int

const (
	SearchAttributeTypeString SearchAttributeType = iota
	SearchAttributeTypeKeyword
	SearchAttributeTypeInt
	SearchAttributeTypeDouble
	SearchAttributeTypeBool
	SearchAttributeDatetime
)

// ArchivalConfig defines archival settings.
type ArchivalConfig struct {
	Enabled       bool
	URI           string
	HistoryURI    string
	VisibilityURI string
}

// ServiceInstance represents a registered service instance.
type ServiceInstance struct {
	ID        string
	Service   string
	Address   string
	Port      int
	Metadata  map[string]string
	Health    HealthStatus
	LastCheck time.Time
	Version   string
}

// HealthStatus represents service health.
type HealthStatus int

const (
	HealthStatusUnknown HealthStatus = iota
	HealthStatusServing
	HealthStatusNotServing
)

// Config holds control plane configuration.
type Config struct {
	ClusterID         string
	ClusterName       string
	Region            string
	Endpoint          string
	Logger            *slog.Logger
	ClusterSyncConfig *ClusterSyncConfig
}

// Service is the control plane service.
type Service struct {
	config Config
	logger *slog.Logger

	clusters      map[string]*ClusterInfo
	namespaces    map[string]*NamespaceConfig
	services      map[string][]*ServiceInstance
	dynamicConfig *DynamicConfig
	configStore   map[string]json.RawMessage
	syncClient    ClusterSyncClient

	mu       sync.RWMutex
	configMu sync.RWMutex
	stopCh   chan struct{}
	running  bool
}

// NewService creates a new control plane service.
func NewService(config Config) *Service {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ClusterSyncConfig == nil {
		config.ClusterSyncConfig = &ClusterSyncConfig{
			SyncInterval:     30 * time.Second,
			HeartbeatTimeout: 10 * time.Second,
			MaxRetries:       3,
		}
	}
	return &Service{
		config:     config,
		logger:     config.Logger,
		clusters:   make(map[string]*ClusterInfo),
		namespaces: make(map[string]*NamespaceConfig),
		services:   make(map[string][]*ServiceInstance),
		dynamicConfig: &DynamicConfig{
			RateLimits: &RateLimitConfig{
				RequestsPerSecond: 1000,
				BurstSize:         100,
				WindowDuration:    time.Second,
			},
			FeatureFlags: &FeatureFlags{
				EnableMetrics:          true,
				EnableTracing:          true,
				EnableCrossClusterSync: true,
			},
			RetentionPolicies: &RetentionPolicy{
				DefaultRetentionDays: 30,
				MaxRetentionDays:     365,
				CleanupInterval:      24 * time.Hour,
			},
			Custom: make(map[string]json.RawMessage),
		},
		configStore: make(map[string]json.RawMessage),
		stopCh:      make(chan struct{}),
	}
}

// SetSyncClient sets the cluster sync client for cross-cluster communication.
func (s *Service) SetSyncClient(client ClusterSyncClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncClient = client
}

// Start starts the control plane service.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("control plane already running")
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	// Register self as a cluster
	s.RegisterCluster(ctx, &ClusterInfo{
		ID:       s.config.ClusterID,
		Name:     s.config.ClusterName,
		Region:   s.config.Region,
		Status:   ClusterStatusHealthy,
		Metadata: map[string]string{"role": "primary"},
	})

	// Start background tasks
	go s.runHealthChecker(ctx)
	go s.runClusterSync(ctx)

	s.logger.Info("control plane started",
		slog.String("cluster_id", s.config.ClusterID),
		slog.String("region", s.config.Region),
	)

	return nil
}

// Stop stops the control plane service.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.logger.Info("control plane stopped")
	return nil
}

// RegisterCluster registers a cluster.
func (s *Service) RegisterCluster(ctx context.Context, cluster *ClusterInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster.LastHeartbeat = time.Now()
	s.clusters[cluster.ID] = cluster

	s.logger.Info("cluster registered",
		slog.String("cluster_id", cluster.ID),
		slog.String("region", cluster.Region),
	)

	return nil
}

// GetCluster retrieves a cluster by ID.
func (s *Service) GetCluster(ctx context.Context, clusterID string) (*ClusterInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, exists := s.clusters[clusterID]
	if !exists {
		return nil, ErrClusterNotFound
	}
	return cluster, nil
}

// ListClusters returns all clusters.
func (s *Service) ListClusters(ctx context.Context) []*ClusterInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusters := make([]*ClusterInfo, 0, len(s.clusters))
	for _, c := range s.clusters {
		clusters = append(clusters, c)
	}
	return clusters
}

// CreateNamespace creates a new namespace.
func (s *Service) CreateNamespace(ctx context.Context, ns *NamespaceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.namespaces[ns.Name]; exists {
		return ErrNamespaceExists
	}

	s.namespaces[ns.Name] = ns

	s.logger.Info("namespace created",
		slog.String("namespace", ns.Name),
	)

	return nil
}

// GetNamespace retrieves a namespace by name.
func (s *Service) GetNamespace(ctx context.Context, name string) (*NamespaceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ns, exists := s.namespaces[name]
	if !exists {
		return nil, errors.New("namespace not found")
	}
	return ns, nil
}

// UpdateNamespace updates a namespace.
func (s *Service) UpdateNamespace(ctx context.Context, ns *NamespaceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.namespaces[ns.Name]; !exists {
		return errors.New("namespace not found")
	}

	s.namespaces[ns.Name] = ns
	return nil
}

// ListNamespaces returns all namespaces.
func (s *Service) ListNamespaces(ctx context.Context) []*NamespaceConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	namespaces := make([]*NamespaceConfig, 0, len(s.namespaces))
	for _, ns := range s.namespaces {
		namespaces = append(namespaces, ns)
	}
	return namespaces
}

// RegisterService registers a service instance.
func (s *Service) RegisterService(ctx context.Context, instance *ServiceInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	instance.LastCheck = time.Now()
	instance.Health = HealthStatusServing

	instances := s.services[instance.Service]

	// Update existing or add new
	found := false
	for i, inst := range instances {
		if inst.ID == instance.ID {
			instances[i] = instance
			found = true
			break
		}
	}
	if !found {
		s.services[instance.Service] = append(instances, instance)
	}

	s.logger.Info("service registered",
		slog.String("service", instance.Service),
		slog.String("instance_id", instance.ID),
		slog.String("address", instance.Address),
	)

	return nil
}

// DeregisterService removes a service instance.
func (s *Service) DeregisterService(ctx context.Context, service, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	instances := s.services[service]
	for i, inst := range instances {
		if inst.ID == instanceID {
			s.services[service] = append(instances[:i], instances[i+1:]...)
			break
		}
	}

	return nil
}

// GetServiceInstances returns all instances of a service.
func (s *Service) GetServiceInstances(ctx context.Context, service string) ([]*ServiceInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances, exists := s.services[service]
	if !exists {
		return nil, ErrServiceNotFound
	}

	// Filter healthy instances
	healthy := make([]*ServiceInstance, 0)
	for _, inst := range instances {
		if inst.Health == HealthStatusServing {
			healthy = append(healthy, inst)
		}
	}

	return healthy, nil
}

// RouteRequest determines which cluster should handle a request.
func (s *Service) RouteRequest(ctx context.Context, namespaceID, workflowID string) (*ClusterInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get namespace config
	ns, exists := s.namespaces[namespaceID]
	if !exists {
		// Use default cluster
		for _, cluster := range s.clusters {
			if cluster.Status == ClusterStatusHealthy {
				return cluster, nil
			}
		}
		return nil, errors.New("no healthy cluster available")
	}

	// Use namespace's default cluster if specified
	if ns.DefaultCluster != "" {
		if cluster, exists := s.clusters[ns.DefaultCluster]; exists {
			if cluster.Status == ClusterStatusHealthy {
				return cluster, nil
			}
		}
	}

	// Find first healthy allowed cluster
	for _, clusterID := range ns.AllowedClusters {
		if cluster, exists := s.clusters[clusterID]; exists {
			if cluster.Status == ClusterStatusHealthy {
				return cluster, nil
			}
		}
	}

	return nil, errors.New("no healthy cluster available for namespace")
}

// GetConfig returns configuration for the specified key as JSON.
func (s *Service) GetConfig(ctx context.Context, key string) (json.RawMessage, error) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()

	switch key {
	case "rate_limits":
		if s.dynamicConfig.RateLimits == nil {
			return nil, ErrConfigKeyNotFound
		}
		return json.Marshal(s.dynamicConfig.RateLimits)
	case "feature_flags":
		if s.dynamicConfig.FeatureFlags == nil {
			return nil, ErrConfigKeyNotFound
		}
		return json.Marshal(s.dynamicConfig.FeatureFlags)
	case "retention_policies":
		if s.dynamicConfig.RetentionPolicies == nil {
			return nil, ErrConfigKeyNotFound
		}
		return json.Marshal(s.dynamicConfig.RetentionPolicies)
	default:
		if val, exists := s.configStore[key]; exists {
			return val, nil
		}
		if s.dynamicConfig.Custom != nil {
			if val, exists := s.dynamicConfig.Custom[key]; exists {
				return val, nil
			}
		}
		return nil, ErrConfigKeyNotFound
	}
}

// SetConfig sets configuration for the specified key.
func (s *Service) SetConfig(ctx context.Context, key string, value json.RawMessage) error {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	switch key {
	case "rate_limits":
		var cfg RateLimitConfig
		if err := json.Unmarshal(value, &cfg); err != nil {
			return err
		}
		s.dynamicConfig.RateLimits = &cfg
	case "feature_flags":
		var cfg FeatureFlags
		if err := json.Unmarshal(value, &cfg); err != nil {
			return err
		}
		s.dynamicConfig.FeatureFlags = &cfg
	case "retention_policies":
		var cfg RetentionPolicy
		if err := json.Unmarshal(value, &cfg); err != nil {
			return err
		}
		s.dynamicConfig.RetentionPolicies = &cfg
	default:
		s.configStore[key] = value
		if s.dynamicConfig.Custom == nil {
			s.dynamicConfig.Custom = make(map[string]json.RawMessage)
		}
		s.dynamicConfig.Custom[key] = value
	}

	s.logger.Info("config updated",
		slog.String("key", key),
	)

	return nil
}

// DeleteConfig removes configuration for the specified key.
func (s *Service) DeleteConfig(ctx context.Context, key string) error {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	switch key {
	case "rate_limits", "feature_flags", "retention_policies":
		return errors.New("cannot delete built-in config keys")
	default:
		delete(s.configStore, key)
		if s.dynamicConfig.Custom != nil {
			delete(s.dynamicConfig.Custom, key)
		}
	}

	s.logger.Info("config deleted",
		slog.String("key", key),
	)

	return nil
}

// GetAllConfig returns the entire dynamic configuration.
func (s *Service) GetAllConfig(ctx context.Context) (*DynamicConfig, error) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()

	configCopy := &DynamicConfig{
		Custom: make(map[string]json.RawMessage),
	}
	if s.dynamicConfig.RateLimits != nil {
		cfg := *s.dynamicConfig.RateLimits
		configCopy.RateLimits = &cfg
	}
	if s.dynamicConfig.FeatureFlags != nil {
		cfg := *s.dynamicConfig.FeatureFlags
		configCopy.FeatureFlags = &cfg
	}
	if s.dynamicConfig.RetentionPolicies != nil {
		cfg := *s.dynamicConfig.RetentionPolicies
		configCopy.RetentionPolicies = &cfg
	}
	for k, v := range s.dynamicConfig.Custom {
		configCopy.Custom[k] = v
	}

	return configCopy, nil
}

// ListConfigKeys returns all available config keys.
func (s *Service) ListConfigKeys(ctx context.Context) []string {
	s.configMu.RLock()
	defer s.configMu.RUnlock()

	keys := []string{"rate_limits", "feature_flags", "retention_policies"}
	for k := range s.configStore {
		keys = append(keys, k)
	}
	return keys
}

func (s *Service) runHealthChecker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkServiceHealth()
		}
	}
}

func (s *Service) checkServiceHealth() {
	s.mu.Lock()
	defer s.mu.Unlock()

	staleThreshold := 30 * time.Second

	for service, instances := range s.services {
		for _, inst := range instances {
			if time.Since(inst.LastCheck) > staleThreshold {
				inst.Health = HealthStatusNotServing
				s.logger.Warn("service instance unhealthy",
					slog.String("service", service),
					slog.String("instance_id", inst.ID),
				)
			}
		}
	}

	for _, cluster := range s.clusters {
		if time.Since(cluster.LastHeartbeat) > staleThreshold {
			cluster.Status = ClusterStatusOffline
			s.logger.Warn("cluster offline",
				slog.String("cluster_id", cluster.ID),
			)
		}
	}
}

func (s *Service) runClusterSync(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			// Sync cluster state with other clusters in federation
			s.syncClusters()
		}
	}
}

func (s *Service) syncClusters() {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ClusterSyncConfig.HeartbeatTimeout)
	defer cancel()

	s.mu.Lock()
	if cluster, exists := s.clusters[s.config.ClusterID]; exists {
		cluster.LastHeartbeat = time.Now()
		cluster.Status = ClusterStatusHealthy
	}

	if !s.dynamicConfig.FeatureFlags.EnableCrossClusterSync {
		s.mu.Unlock()
		return
	}

	syncClient := s.syncClient
	peerClusters := s.config.ClusterSyncConfig.PeerClusters
	ownClusterID := s.config.ClusterID
	ownClusterName := s.config.ClusterName
	ownRegion := s.config.Region

	var ownMetadata map[string]string
	if cluster, exists := s.clusters[ownClusterID]; exists {
		ownMetadata = cluster.Metadata
	}
	s.mu.Unlock()

	if len(peerClusters) == 0 {
		s.logger.Debug("no peer clusters configured for sync")
		return
	}

	if syncClient == nil {
		s.logger.Debug("sync client not configured, skipping cross-cluster sync")
		return
	}

	s.logger.Info("starting cluster sync",
		slog.Int("peer_count", len(peerClusters)),
	)

	req := &HeartbeatRequest{
		ClusterID:   ownClusterID,
		ClusterName: ownClusterName,
		Region:      ownRegion,
		Status:      ClusterStatusHealthy,
		Timestamp:   time.Now(),
		Metadata:    ownMetadata,
	}

	var wg sync.WaitGroup
	results := make(chan *peerSyncResult, len(peerClusters))

	for _, peer := range peerClusters {
		if peer.ID == ownClusterID {
			continue
		}
		wg.Add(1)
		go func(p PeerCluster) {
			defer wg.Done()
			result := s.syncWithPeer(ctx, syncClient, p, req)
			results <- result
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		s.processPeerSyncResult(result)
	}
}

type peerSyncResult struct {
	peer     PeerCluster
	response *HeartbeatResponse
	err      error
}

func (s *Service) syncWithPeer(ctx context.Context, client ClusterSyncClient, peer PeerCluster, req *HeartbeatRequest) *peerSyncResult {
	result := &peerSyncResult{peer: peer}

	var lastErr error
	maxRetries := s.config.ClusterSyncConfig.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				result.err = ctx.Err()
				return result
			case <-time.After(time.Duration(attempt*100) * time.Millisecond):
			}
		}

		resp, err := client.SendHeartbeat(ctx, peer.Endpoint, req)
		if err == nil {
			result.response = resp
			return result
		}
		lastErr = err

		s.logger.Warn("heartbeat attempt failed",
			slog.String("peer_id", peer.ID),
			slog.String("peer_endpoint", peer.Endpoint),
			slog.Int("attempt", attempt+1),
			slog.String("error", err.Error()),
		)
	}

	result.err = lastErr
	return result
}

func (s *Service) processPeerSyncResult(result *peerSyncResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if result.err != nil {
		s.logger.Warn("peer sync failed",
			slog.String("peer_id", result.peer.ID),
			slog.String("peer_endpoint", result.peer.Endpoint),
			slog.String("error", result.err.Error()),
		)

		if cluster, exists := s.clusters[result.peer.ID]; exists {
			cluster.Status = ClusterStatusDegraded
		} else {
			s.clusters[result.peer.ID] = &ClusterInfo{
				ID:            result.peer.ID,
				Region:        result.peer.Region,
				Endpoint:      result.peer.Endpoint,
				Status:        ClusterStatusOffline,
				LastHeartbeat: time.Time{},
			}
		}
		return
	}

	resp := result.response
	s.logger.Info("peer sync successful",
		slog.String("peer_id", result.peer.ID),
		slog.String("peer_status", clusterStatusString(resp.Status)),
	)

	if cluster, exists := s.clusters[result.peer.ID]; exists {
		cluster.Status = resp.Status
		cluster.LastHeartbeat = resp.Timestamp
	} else {
		s.clusters[result.peer.ID] = &ClusterInfo{
			ID:            resp.ClusterID,
			Region:        result.peer.Region,
			Endpoint:      result.peer.Endpoint,
			Status:        resp.Status,
			LastHeartbeat: resp.Timestamp,
		}
	}

	for _, remoteCluster := range resp.Clusters {
		if remoteCluster.ID == s.config.ClusterID {
			continue
		}
		if existing, exists := s.clusters[remoteCluster.ID]; exists {
			if remoteCluster.LastHeartbeat.After(existing.LastHeartbeat) {
				existing.Status = remoteCluster.Status
				existing.LastHeartbeat = remoteCluster.LastHeartbeat
				existing.Metadata = remoteCluster.Metadata
			}
		} else {
			s.clusters[remoteCluster.ID] = remoteCluster
		}
	}
}

func clusterStatusString(status ClusterStatus) string {
	switch status {
	case ClusterStatusHealthy:
		return "healthy"
	case ClusterStatusDegraded:
		return "degraded"
	case ClusterStatusUnhealthy:
		return "unhealthy"
	case ClusterStatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// AddPeerCluster adds a peer cluster for synchronization.
func (s *Service) AddPeerCluster(peer PeerCluster) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, existing := range s.config.ClusterSyncConfig.PeerClusters {
		if existing.ID == peer.ID {
			return
		}
	}
	s.config.ClusterSyncConfig.PeerClusters = append(s.config.ClusterSyncConfig.PeerClusters, peer)

	s.logger.Info("peer cluster added",
		slog.String("peer_id", peer.ID),
		slog.String("peer_endpoint", peer.Endpoint),
	)
}

// RemovePeerCluster removes a peer cluster from synchronization.
func (s *Service) RemovePeerCluster(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	peers := s.config.ClusterSyncConfig.PeerClusters
	for i, peer := range peers {
		if peer.ID == peerID {
			s.config.ClusterSyncConfig.PeerClusters = append(peers[:i], peers[i+1:]...)
			break
		}
	}

	s.logger.Info("peer cluster removed",
		slog.String("peer_id", peerID),
	)
}

// GetPeerClusters returns the list of configured peer clusters.
func (s *Service) GetPeerClusters() []PeerCluster {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]PeerCluster, len(s.config.ClusterSyncConfig.PeerClusters))
	copy(peers, s.config.ClusterSyncConfig.PeerClusters)
	return peers
}

// TriggerSync manually triggers a cluster sync.
func (s *Service) TriggerSync() {
	go s.syncClusters()
}
