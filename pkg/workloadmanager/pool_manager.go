/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadmanager

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type PoolLevel string

const (
	PoolLevelHot  PoolLevel = "hot"
	PoolLevelWarm PoolLevel = "warm"
	PoolLevelCold PoolLevel = "cold"
)

type PooledSandbox struct {
	SandboxID     string              `json:"sandboxId"`
	Name          string              `json:"name"`
	Namespace     string              `json:"namespace"`
	Kind          string              `json:"kind"`
	EntryPoints   []SandboxEntryPoint `json:"entryPoints"`
	PodIP         string              `json:"podIP"`
	Level         PoolLevel           `json:"level"`
	Allocated     bool                `json:"allocated"`
	SessionID     string              `json:"sessionId,omitempty"`
	CreatedAt     time.Time           `json:"createdAt"`
	AllocatedAt   time.Time           `json:"allocatedAt,omitempty"`
	ReuseCount    int                 `json:"reuseCount"`
	LastCleanupAt time.Time           `json:"lastCleanupAt,omitempty"`
	MaxReuseCount int                 `json:"maxReuseCount"`
}

type SandboxEntryPoint struct {
	Path     string `json:"path"`
	Protocol string `json:"protocol"`
	Endpoint string `json:"endpoint"`
}

type PoolConfig struct {
	HotPoolEnabled  bool          `json:"hotPoolEnabled"`
	HotPoolMinSize  int           `json:"hotPoolMinSize"`
	HotPoolMaxSize  int           `json:"hotPoolMaxSize"`
	HotPoolIdleTTL  time.Duration `json:"hotPoolIdleTTL"`
	WarmPoolEnabled bool          `json:"warmPoolEnabled"`
	ReuseEnabled    bool          `json:"reuseEnabled"`
	CleanupTimeout  time.Duration `json:"cleanupTimeout"`
	MaxReuseCount   int           `json:"maxReuseCount"`
}

type PoolStats struct {
	HotPoolSize      int `json:"hotPoolSize"`
	HotPoolAvailable int `json:"hotPoolAvailable"`
	HotPoolAllocated int `json:"hotPoolAllocated"`
	WarmPoolSize     int `json:"warmPoolSize"`
	TotalReuse       int `json:"totalReuse"`
	ReuseFailed      int `json:"reuseFailed"`
}

// JWTProvider provides JWT tokens for internal API calls
type JWTProvider interface {
	GenerateInternalToken(sandboxID string) (string, error)
}

type PoolManager struct {
	config      PoolConfig
	httpClient  *http.Client
	jwtProvider JWTProvider

	hotPool   map[string]*PooledSandbox
	hotPoolMu sync.RWMutex

	stats   PoolStats
	statsMu sync.RWMutex
}

func NewPoolManager(config PoolConfig, jwtProvider JWTProvider) *PoolManager {
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 30 * time.Second
	}
	if config.MaxReuseCount == 0 {
		config.MaxReuseCount = 100
	}
	if config.HotPoolIdleTTL == 0 {
		config.HotPoolIdleTTL = 5 * time.Minute
	}

	httpClient := &http.Client{
		Timeout: config.CleanupTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Use default JWT provider if none provided
	if jwtProvider == nil {
		jwtProvider = &defaultJWTProvider{}
	}

	return &PoolManager{
		config:      config,
		httpClient:  httpClient,
		jwtProvider: jwtProvider,
		hotPool:     make(map[string]*PooledSandbox),
	}
}

func (pm *PoolManager) GetHotPoolSandbox(namespace, name, kind string) *PooledSandbox {
	if !pm.config.HotPoolEnabled && !pm.config.ReuseEnabled {
		return nil
	}

	pm.hotPoolMu.Lock()
	defer pm.hotPoolMu.Unlock()

	key := pm.poolKey(namespace, name, kind)

	for _, sb := range pm.hotPool {
		if pm.poolKey(sb.Namespace, sb.Name, sb.Kind) != key {
			continue
		}
		if sb.Allocated {
			continue
		}

		sb.Allocated = true
		sb.AllocatedAt = time.Now()

		pm.statsMu.Lock()
		pm.stats.HotPoolAvailable--
		pm.stats.HotPoolAllocated++
		pm.statsMu.Unlock()

		klog.Infof("Allocated sandbox %s from hot pool for %s/%s (%s)",
			sb.SandboxID, namespace, name, kind)
		return sb
	}

	return nil
}

func (pm *PoolManager) ReturnToHotPool(sandbox *PooledSandbox) error {
	if !pm.config.ReuseEnabled {
		return nil
	}

	if sandbox.ReuseCount >= pm.config.MaxReuseCount {
		klog.Infof("Sandbox %s reached max reuse count (%d), not returning to pool",
			sandbox.SandboxID, sandbox.ReuseCount)
		return fmt.Errorf("max reuse count reached")
	}

	// Generate JWT token for cleanup request
	var jwtToken string
	if pm.jwtProvider != nil {
		var err error
		jwtToken, err = pm.jwtProvider.GenerateInternalToken(sandbox.SandboxID)
		if err != nil {
			klog.Warningf("Failed to generate JWT token for cleanup: %v", err)
		}
	}

	if err := pm.cleanupSandbox(sandbox, jwtToken); err != nil {
		klog.Warningf("Failed to cleanup sandbox %s: %v", sandbox.SandboxID, err)
		pm.statsMu.Lock()
		pm.stats.ReuseFailed++
		pm.statsMu.Unlock()
		return fmt.Errorf("cleanup failed: %w", err)
	}

	pm.hotPoolMu.Lock()
	defer pm.hotPoolMu.Unlock()

	sandbox.Allocated = false
	sandbox.SessionID = ""
	sandbox.AllocatedAt = time.Time{}
	sandbox.ReuseCount++
	sandbox.LastCleanupAt = time.Now()

	pm.hotPool[sandbox.SandboxID] = sandbox

	pm.statsMu.Lock()
	pm.stats.HotPoolAvailable++
	pm.stats.HotPoolAllocated--
	pm.stats.TotalReuse++
	pm.statsMu.Unlock()

	klog.Infof("Returned sandbox %s to hot pool (reuse count: %d)",
		sandbox.SandboxID, sandbox.ReuseCount)
	return nil
}

func (pm *PoolManager) AddToHotPool(sandbox *PooledSandbox) {
	pm.hotPoolMu.Lock()
	defer pm.hotPoolMu.Unlock()

	sandbox.Level = PoolLevelHot
	sandbox.Allocated = false
	sandbox.MaxReuseCount = pm.config.MaxReuseCount

	if sandbox.CreatedAt.IsZero() {
		sandbox.CreatedAt = time.Now()
	}

	pm.hotPool[sandbox.SandboxID] = sandbox

	pm.statsMu.Lock()
	pm.stats.HotPoolSize++
	pm.stats.HotPoolAvailable++
	pm.statsMu.Unlock()

	klog.Infof("Added sandbox %s to hot pool", sandbox.SandboxID)
}

func (pm *PoolManager) RemoveFromHotPool(sandboxID string) {
	pm.hotPoolMu.Lock()
	defer pm.hotPoolMu.Unlock()

	if sb, exists := pm.hotPool[sandboxID]; exists {
		pm.statsMu.Lock()
		pm.stats.HotPoolSize--
		if sb.Allocated {
			pm.stats.HotPoolAllocated--
		} else {
			pm.stats.HotPoolAvailable--
		}
		pm.statsMu.Unlock()
		delete(pm.hotPool, sandboxID)
		klog.Infof("Removed sandbox %s from hot pool", sandboxID)
	}
}

func (pm *PoolManager) cleanupSandbox(sandbox *PooledSandbox, jwtToken string) error {
	if len(sandbox.EntryPoints) == 0 {
		return fmt.Errorf("no entry points available")
	}

	endpoint := fmt.Sprintf("http://%s/internal/cleanup", sandbox.EntryPoints[0].Endpoint)

	reqBody := map[string]bool{
		"clearWorkspace": true,
		"killProcesses":  true,
		"resetEnv":       true,
		"clearNetwork":   false,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal cleanup request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("failed to create cleanup request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add JWT authentication if token is available
	if jwtToken != "" {
		req.Header.Set("Authorization", "Bearer "+jwtToken)
	}

	resp, err := pm.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cleanup request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cleanup returned status %d", resp.StatusCode)
	}

	klog.V(2).Infof("Successfully cleaned up sandbox %s", sandbox.SandboxID)
	return nil
}

func (pm *PoolManager) GetStats() PoolStats {
	pm.statsMu.RLock()
	defer pm.statsMu.RUnlock()
	return pm.stats
}

func (pm *PoolManager) ListHotPool() []*PooledSandbox {
	pm.hotPoolMu.RLock()
	defer pm.hotPoolMu.RUnlock()

	result := make([]*PooledSandbox, 0, len(pm.hotPool))
	for _, sb := range pm.hotPool {
		result = append(result, sb)
	}
	return result
}

func (pm *PoolManager) poolKey(namespace, name, kind string) string {
	return fmt.Sprintf("%s/%s/%s", kind, namespace, name)
}

// defaultJWTProvider provides a no-op JWT token implementation
type defaultJWTProvider struct{}

func (d *defaultJWTProvider) GenerateInternalToken(sandboxID string) (string, error) {
	// Return a dummy token for now - in a real implementation this would generate a proper JWT
	return "dummy-jwt-token", nil
}

func (pm *PoolManager) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.evictExpiredSandboxes()
		}
	}
}

func (pm *PoolManager) evictExpiredSandboxes() {
	pm.hotPoolMu.Lock()
	defer pm.hotPoolMu.Unlock()

	now := time.Now()
	for id, sb := range pm.hotPool {
		if sb.Allocated {
			continue
		}

		// Use LastCleanupAt if available, otherwise use CreatedAt
		idleSince := sb.LastCleanupAt
		if idleSince.IsZero() {
			idleSince = sb.CreatedAt
		}
		if idleSince.IsZero() {
			continue // Skip if no valid timestamp
		}
		if now.Sub(idleSince) > pm.config.HotPoolIdleTTL {
			delete(pm.hotPool, id)
			pm.statsMu.Lock()
			pm.stats.HotPoolSize--
			pm.stats.HotPoolAvailable--
			pm.statsMu.Unlock()
			klog.Infof("Evicted idle sandbox %s from hot pool (idle for %v)", id, now.Sub(idleSince))
		}
	}
}
