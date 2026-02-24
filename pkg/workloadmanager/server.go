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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/agentcube/pkg/store"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Server is the main structure for workload manager
type Server struct {
	config            *Config
	router            *gin.Engine
	httpServer        *http.Server
	k8sClient         *K8sClient
	sandboxController *SandboxReconciler
	tokenCache        *TokenCache
	informers         *Informers
	storeClient       store.Store
	poolManager       *PoolManager
	poolAutoscaler    *PoolAutoscaler
}

type Config struct {
	// Port is the port the API server listens on
	Port string
	// RuntimeClassName is the RuntimeClassName for sandbox pods
	RuntimeClassName string
	// EnableTLS enables HTTPS
	EnableTLS bool
	// TLSCert is the path to the TLS certificate file
	TLSCert string
	// TLSKey is the path to the TLS private key file
	TLSKey string
	// EnableAuth enable auth by service account
	EnableAuth bool
	// PoolConfig for hot/warm pool management
	PoolConfig PoolConfig
}

// NewServer creates a new API server instance
func NewServer(config *Config, sandboxController *SandboxReconciler) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Create Kubernetes client
	k8sClient, err := NewK8sClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Initialize public key cache from Router's Secret in background
	// This will retry until successful (handles case where Router isn't ready yet)
	InitPublicKeyCache(k8sClient.clientset)

	// Create token cache (cache up to 1000 tokens, 5min TTL)
	tokenCache := NewTokenCache(1000, 5*time.Minute)

	// Create pool autoscaler with k8s client for actual CRD updates
	var autoscaler *PoolAutoscaler
	if config.PoolConfig.HotPoolEnabled || config.PoolConfig.WarmPoolEnabled {
		autoscalerConfig := PoolAutoscalerConfig{
			Enabled:       true,
			CheckInterval: 5 * time.Minute,
			MinSize:       int32(config.PoolConfig.HotPoolMinSize),
			MaxSize:       int32(config.PoolConfig.HotPoolMaxSize),
			ScaleStep:     5,
		}
		autoscaler = NewPoolAutoscaler(autoscalerConfig, nil, nil, k8sClient.dynamicClient)
	}

	poolManager := NewPoolManager(config.PoolConfig, nil)

	server := &Server{
		config:            config,
		k8sClient:         k8sClient,
		sandboxController: sandboxController,
		tokenCache:        tokenCache,
		informers:         NewInformers(k8sClient),
		storeClient:       store.Storage(),
		poolManager:       poolManager,
		poolAutoscaler:    autoscaler,
	}

	// Setup routes
	server.setupRoutes()

	return server, nil
}

// setupRoutes configures HTTP routes
func (s *Server) setupRoutes() {
	s.router = gin.New()

	// Health check (no authentication required)
	s.router.GET("/health", s.handleHealth)

	// API v1 routes
	v1Group := s.router.Group("/v1")
	// Apply middleware (logging first, then auth)
	v1Group.Use(s.loggingMiddleware)
	v1Group.Use(s.authMiddleware)

	// agent runtime management endpoints
	v1Group.POST("/agent-runtime", s.handleAgentRuntimeCreate)
	v1Group.DELETE("/agent-runtime/sessions/:sessionId", s.handleDeleteSandbox)
	// code interpreter management endpoints
	v1Group.POST("/code-interpreter", s.handleCodeInterpreterCreate)
	v1Group.DELETE("/code-interpreter/sessions/:sessionId", s.handleDeleteSandbox)
	// pool management endpoints
	v1Group.GET("/pool/stats", s.handlePoolStats)
	v1Group.POST("/pool/return/:sandboxId", s.handlePoolReturn)
}

// Start starts the API server
func (s *Server) Start(ctx context.Context) error {
	// Initialize store with informer before starting server

	if err := s.informers.RunAndWaitForCacheSync(ctx); err != nil {
		return fmt.Errorf("failed to wait for caches to sync: %w", err)
	}

	if err := s.storeClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping store: %w", err)
	}

	klog.Info("kv store Ping check successfully")

	addr := ":" + s.config.Port

	// Create HTTP/2 server for better performance
	h2s := &http2.Server{}

	// Wrap handler with h2c for HTTP/2 cleartext support
	h2cHandler := h2c.NewHandler(s.router, h2s)

	s.httpServer = &http.Server{
		Addr:        addr,
		Handler:     h2cHandler,
		ReadTimeout: 15 * time.Second,
		IdleTimeout: 90 * time.Second, // golang http default transport's idletimeout is 90s
	}

	// Listen for shutdown signal in goroutine
	go func() {
		<-ctx.Done()
		klog.Info("Shutting down server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			klog.Errorf("Server shutdown error: %v", err)
		}
	}()

	klog.Infof("Server listening on %s", addr)

	gc := newGarbageCollector(s.k8sClient, s.storeClient, 15*time.Second)
	go gc.run(ctx.Done())

	// Start pool manager background tasks
	if s.poolManager != nil && (s.config.PoolConfig.HotPoolEnabled || s.config.PoolConfig.ReuseEnabled) {
		go s.poolManager.Run(ctx)
		klog.Info("Pool manager started")
	}

	// Start HTTP or HTTPS server
	if s.config.EnableTLS {
		if s.config.TLSCert == "" || s.config.TLSKey == "" {
			return fmt.Errorf("TLS enabled but cert/key not provided")
		}
		return s.httpServer.ListenAndServeTLS(s.config.TLSCert, s.config.TLSKey)
	}

	return s.httpServer.ListenAndServe()
}

// loggingMiddleware logs each request (except /health)
func (s *Server) loggingMiddleware(c *gin.Context) {
	start := time.Now()
	klog.Infof("%s %s %s", c.Request.Method, c.Request.RequestURI, c.ClientIP())
	c.Next()
	klog.Infof("%s %s - completed in %v", c.Request.Method, c.Request.RequestURI, time.Since(start))
}

// handlePoolStats returns pool statistics
func (s *Server) handlePoolStats(c *gin.Context) {
	if s.poolManager == nil {
		respondJSON(c, http.StatusOK, gin.H{
			"message": "pool manager not enabled",
		})
		return
	}
	stats := s.poolManager.GetStats()
	respondJSON(c, http.StatusOK, stats)
}

// handlePoolReturn returns a sandbox to the pool for reuse
func (s *Server) handlePoolReturn(c *gin.Context) {
	sandboxID := c.Param("sandboxId")
	if sandboxID == "" {
		respondError(c, http.StatusBadRequest, "sandboxId is required")
		return
	}

	if s.poolManager == nil {
		respondError(c, http.StatusBadRequest, "pool manager not enabled")
		return
	}

	sandbox, err := s.storeClient.GetSandboxBySessionID(c.Request.Context(), sandboxID)
	if err != nil {
		respondError(c, http.StatusNotFound, "sandbox not found")
		return
	}

	pooledSandbox := &PooledSandbox{
		SandboxID:   sandbox.SandboxID,
		Name:        sandbox.Name,
		Namespace:   sandbox.SandboxNamespace,
		Kind:        sandbox.Kind,
		EntryPoints: make([]SandboxEntryPoint, len(sandbox.EntryPoints)),
		PodIP:       sandbox.PodIP,
		ReuseCount:  sandbox.ReuseCount,
	}
	for i, ep := range sandbox.EntryPoints {
		pooledSandbox.EntryPoints[i] = SandboxEntryPoint{
			Path:     ep.Path,
			Protocol: ep.Protocol,
			Endpoint: ep.Endpoint,
		}
	}

	if err := s.poolManager.ReturnToHotPool(pooledSandbox); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(c, http.StatusOK, gin.H{
		"message":    "sandbox returned to pool",
		"sandboxId":  sandboxID,
		"reuseCount": pooledSandbox.ReuseCount,
	})
}
