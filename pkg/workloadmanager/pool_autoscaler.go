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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/agentcube/pkg/common/types"
)

type PoolTarget struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
}

type PoolAutoscalerConfig struct {
	Enabled       bool          `json:"enabled"`
	CheckInterval time.Duration `json:"checkInterval"`
	MinSize       int32         `json:"minSize"`
	MaxSize       int32         `json:"maxSize"`
	ScaleStep     int32         `json:"scaleStep"`
}

type PoolAutoscaler struct {
	config      PoolAutoscalerConfig
	predictor   *TrafficPredictor
	poolManager *PoolManager
	client      dynamic.Interface
	targets     map[string]PoolTarget
	targetsMu   sync.RWMutex
}

func NewPoolAutoscaler(config PoolAutoscalerConfig, predictor *TrafficPredictor, poolManager *PoolManager, client dynamic.Interface) *PoolAutoscaler {
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Minute
	}
	if config.ScaleStep == 0 {
		config.ScaleStep = 5
	}

	return &PoolAutoscaler{
		config:      config,
		predictor:   predictor,
		poolManager: poolManager,
		client:      client,
		targets:     make(map[string]PoolTarget),
	}
}

func (pa *PoolAutoscaler) RegisterTarget(target PoolTarget) {
	pa.targetsMu.Lock()
	defer pa.targetsMu.Unlock()

	key := pa.targetKey(target)
	pa.targets[key] = target
	klog.Infof("Registered pool autoscaler target: %s", key)
}

func (pa *PoolAutoscaler) UnregisterTarget(target PoolTarget) {
	pa.targetsMu.Lock()
	defer pa.targetsMu.Unlock()

	key := pa.targetKey(target)
	delete(pa.targets, key)
	klog.Infof("Unregistered pool autoscaler target: %s", key)
}

func (pa *PoolAutoscaler) Run(ctx context.Context) {
	if !pa.config.Enabled {
		return
	}

	ticker := time.NewTicker(pa.config.CheckInterval)
	defer ticker.Stop()

	klog.Info("Pool autoscaler started")

	for {
		select {
		case <-ctx.Done():
			klog.Info("Pool autoscaler stopped")
			return
		case <-ticker.C:
			pa.reconcile(ctx)
		}
	}
}

func (pa *PoolAutoscaler) reconcile(ctx context.Context) {
	pa.targetsMu.RLock()
	targets := make([]PoolTarget, 0, len(pa.targets))
	for _, t := range pa.targets {
		targets = append(targets, t)
	}
	pa.targetsMu.RUnlock()

	for _, target := range targets {
		if err := pa.reconcileTarget(ctx, target); err != nil {
			klog.Warningf("Failed to reconcile target %s: %v", pa.targetKey(target), err)
		}
	}
}

func (pa *PoolAutoscaler) reconcileTarget(ctx context.Context, target PoolTarget) error {
	if target.Kind != types.CodeInterpreterKind {
		return nil
	}

	prediction := pa.predictor.Predict()
	if prediction.Confidence < 0.5 {
		klog.V(2).Infof("Skipping autoscale for %s due to low confidence: %.2f",
			pa.targetKey(target), prediction.Confidence)
		return nil
	}

	key := pa.targetKey(target)

	stats := pa.poolManager.GetStats()
	currentSize := int32(stats.HotPoolSize + stats.WarmPoolSize)

	desiredSize := prediction.RecommendedSize

	if desiredSize > currentSize {
		if !pa.predictor.CanScaleUp(key) {
			klog.V(2).Infof("Scale up cooldown active for %s", key)
			return nil
		}

		newSize := currentSize + pa.config.ScaleStep
		if newSize > desiredSize {
			newSize = desiredSize
		}
		if newSize > pa.config.MaxSize {
			newSize = pa.config.MaxSize
		}

		klog.Infof("Scaling up pool for %s: %d -> %d (predicted: %d, confidence: %.2f)",
			key, currentSize, newSize, prediction.PredictedSessions, prediction.Confidence)

		if err := pa.scaleWarmPool(ctx, target, newSize); err != nil {
			klog.Warningf("Failed to scale up pool for %s: %v", key, err)
			return err
		}
		pa.predictor.RecordScaleUp(key)
	} else if desiredSize < currentSize {
		if !pa.predictor.CanScaleDown(key) {
			klog.V(2).Infof("Scale down cooldown active for %s", key)
			return nil
		}

		newSize := currentSize - pa.config.ScaleStep
		if newSize < desiredSize {
			newSize = desiredSize
		}
		if newSize < pa.config.MinSize {
			newSize = pa.config.MinSize
		}

		klog.Infof("Scaling down pool for %s: %d -> %d (predicted: %d, confidence: %.2f)",
			key, currentSize, newSize, prediction.PredictedSessions, prediction.Confidence)

		if err := pa.scaleWarmPool(ctx, target, newSize); err != nil {
			klog.Warningf("Failed to scale down pool for %s: %v", key, err)
			return err
		}
		pa.predictor.RecordScaleDown(key)
	}

	return nil
}

func (pa *PoolAutoscaler) GetStats() map[string]interface{} {
	stats := pa.poolManager.GetStats()
	prediction := pa.predictor.Predict()

	pa.targetsMu.RLock()
	targetCount := len(pa.targets)
	pa.targetsMu.RUnlock()

	return map[string]interface{}{
		"enabled":           pa.config.Enabled,
		"registeredTargets": targetCount,
		"currentPoolSize":   stats.HotPoolSize + stats.WarmPoolSize,
		"predictedSize":     prediction.RecommendedSize,
		"confidence":        prediction.Confidence,
		"hotPoolStats":      stats,
	}
}

func (pa *PoolAutoscaler) targetKey(target PoolTarget) string {
	return fmt.Sprintf("%s/%s/%s", target.Kind, target.Namespace, target.Name)
}

func (pa *PoolAutoscaler) scaleWarmPool(ctx context.Context, target PoolTarget, newSize int32) error {
	if pa.client == nil {
		return fmt.Errorf("k8s client not configured")
	}

	gvr := SandboxWarmPoolGVR

	warmPool, err := pa.client.Resource(gvr).Namespace(target.Namespace).Get(ctx, target.Name, v1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("WarmPool %s/%s not found, skipping scale", target.Namespace, target.Name)
			return nil
		}
		return fmt.Errorf("failed to get warm pool: %w", err)
	}

	currentReplicas, ok, err := unstructured.NestedInt64(warmPool.Object, "spec", "replicas")
	if err != nil {
		return fmt.Errorf("failed to get replicas: %w", err)
	}
	if ok && int32(currentReplicas) == newSize {
		return nil
	}

	if err := unstructured.SetNestedField(warmPool.Object, int64(newSize), "spec", "replicas"); err != nil {
		return fmt.Errorf("failed to set replicas: %w", err)
	}

	_, err = pa.client.Resource(gvr).Namespace(target.Namespace).Update(ctx, warmPool, v1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update warm pool: %w", err)
	}

	klog.Infof("Scaled WarmPool %s/%s replicas: %d -> %d", target.Namespace, target.Name, currentReplicas, newSize)
	return nil
}
