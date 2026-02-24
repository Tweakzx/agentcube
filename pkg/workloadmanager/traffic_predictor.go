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
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type TrafficSample struct {
	Timestamp    time.Time `json:"timestamp"`
	RequestCount int64     `json:"requestCount"`
	SessionCount int64     `json:"sessionCount"`
}

type PredictionResult struct {
	PredictedSessions int64     `json:"predictedSessions"`
	Confidence        float64   `json:"confidence"`
	RecommendedSize   int32     `json:"recommendedSize"`
	PredictedAt       time.Time `json:"predictedAt"`
}

type TrafficPredictorConfig struct {
	HistoryWindow     time.Duration `json:"historyWindow"`
	PredictionWindow  time.Duration `json:"predictionWindow"`
	MinSamples        int           `json:"minSamples"`
	SafetyMargin      float64       `json:"safetyMargin"`
	MinPoolSize       int32         `json:"minPoolSize"`
	MaxPoolSize       int32         `json:"maxPoolSize"`
	ScaleUpCooldown   time.Duration `json:"scaleUpCooldown"`
	ScaleDownCooldown time.Duration `json:"scaleDownCooldown"`
}

type TrafficPredictor struct {
	config        TrafficPredictorConfig
	history       []TrafficSample
	historyMu     sync.RWMutex
	predictions   map[string]*PredictionResult
	predMu        sync.RWMutex
	lastScaleUp   map[string]time.Time
	lastScaleDown map[string]time.Time
	scaleMu       sync.RWMutex
}

func NewTrafficPredictor(config TrafficPredictorConfig) *TrafficPredictor {
	if config.HistoryWindow == 0 {
		config.HistoryWindow = 2 * time.Hour
	}
	if config.PredictionWindow == 0 {
		config.PredictionWindow = 30 * time.Minute
	}
	if config.MinSamples == 0 {
		config.MinSamples = 10
	}
	if config.SafetyMargin == 0 {
		config.SafetyMargin = 1.2
	}
	if config.ScaleUpCooldown == 0 {
		config.ScaleUpCooldown = 5 * time.Minute
	}
	if config.ScaleDownCooldown == 0 {
		config.ScaleDownCooldown = 15 * time.Minute
	}

	return &TrafficPredictor{
		config:        config,
		history:       make([]TrafficSample, 0),
		predictions:   make(map[string]*PredictionResult),
		lastScaleUp:   make(map[string]time.Time),
		lastScaleDown: make(map[string]time.Time),
	}
}

func (tp *TrafficPredictor) RecordSample(sample TrafficSample) {
	tp.historyMu.Lock()
	defer tp.historyMu.Unlock()

	tp.history = append(tp.history, sample)

	cutoff := time.Now().Add(-tp.config.HistoryWindow)
	validIdx := 0
	for i, s := range tp.history {
		if s.Timestamp.After(cutoff) {
			validIdx = i
			break
		}
	}
	if validIdx > 0 {
		tp.history = tp.history[validIdx:]
	}
}

func (tp *TrafficPredictor) Predict() *PredictionResult {
	tp.historyMu.RLock()
	defer tp.historyMu.RUnlock()

	if len(tp.history) < tp.config.MinSamples {
		return &PredictionResult{
			PredictedSessions: 0,
			Confidence:        0,
			RecommendedSize:   tp.config.MinPoolSize,
			PredictedAt:       time.Now(),
		}
	}

	baseLoad := tp.calculateBaseLoad()
	periodicPattern := tp.calculatePeriodicPattern()
	trend := tp.calculateTrend()

	predictedRaw := baseLoad + periodicPattern + trend
	predicted := int64(float64(predictedRaw) * tp.config.SafetyMargin)

	confidence := tp.calculateConfidence()

	recommendedSize := int32(predicted)
	if recommendedSize < tp.config.MinPoolSize {
		recommendedSize = tp.config.MinPoolSize
	}
	if recommendedSize > tp.config.MaxPoolSize {
		recommendedSize = tp.config.MaxPoolSize
	}

	return &PredictionResult{
		PredictedSessions: predicted,
		Confidence:        confidence,
		RecommendedSize:   recommendedSize,
		PredictedAt:       time.Now(),
	}
}

func (tp *TrafficPredictor) calculateBaseLoad() int64 {
	if len(tp.history) == 0 {
		return 0
	}

	var total int64
	for _, s := range tp.history {
		total += s.SessionCount
	}
	return total / int64(len(tp.history))
}

func (tp *TrafficPredictor) calculatePeriodicPattern() int64 {
	if len(tp.history) < 24 {
		return 0
	}

	now := time.Now()
	currentHour := now.Hour()

	var hourSamples []int64
	for _, s := range tp.history {
		if s.Timestamp.Hour() == currentHour {
			hourSamples = append(hourSamples, s.SessionCount)
		}
	}

	if len(hourSamples) == 0 {
		return 0
	}

	var total int64
	for _, v := range hourSamples {
		total += v
	}
	avgHour := total / int64(len(hourSamples))

	avgAll := tp.calculateBaseLoad()

	return avgHour - avgAll
}

func (tp *TrafficPredictor) calculateTrend() int64 {
	if len(tp.history) < 5 {
		return 0
	}

	recent := tp.history[len(tp.history)-5:]
	var recentTotal int64
	for _, s := range recent {
		recentTotal += s.SessionCount
	}
	recentAvg := recentTotal / 5

	older := tp.history[:len(tp.history)-5]
	if len(older) == 0 {
		return 0
	}
	var olderTotal int64
	for _, s := range older {
		olderTotal += s.SessionCount
	}
	olderAvg := olderTotal / int64(len(older))

	return recentAvg - olderAvg
}

func (tp *TrafficPredictor) calculateConfidence() float64 {
	n := len(tp.history)
	if n < tp.config.MinSamples {
		return 0.1
	}

	maxSamples := int(tp.config.HistoryWindow / time.Minute)
	confidence := float64(n) / float64(maxSamples)
	if confidence > 1.0 {
		confidence = 1.0
	}

	return 0.5 + 0.5*confidence
}

func (tp *TrafficPredictor) CanScaleUp(key string) bool {
	tp.scaleMu.RLock()
	defer tp.scaleMu.RUnlock()

	lastScale, exists := tp.lastScaleUp[key]
	if !exists {
		return true
	}
	return time.Since(lastScale) >= tp.config.ScaleUpCooldown
}

func (tp *TrafficPredictor) CanScaleDown(key string) bool {
	tp.scaleMu.RLock()
	defer tp.scaleMu.RUnlock()

	lastScale, exists := tp.lastScaleDown[key]
	if !exists {
		return true
	}
	return time.Since(lastScale) >= tp.config.ScaleDownCooldown
}

func (tp *TrafficPredictor) RecordScaleUp(key string) {
	tp.scaleMu.Lock()
	defer tp.scaleMu.Unlock()
	tp.lastScaleUp[key] = time.Now()
}

func (tp *TrafficPredictor) RecordScaleDown(key string) {
	tp.scaleMu.Lock()
	defer tp.scaleMu.Unlock()
	tp.lastScaleDown[key] = time.Now()
}

func (tp *TrafficPredictor) GetHistory() []TrafficSample {
	tp.historyMu.RLock()
	defer tp.historyMu.RUnlock()

	result := make([]TrafficSample, len(tp.history))
	copy(result, tp.history)
	return result
}

type TrafficCollector struct {
	predictor *TrafficPredictor
	store     TrafficStore
	interval  time.Duration
}

type TrafficStore interface {
	GetSessionCount(ctx context.Context, since time.Time) (int64, error)
	GetRequestCount(ctx context.Context, since time.Time) (int64, error)
}

func NewTrafficCollector(predictor *TrafficPredictor, store TrafficStore, interval time.Duration) *TrafficCollector {
	if interval == 0 {
		interval = 1 * time.Minute
	}
	return &TrafficCollector{
		predictor: predictor,
		store:     store,
		interval:  interval,
	}
}

func (tc *TrafficCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(tc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tc.collect(ctx)
		}
	}
}

func (tc *TrafficCollector) collect(ctx context.Context) {
	since := time.Now().Add(-tc.interval)

	sessionCount, err := tc.store.GetSessionCount(ctx, since)
	if err != nil {
		klog.Warningf("Failed to get session count: %v", err)
		return
	}

	requestCount, err := tc.store.GetRequestCount(ctx, since)
	if err != nil {
		klog.Warningf("Failed to get request count: %v", err)
		return
	}

	sample := TrafficSample{
		Timestamp:    time.Now(),
		RequestCount: requestCount,
		SessionCount: sessionCount,
	}

	tc.predictor.RecordSample(sample)
	klog.V(2).Infof("Recorded traffic sample: sessions=%d, requests=%d", sessionCount, requestCount)
}
