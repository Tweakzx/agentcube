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
	"testing"
	"time"
)

func TestTrafficPredictor_Predict_InsufficientSamples(t *testing.T) {
	config := TrafficPredictorConfig{
		MinSamples:  10,
		MinPoolSize: 5,
		MaxPoolSize: 50,
	}
	tp := NewTrafficPredictor(config)

	for i := 0; i < 5; i++ {
		tp.RecordSample(TrafficSample{
			Timestamp:    time.Now(),
			SessionCount: 10,
		})
	}

	result := tp.Predict()
	if result.Confidence != 0 {
		t.Errorf("Expected confidence 0 with insufficient samples, got %f", result.Confidence)
	}
	if result.RecommendedSize != config.MinPoolSize {
		t.Errorf("Expected min pool size %d, got %d", config.MinPoolSize, result.RecommendedSize)
	}
}

func TestTrafficPredictor_Predict_SufficientSamples(t *testing.T) {
	config := TrafficPredictorConfig{
		MinSamples:   5,
		MinPoolSize:  5,
		MaxPoolSize:  100,
		SafetyMargin: 1.0,
	}
	tp := NewTrafficPredictor(config)

	for i := 0; i < 20; i++ {
		tp.RecordSample(TrafficSample{
			Timestamp:    time.Now().Add(-time.Duration(i) * time.Minute),
			SessionCount: 10,
		})
	}

	result := tp.Predict()
	if result.Confidence <= 0 {
		t.Errorf("Expected positive confidence, got %f", result.Confidence)
	}
	if result.RecommendedSize < config.MinPoolSize {
		t.Errorf("Recommended size %d less than min %d", result.RecommendedSize, config.MinPoolSize)
	}
	if result.RecommendedSize > config.MaxPoolSize {
		t.Errorf("Recommended size %d greater than max %d", result.RecommendedSize, config.MaxPoolSize)
	}
}

func TestTrafficPredictor_ScaleCooldown(t *testing.T) {
	config := TrafficPredictorConfig{
		ScaleUpCooldown:   5 * time.Minute,
		ScaleDownCooldown: 10 * time.Minute,
	}
	tp := NewTrafficPredictor(config)

	key := "test-key"

	if !tp.CanScaleUp(key) {
		t.Error("Expected to be able to scale up initially")
	}
	if !tp.CanScaleDown(key) {
		t.Error("Expected to be able to scale down initially")
	}

	tp.RecordScaleUp(key)
	if tp.CanScaleUp(key) {
		t.Error("Expected scale up to be blocked after recording scale up")
	}

	tp.RecordScaleDown(key)
	if tp.CanScaleDown(key) {
		t.Error("Expected scale down to be blocked after recording scale down")
	}
}

func TestTrafficPredictor_HistoryWindow(t *testing.T) {
	config := TrafficPredictorConfig{
		HistoryWindow: 1 * time.Hour,
	}
	tp := NewTrafficPredictor(config)

	tp.RecordSample(TrafficSample{
		Timestamp:    time.Now().Add(-2 * time.Hour),
		SessionCount: 100,
	})
	tp.RecordSample(TrafficSample{
		Timestamp:    time.Now().Add(-30 * time.Minute),
		SessionCount: 50,
	})
	tp.RecordSample(TrafficSample{
		Timestamp:    time.Now(),
		SessionCount: 25,
	})

	history := tp.GetHistory()
	if len(history) != 2 {
		t.Errorf("Expected 2 samples after cutoff, got %d", len(history))
	}
}

func TestTrafficPredictor_SafetyMargin(t *testing.T) {
	config := TrafficPredictorConfig{
		MinSamples:   5,
		MinPoolSize:  5,
		MaxPoolSize:  1000,
		SafetyMargin: 1.5,
	}
	tp := NewTrafficPredictor(config)

	for i := 0; i < 20; i++ {
		tp.RecordSample(TrafficSample{
			Timestamp:    time.Now().Add(-time.Duration(i) * time.Minute),
			SessionCount: 10,
		})
	}

	result := tp.Predict()
	if result.RecommendedSize < 10 {
		t.Errorf("Expected recommended size >= 10 with 1.5x safety margin, got %d", result.RecommendedSize)
	}
}
