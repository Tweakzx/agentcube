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
)

func TestPoolManager_GetHotPoolSandbox(t *testing.T) {
	config := PoolConfig{
		HotPoolEnabled: true,
		ReuseEnabled:   true,
		MaxReuseCount:  10,
	}
	pm := NewPoolManager(config, nil)

	sandbox := &PooledSandbox{
		SandboxID: "test-sandbox-1",
		Name:      "test-sandbox",
		Namespace: "default",
		Kind:      "CodeInterpreter",
		EntryPoints: []SandboxEntryPoint{
			{Path: "/api", Protocol: "http", Endpoint: "10.0.0.1:8080"},
		},
		Allocated: false,
	}

	pm.AddToHotPool(sandbox)

	got := pm.GetHotPoolSandbox("default", "test-sandbox", "CodeInterpreter")
	if got == nil {
		t.Error("Expected to get sandbox from hot pool, got nil")
	}
	if got.SandboxID != "test-sandbox-1" {
		t.Errorf("Expected sandbox ID test-sandbox-1, got %s", got.SandboxID)
	}
	if !got.Allocated {
		t.Error("Expected sandbox to be marked as allocated")
	}

	got2 := pm.GetHotPoolSandbox("default", "test-sandbox", "CodeInterpreter")
	if got2 != nil {
		t.Error("Expected nil when getting already allocated sandbox")
	}
}

func TestPoolManager_GetHotPoolSandbox_Disabled(t *testing.T) {
	config := PoolConfig{
		HotPoolEnabled: false,
		ReuseEnabled:   false,
	}
	pm := NewPoolManager(config, nil)

	sandbox := &PooledSandbox{
		SandboxID: "test-sandbox-1",
		Name:      "test-sandbox",
		Namespace: "default",
		Kind:      "CodeInterpreter",
	}

	pm.AddToHotPool(sandbox)

	got := pm.GetHotPoolSandbox("default", "test-sandbox", "CodeInterpreter")
	if got != nil {
		t.Error("Expected nil when hot pool is disabled")
	}
}

func TestPoolManager_RemoveFromHotPool(t *testing.T) {
	config := PoolConfig{
		HotPoolEnabled: true,
		ReuseEnabled:   true,
	}
	pm := NewPoolManager(config, nil)

	sandbox := &PooledSandbox{
		SandboxID: "test-sandbox-1",
		Name:      "test-sandbox",
		Namespace: "default",
		Kind:      "CodeInterpreter",
	}

	pm.AddToHotPool(sandbox)
	stats := pm.GetStats()
	if stats.HotPoolSize != 1 {
		t.Errorf("Expected hot pool size 1, got %d", stats.HotPoolSize)
	}

	pm.RemoveFromHotPool("test-sandbox-1")
	stats = pm.GetStats()
	if stats.HotPoolSize != 0 {
		t.Errorf("Expected hot pool size 0 after removal, got %d", stats.HotPoolSize)
	}
}

func TestPoolManager_GetStats(t *testing.T) {
	config := PoolConfig{
		HotPoolEnabled: true,
		ReuseEnabled:   true,
	}
	pm := NewPoolManager(config, nil)

	pm.AddToHotPool(&PooledSandbox{SandboxID: "sandbox-1", Namespace: "ns1", Name: "n1", Kind: "k1"})
	pm.AddToHotPool(&PooledSandbox{SandboxID: "sandbox-2", Namespace: "ns2", Name: "n2", Kind: "k2"})

	stats := pm.GetStats()
	if stats.HotPoolSize != 2 {
		t.Errorf("Expected hot pool size 2, got %d", stats.HotPoolSize)
	}
	if stats.HotPoolAvailable != 2 {
		t.Errorf("Expected hot pool available 2, got %d", stats.HotPoolAvailable)
	}
}

func TestPoolManager_MaxReuseCount(t *testing.T) {
	config := PoolConfig{
		HotPoolEnabled: true,
		ReuseEnabled:   true,
		MaxReuseCount:  2,
	}
	pm := NewPoolManager(config, nil)

	sandbox := &PooledSandbox{
		SandboxID:   "test-sandbox-1",
		ReuseCount:  2,
		EntryPoints: []SandboxEntryPoint{{Endpoint: "10.0.0.1:8080"}},
	}

	err := pm.ReturnToHotPool(sandbox)
	if err == nil {
		t.Error("Expected error when returning sandbox that reached max reuse count")
	}
}
