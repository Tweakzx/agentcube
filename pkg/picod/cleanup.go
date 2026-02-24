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

package picod

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

const (
	defaultCleanupTimeout = 30 * time.Second
)

type CleanupRequest struct {
	ClearWorkspace bool `json:"clearWorkspace"`
	KillProcesses  bool `json:"killProcesses"`
	ResetEnv       bool `json:"resetEnv"`
	ClearNetwork   bool `json:"clearNetwork"`
}

type CleanupResponse struct {
	Success       bool          `json:"success"`
	Message       string        `json:"message"`
	Duration      time.Duration `json:"duration"`
	FilesCleared  int           `json:"filesCleared"`
	ProcessKilled int           `json:"processKilled"`
	Errors        []string      `json:"errors,omitempty"`
}

func (s *Server) CleanupHandler(c *gin.Context) {
	startTime := time.Now()

	var req CleanupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
			"code":  http.StatusBadRequest,
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultCleanupTimeout)
	defer cancel()

	response := &CleanupResponse{
		Success: true,
		Errors:  []string{},
	}

	if req.ClearWorkspace {
		count, err := s.clearWorkspace(ctx)
		response.FilesCleared = count
		if err != nil {
			response.Success = false
			response.Errors = append(response.Errors, fmt.Sprintf("workspace cleanup: %v", err))
		}
	}

	if req.KillProcesses {
		count, err := s.killUserProcesses(ctx)
		response.ProcessKilled = count
		if err != nil {
			response.Success = false
			response.Errors = append(response.Errors, fmt.Sprintf("process cleanup: %v", err))
		}
	}

	if req.ClearNetwork {
		if err := s.clearNetworkState(ctx); err != nil {
			response.Success = false
			response.Errors = append(response.Errors, fmt.Sprintf("network cleanup: %v", err))
		}
	}

	response.Duration = time.Since(startTime)
	if response.Success {
		response.Message = "Cleanup completed successfully"
		klog.Infof("Cleanup completed: files=%d, processes=%d, duration=%v",
			response.FilesCleared, response.ProcessKilled, response.Duration)
	} else {
		response.Message = "Cleanup completed with errors"
		klog.Warningf("Cleanup completed with errors: %v", response.Errors)
	}

	c.JSON(http.StatusOK, response)
}

func (s *Server) clearWorkspace(ctx context.Context) (int, error) {
	if s.workspaceDir == "" {
		return 0, fmt.Errorf("workspace directory not initialized")
	}

	entries, err := os.ReadDir(s.workspaceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read workspace directory: %w", err)
	}

	count := 0
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}

		entryPath := filepath.Join(s.workspaceDir, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			klog.Warningf("Failed to remove %s: %v", entryPath, err)
		} else {
			count++
		}
	}

	return count, nil
}

func (s *Server) killUserProcesses(ctx context.Context) (int, error) {
	currentPid := os.Getpid()
	count := 0

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc: %w", err)
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}

		if !entry.IsDir() {
			continue
		}

		pid := 0
		if _, err := fmt.Sscanf(entry.Name(), "%d", &pid); err != nil {
			continue
		}

		if pid <= 1 || pid == currentPid {
			continue
		}

		cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}

		if len(cmdline) == 0 {
			continue
		}

		cmdStr := string(cmdline)
		if strings.Contains(cmdStr, "picod") ||
			strings.Contains(cmdStr, "pause") ||
			strings.Contains(cmdStr, "agent-sandbox") {
			continue
		}

		process, err := os.FindProcess(pid)
		if err != nil {
			continue
		}

		if err := process.Signal(syscall.SIGTERM); err != nil {
			klog.V(2).Infof("Failed to send SIGTERM to process %d: %v", pid, err)
		} else {
			count++
			klog.V(2).Infof("Sent SIGTERM to user process %d", pid)
		}
	}

	if count > 0 {
		time.Sleep(100 * time.Millisecond)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			pid := 0
			if _, err := fmt.Sscanf(entry.Name(), "%d", &pid); err != nil {
				continue
			}
			if pid <= 1 || pid == currentPid {
				continue
			}
			process, _ := os.FindProcess(pid)
			process.Signal(syscall.SIGKILL)
		}
	}

	return count, nil
}

func (s *Server) clearNetworkState(ctx context.Context) error {
	commands := [][]string{
		{"iptables", "-F"},
		{"iptables", "-t", "nat", "-F"},
		{"iptables", "-t", "mangle", "-F"},
	}

	var errors []string
	for _, cmd := range commands {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		execCmd := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
		if err := execCmd.Run(); err != nil {
			if !strings.Contains(err.Error(), "not found") {
				errors = append(errors, fmt.Sprintf("%s: %v", strings.Join(cmd, " "), err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("network cleanup errors: %s", strings.Join(errors, "; "))
	}

	return nil
}
