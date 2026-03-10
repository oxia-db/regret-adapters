package task

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/coordinator/internal/task/generator"
)

type TaskStatus struct {
	Name             string     `json:"name"`
	Type             string     `json:"type"`
	Namespace        string     `json:"namespace"`
	WorkerEndpoint   string     `json:"workerEndpoint"`
	State            string     `json:"state"`
	Operations       int64      `json:"operations"`
	AssertionsPassed int64      `json:"assertions_passed"`
	AssertionsFailed int64      `json:"assertions_failed"`
	RunningSince     *time.Time `json:"running_since"`
	LastFailure      *string    `json:"last_failure"`
}

type Manager struct {
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
	tasks           map[string]Task
	configs         map[string]*config.TestCaseConfig
	statuses        map[string]*TaskStatus
	providerManager *ProviderManager
}

func (m *Manager) CreateTask(tc *config.TestCaseConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exist := m.tasks[tc.Name]; exist {
		return fmt.Errorf("testcase %q already exists", tc.Name)
	}

	gen := m.createGenerator(tc)
	if gen == nil {
		return fmt.Errorf("unknown testcase type: %s", tc.Type)
	}

	now := time.Now()
	m.statuses[tc.Name] = &TaskStatus{
		Name:           tc.Name,
		Type:           tc.Type,
		Namespace:      tc.Namespace,
		WorkerEndpoint: tc.WorkerEndpoint,
		State:          "running",
		RunningSince:   &now,
	}

	newTask := NewTask(m.ctx, m.providerManager, tc.Name, tc.Namespace, gen, tc.WorkerEndpoint, m.statuses[tc.Name])
	m.tasks[tc.Name] = newTask
	m.configs[tc.Name] = tc

	newTask.Run()
	slog.Info("Task created and started", "name", tc.Name, "type", tc.Type, "worker", tc.WorkerEndpoint)
	return nil
}

func (m *Manager) DeleteTask(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, exist := m.tasks[name]
	if !exist {
		return fmt.Errorf("testcase %q not found", name)
	}

	if err := t.Close(); err != nil {
		slog.Error("Failed to close task", "name", name, "error", err)
	}
	delete(m.tasks, name)
	delete(m.configs, name)
	delete(m.statuses, name)

	slog.Info("Task deleted", "name", name)
	return nil
}

func (m *Manager) GetStatus(name string) (*TaskStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exist := m.statuses[name]
	return status, exist
}

func (m *Manager) ListStatuses() []*TaskStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*TaskStatus, 0, len(m.statuses))
	for _, s := range m.statuses {
		result = append(result, s)
	}
	return result
}

func (m *Manager) createGenerator(tc *config.TestCaseConfig) generator.Generator {
	switch tc.Type {
	case config.TestCaseTypeBasicKv:
		return generator.NewBasicKv(m.ctx, tc)
	case config.TestCaseTypeStreamingSequence:
		return generator.NewStreamingSequence(m.ctx, tc)
	case config.TestCaseTypeMetadataWithNotification:
		return generator.NewMetadataNotificationGenerator(m.ctx, tc)
	case config.TestCaseTypeMetadataWithEphemeral:
		return generator.NewMetadataEphemeralGenerator(m.ctx, tc)
	case config.TestCaseTypeConditionalPut:
		return generator.NewConditionalPut(m.ctx, tc)
	default:
		return nil
	}
}

func NewManager(ctx context.Context) *Manager {
	currentContext, currentContextCancel := context.WithCancel(ctx)

	return &Manager{
		ctx:             currentContext,
		cancel:          currentContextCancel,
		tasks:           make(map[string]Task),
		configs:         make(map[string]*config.TestCaseConfig),
		statuses:        make(map[string]*TaskStatus),
		providerManager: NewProviderManager(),
	}
}
