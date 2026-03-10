package config

import "time"

// TestCaseConfig replaces the K8s TestCase CRD with a standalone config struct.
type TestCaseConfig struct {
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Namespace      string            `json:"namespace,omitempty"`
	WorkerEndpoint string            `json:"workerEndpoint"`
	OpRate         int               `json:"opRate,omitempty"`
	Duration       string            `json:"duration,omitempty"`
	Properties     map[string]string `json:"properties,omitempty"`
}

func (c *TestCaseConfig) GetOpRate() int {
	if c.OpRate <= 0 {
		return 10
	}
	return c.OpRate
}

func (c *TestCaseConfig) GetDuration() *time.Duration {
	if c.Duration == "" {
		return nil
	}
	d, err := time.ParseDuration(c.Duration)
	if err != nil {
		fallback := 10 * time.Minute
		return &fallback
	}
	return &d
}

// TestCase type constants.
const (
	TestCaseTypeBasicKv                  = "basic"
	TestCaseTypeStreamingSequence        = "streamingSequence"
	TestCaseTypeMetadataWithEphemeral    = "metadataWithEphemeral"
	TestCaseTypeMetadataWithNotification = "metadataWithNotification"
	TestCaseTypeConditionalPut           = "conditionalPut"
)
