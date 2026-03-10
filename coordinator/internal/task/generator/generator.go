package generator

import "github.com/oxia-io/okk/coordinator/internal/proto"

type Generator interface {
	Name() string

	Next() (*proto.Operation, bool)
}

// ResponseAwareGenerator is an optional interface for generators that need
// feedback from operation responses (e.g., to track version IDs).
type ResponseAwareGenerator interface {
	Generator
	OnResponse(*proto.ExecuteResponse)
}
