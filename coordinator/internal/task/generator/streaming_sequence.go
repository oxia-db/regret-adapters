package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/coordinator/internal/proto"
	"golang.org/x/time/rate"
)

var _ Generator = &streamingSequence{}

type streamingSequence struct {
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
	taskName string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time

	sequence    int64
	needsCleanup bool
}

func (s *streamingSequence) Name() string {
	return "streaming-sequence"
}

func (s *streamingSequence) Next() (*proto.Operation, bool) {
	if s.needsCleanup {
		s.needsCleanup = false
		s.logger.Info("Cleaning up stale data from previous run", "prefix", s.taskName)
		keyStart := s.taskName
		keyEnd := s.taskName + "~"
		return &proto.Operation{
			Timestamp: time.Now().UnixNano(),
			Sequence:  0,
			Operation: &proto.Operation_DeleteRange{
				DeleteRange: &proto.OperationDeleteRange{
					KeyStart: keyStart,
					KeyEnd:   keyEnd,
				},
			},
		}, true
	}

	if s.duration != nil && time.Since(s.startTime) > *s.duration {
		s.logger.Info("Finish the streaming sequence generator", "name", s.taskName)
		return nil, false
	}
	if err := s.rateLimit.Wait(s.ctx); err != nil {
		s.logger.Error("Failed to wait for rate limiter", "error", err)
		return nil, false
	}
	sequence := s.nextSequence()

	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		s.logger.Error("Failed to generate random sequence", "error", err)
		return nil, false
	}

	partitionKey := s.taskName
	bypassIfExist := true
	return &proto.Operation{
		Timestamp: time.Now().UnixNano(),
		Sequence:  sequence,
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:              s.taskName,
				Value:            b,
				PartitionKey:     &partitionKey,
				SequenceKeyDelta: []uint64{1, 2, 3},
			},
		},
		Precondition: &proto.Precondition{
			BypassIfAssertKeyExist: &bypassIfExist,
		},
		Assertion: &proto.Assertion{
			Records: []*proto.Record{
				{
					Key:   fmt.Sprintf("%s-%020d-%020d-%020d", s.taskName, sequence, sequence*2, sequence*3),
					Value: b,
				},
			},
			PartitionKey: &partitionKey,
		},
	}, true
}

func (s *streamingSequence) nextSequence() int64 {
	nextSequence := s.sequence
	s.sequence = s.sequence + 1
	return nextSequence
}

func NewStreamingSequence(ctx context.Context, tc *config.TestCaseConfig) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	logger := slog.With("generator", "streaming-sequence", "name", tc.Name)
	logger.Info("Starting streaming sequence generator")

	opRate := tc.GetOpRate()
	return &streamingSequence{
		logger:    logger,
		ctx:       currentContext,
		cancel:    currentContextCanceled,
		taskName:  tc.Name,
		duration:  tc.GetDuration(),
		startTime: time.Now(),
		rateLimit: rate.NewLimiter(rate.Limit(opRate), opRate),
		sequence:     1,
		needsCleanup: true,
	}
}
