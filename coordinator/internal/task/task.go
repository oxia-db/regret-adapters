package task

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oxia-io/okk/coordinator/internal/task/generator"
	"github.com/oxia-io/okk/coordinator/internal/proto"
	osserrors "github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrRetryable        = errors.New("retryable error")
	ErrNonRetryable     = errors.New("non retryable error")
	ErrAssertionFailure = errors.New("assertion failure")

	operationLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_operation_duration_seconds",
		Help:    "Duration of task operations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"task_name", "status"})
)

type Task interface {
	io.Closer

	Run()

	Wait()
}

var _ Task = &task{}

type task struct {
	sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	generator       generator.Generator
	providerManager *ProviderManager
	name            string
	namespace       string
	worker          string
	status          *TaskStatus

	operations       atomic.Int64
	assertionsPassed atomic.Int64
	assertionsFailed atomic.Int64
}

func (t *task) Close() error {
	t.cancel()
	t.Wait()

	return nil
}

func (t *task) Run() {
	t.Add(1)
	go func() {
		defer t.WaitGroup.Done()

		err := backoff.RetryNotify(t.run, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
			t.logger.Error("Task running failed", "error", err, "retry-after", duration)
		})
		if err != nil {
			t.logger.Error("Task running failed", "error", err)
		}
	}()
}

func (t *task) run() error {
	var provider proto.OkkClient
	var err error
	if provider, err = t.providerManager.GetProvider(t.worker); err != nil {
		return err
	}
	stream, err := provider.Execute(t.ctx)
	if err != nil {
		return err
	}
	bo := backoff.NewExponentialBackOff()

	for {
		select {
		case <-t.ctx.Done():
			t.logger.Info("Task context done")
			return nil
		default:
			operation, hasNext := t.generator.Next()
			if !hasNext {
				return nil
			}
			err = backoff.RetryNotify(func() error {
				startTime := time.Now()
				if err := stream.Send(&proto.ExecuteCommand{
					Testcase:  t.name,
					Namespace: t.namespace,
					Operation: operation,
				}); err != nil {
					if errors.Is(err, io.EOF) {
						return backoff.Permanent(errors.New("stream closed"))
					}
					return err
				}
				response, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return backoff.Permanent(errors.New("stream closed"))
					}
					return err
				}
				status := response.Status
				switch status {
				case proto.Status_Ok:
					bo.Reset()
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_Ok.String()).Observe(time.Since(startTime).Seconds())
					t.operations.Add(1)
					if operation.Assertion != nil {
						t.assertionsPassed.Add(1)
					}
					if rag, ok := t.generator.(generator.ResponseAwareGenerator); ok {
						rag.OnResponse(response)
					}
					t.syncStatus()
					return nil
				case proto.Status_RetryableFailure:
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_RetryableFailure.String()).Observe(time.Since(startTime).Seconds())
					return osserrors.Wrap(ErrRetryable, response.StatusInfo)
				case proto.Status_NonRetryableFailure:
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_NonRetryableFailure.String()).Observe(time.Since(startTime).Seconds())
					return backoff.Permanent(osserrors.Wrap(ErrNonRetryable, response.StatusInfo))
				case proto.Status_AssertionFailure:
					assertion := operation.Assertion
					timestamp := operation.GetTimestamp()
					if assertion != nil && assertion.GetEventuallyEmpty() &&
						time.Since(time.Unix(0, timestamp)) < 5*time.Minute {
						operationLatencyHistogram.WithLabelValues(t.name, proto.Status_RetryableFailure.String()).Observe(time.Since(startTime).Seconds())
						return osserrors.Wrap(ErrRetryable, response.StatusInfo)
					}
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_AssertionFailure.String()).Observe(time.Since(startTime).Seconds())
					t.assertionsFailed.Add(1)
					failureMsg := response.StatusInfo
					t.status.LastFailure = &failureMsg
					t.syncStatus()
					return backoff.Permanent(osserrors.Wrap(ErrAssertionFailure, response.StatusInfo))
				default:
					operationLatencyHistogram.WithLabelValues(t.name, "Unknown").Observe(time.Since(startTime).Seconds())
					return errors.New("unknown status")
				}
			}, bo, func(err error, duration time.Duration) {
				t.logger.Error("Send command failed", "error", err, "retry-after", duration)
			})
			if err != nil {
				if errors.Is(err, ErrAssertionFailure) {
					return backoff.Permanent(err)
				}
				return err
			}
		}
	}
}

func (t *task) syncStatus() {
	t.status.Operations = t.operations.Load()
	t.status.AssertionsPassed = t.assertionsPassed.Load()
	t.status.AssertionsFailed = t.assertionsFailed.Load()
}

func NewTask(ctx context.Context, providerManager *ProviderManager,
	name string, namespace string, gen generator.Generator, worker string, status *TaskStatus) Task {
	currentContext, contextCancel := context.WithCancel(ctx)
	logger := slog.With("task", name)
	t := &task{
		ctx:             currentContext,
		cancel:          contextCancel,
		logger:          logger,
		WaitGroup:       sync.WaitGroup{},
		generator:       gen,
		name:            name,
		namespace:       namespace,
		worker:          worker,
		providerManager: providerManager,
		status:          status,
	}
	return t
}
