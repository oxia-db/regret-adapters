package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/coordinator/internal/proto"
	"golang.org/x/time/rate"
)

const propertiesKeyKeySpace = "keySpace"

var _ Generator = &basicKv{}

type basicKv struct {
	ctx    context.Context
	cancel context.CancelFunc
	name   string
	logger *slog.Logger

	duration        *time.Duration
	rateLimit       *rate.Limiter
	startTime       time.Time
	keySpace        int64
	actionGenerator *ActionGenerator

	sequence int64

	initialized  bool
	needsCleanup bool
	data         *DataTree
}

func (b *basicKv) Name() string {
	return "basic-kv"
}

func (b *basicKv) Next() (*proto.Operation, bool) {
	if b.needsCleanup {
		b.needsCleanup = false
		b.logger.Info("Cleaning up stale data from previous run", "prefix", b.name)
		keyStart := b.name
		keyEnd := b.name + "~"
		return &proto.Operation{
			Operation: &proto.Operation_DeleteRange{
				DeleteRange: &proto.OperationDeleteRange{
					KeyStart: keyStart,
					KeyEnd:   keyEnd,
				},
			},
		}, true
	}

	if b.duration != nil && time.Since(b.startTime) > *b.duration {
		b.logger.Info("Finish the basic kv generator", "name", b.name)
		return nil, false
	}
	if err := b.rateLimit.Wait(b.ctx); err != nil {
		b.logger.Error("Failed to wait for rate limiter", "error", err)
		return nil, false
	}

	if !b.initialized {
		return b.processInitStage()
	}

	return b.processDataValidation()
}

func (b *basicKv) processDataValidation() (*proto.Operation, bool) {
	action := b.actionGenerator.Next()
	keyIndex := rand.Int64N(b.keySpace)
	switch action {
	case OpPut:
		{
			uid := uuid.New().String()
			b.data.Put(makeFormatInt64(keyIndex), uid)
			return &proto.Operation{
				Operation: &proto.Operation_Put{
					Put: &proto.OperationPut{
						Key:   makeKey(b.name, keyIndex),
						Value: makeValue(b.name, uid),
					},
				},
			}, true
		}
	case OpDelete:
		{
			b.data.Delete(makeFormatInt64(keyIndex))
			return &proto.Operation{
				Operation: &proto.Operation_Delete{
					Delete: &proto.OperationDelete{
						Key: makeKey(b.name, keyIndex),
					},
				},
			}, true
		}
	case OpDeleteRange:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			b.data.DeleteRange(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			return &proto.Operation{
				Operation: &proto.Operation_DeleteRange{
					DeleteRange: &proto.OperationDeleteRange{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	case OpGet:
		{
			value, found := b.data.Get(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   key,
						Value: makeValue(b.name, value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_EQUAL,
					},
				},
			}, true
		}
	case OpGetFloor:
		{
			entry, found := b.data.GetFloor(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_FLOOR,
					},
				},
			}, true
		}
	case OpGetCeiling:
		{
			entry, found := b.data.GetCeiling(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_CEILING,
					},
				},
			}, true
		}
	case OpGetHigher:
		{
			entry, found := b.data.GetHigher(makeFormatInt64(keyIndex))

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            makeKey(b.name, keyIndex),
						ComparisonType: proto.KeyComparisonType_HIGHER,
					},
				},
			}, true
		}
	case OpGetLower:
		{
			entry, found := b.data.GetLower(makeFormatInt64(keyIndex))
			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            makeKey(b.name, keyIndex),
						ComparisonType: proto.KeyComparisonType_LOWER,
					},
				},
			}, true
		}
	case OpList:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			keys := b.data.List(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			records := make([]*proto.Record, 0)
			for _, key := range keys {
				records = append(records, &proto.Record{
					Key: makeKeyWithFormattedIndex(b.name, key),
				})
			}

			return &proto.Operation{
				Assertion: &proto.Assertion{
					Records: records,
				},
				Operation: &proto.Operation_List{
					List: &proto.OperationList{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	case OpScan:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			entries := b.data.RangeScan(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			records := make([]*proto.Record, 0)
			for _, entry := range entries {
				records = append(records, &proto.Record{
					Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
					Value: makeValue(b.name, entry.Value),
				})
			}

			return &proto.Operation{
				Assertion: &proto.Assertion{
					Records: records,
				},
				Operation: &proto.Operation_Scan{
					Scan: &proto.OperationScan{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	}
	return nil, false
}

func (b *basicKv) processInitStage() (*proto.Operation, bool) {
	sequence := b.nextSequence()
	data := uuid.New().String()
	b.data.Put(makeFormatInt64(sequence), data)

	if sequence >= b.keySpace {
		b.initialized = true
	}
	return &proto.Operation{
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:   makeKey(b.name, sequence),
				Value: makeValue(b.name, data),
			},
		},
	}, true
}

func makeFormatInt64(value int64) string {
	return fmt.Sprintf("%020d", value)
}
func makeKeyWithFormattedIndex(name string, index string) string {
	return fmt.Sprintf("%s-%s", name, index)
}
func makeKey(name string, index int64) string {
	return fmt.Sprintf("%s-%020d", name, index)
}

func makeValue(name string, uid string) []byte {
	return []byte(fmt.Sprintf("%s-%s", name, uid))
}

func (b *basicKv) nextSequence() int64 {
	nextSequence := b.sequence
	b.sequence = b.sequence + 1
	return nextSequence
}

func NewBasicKv(ctx context.Context, tc *config.TestCaseConfig) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	logger := slog.With("generator", "basic-kv", "name", tc.Name)
	logger.Info("Starting basic kv generator")

	keySpace := int64(1000)
	if properties := tc.Properties; properties != nil {
		if num, exist := properties[propertiesKeyKeySpace]; exist {
			intVal, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				logger.Error("Failed to parse keySpace property, using default 1000", "value", num, "error", err)
			} else {
				keySpace = intVal
			}
		}
	}

	opRate := tc.GetOpRate()
	actionGenerator := NewActionGenerator(map[OpType]int{
		OpPut:         10,
		OpDelete:      10,
		OpGet:         10,
		OpGetFloor:    10,
		OpGetCeiling:  10,
		OpGetHigher:   10,
		OpGetLower:    10,
		OpList:        10,
		OpScan:        10,
		OpDeleteRange: 10,
	})
	bkv := basicKv{
		logger:          logger,
		ctx:             currentContext,
		cancel:          currentContextCanceled,
		actionGenerator: actionGenerator,
		name:            tc.Name,
		duration:        tc.GetDuration(),
		startTime:       time.Now(),
		sequence:        0,
		initialized:     false,
		needsCleanup:    true,
		keySpace:        keySpace,
		rateLimit:       rate.NewLimiter(rate.Limit(opRate), opRate),
		data:            NewDataTree(),
	}
	return &bkv
}
