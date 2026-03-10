package generator

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/coordinator/internal/proto"
	"golang.org/x/time/rate"
)

var _ ResponseAwareGenerator = &conditionalPut{}

type keyState struct {
	versionId int64
	value     string
}

type conditionalPut struct {
	ctx    context.Context
	cancel context.CancelFunc
	name   string
	logger *slog.Logger

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time
	keySpace  int64

	needsCleanup bool
	initialized  bool
	sequence     int64

	// Track version per key. Missing key means key doesn't exist in Oxia.
	keys map[int64]*keyState

	// pendingKey tracks which key the last operation targeted,
	// so OnResponse can update the version cache.
	pendingKeyIndex int64
	pendingAction   int // 0=create, 1=update, 2=delete, 3=conflictCreate, 4=staleUpdate, 5=get
	pendingValue    string
}

func (c *conditionalPut) Name() string {
	return "conditional-put"
}

func (c *conditionalPut) OnResponse(resp *proto.ExecuteResponse) {
	if resp.VersionId == nil {
		return
	}
	vid := *resp.VersionId

	switch c.pendingAction {
	case 0: // create
		c.keys[c.pendingKeyIndex] = &keyState{versionId: vid, value: c.pendingValue}
	case 1: // update
		if ks, ok := c.keys[c.pendingKeyIndex]; ok {
			ks.versionId = vid
			ks.value = c.pendingValue
		}
	// case 2 (delete): state already updated eagerly in genDeleteAndRecreate
	// case 3,4 (conflict ops): state doesn't change
	}
}

func (c *conditionalPut) Next() (*proto.Operation, bool) {
	if c.needsCleanup {
		c.needsCleanup = false
		c.logger.Info("Cleaning up stale data from previous run", "prefix", c.name)
		return &proto.Operation{
			Operation: &proto.Operation_DeleteRange{
				DeleteRange: &proto.OperationDeleteRange{
					KeyStart: c.name,
					KeyEnd:   c.name + "~",
				},
			},
		}, true
	}

	if c.duration != nil && time.Since(c.startTime) > *c.duration {
		c.logger.Info("Finished conditional-put generator", "name", c.name)
		return nil, false
	}
	if err := c.rateLimit.Wait(c.ctx); err != nil {
		return nil, false
	}

	if !c.initialized {
		return c.processInitStage()
	}

	return c.processValidation()
}

func (c *conditionalPut) processInitStage() (*proto.Operation, bool) {
	idx := c.sequence
	c.sequence++

	uid := uuid.New().String()
	c.pendingKeyIndex = idx
	c.pendingAction = 0
	c.pendingValue = uid

	versionId := int64(-1) // must not exist
	if c.sequence >= c.keySpace {
		c.initialized = true
	}
	return &proto.Operation{
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:               makeKey(c.name, idx),
				Value:             makeValue(c.name, uid),
				ExpectedVersionId: &versionId,
			},
		},
	}, true
}

func (c *conditionalPut) processValidation() (*proto.Operation, bool) {
	// Weighted random action selection:
	// 25% conditional update, 25% get, 15% conflict create, 15% stale update, 10% delete+recreate, 10% unconditional put
	roll := rand.IntN(100)

	switch {
	case roll < 25:
		return c.genConditionalUpdate()
	case roll < 50:
		return c.genGet()
	case roll < 65:
		return c.genConflictCreate()
	case roll < 80:
		return c.genStaleUpdate()
	case roll < 90:
		return c.genDeleteAndRecreate()
	default:
		return c.genUnconditionalPut()
	}
}

// genConditionalUpdate: put with correct expected_version_id on an existing key.
func (c *conditionalPut) genConditionalUpdate() (*proto.Operation, bool) {
	idx, ks := c.pickExistingKey()
	if ks == nil {
		// No existing keys, fall back to get
		return c.genGet()
	}

	uid := uuid.New().String()
	c.pendingKeyIndex = idx
	c.pendingAction = 1
	c.pendingValue = uid

	vid := ks.versionId
	return &proto.Operation{
		Assertion: &proto.Assertion{
			Records: []*proto.Record{
				{
					Key:   makeKey(c.name, idx),
					Value: makeValue(c.name, uid),
				},
			},
		},
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:               makeKey(c.name, idx),
				Value:             makeValue(c.name, uid),
				ExpectedVersionId: &vid,
			},
		},
	}, true
}

// genGet: get a key and assert its value.
func (c *conditionalPut) genGet() (*proto.Operation, bool) {
	idx := rand.Int64N(c.keySpace)
	c.pendingKeyIndex = idx
	c.pendingAction = 5

	ks, exists := c.keys[idx]
	emptyRecord := !exists

	assertion := &proto.Assertion{
		EmptyRecords: &emptyRecord,
	}
	if exists {
		assertion.Records = []*proto.Record{
			{
				Key:   makeKey(c.name, idx),
				Value: makeValue(c.name, ks.value),
			},
		}
	}
	return &proto.Operation{
		Assertion: assertion,
		Operation: &proto.Operation_Get{
			Get: &proto.OperationGet{
				Key:            makeKey(c.name, idx),
				ComparisonType: proto.KeyComparisonType_EQUAL,
			},
		},
	}, true
}

// genConflictCreate: put with expected_version_id=-1 on a key that exists.
// This should fail with version conflict.
func (c *conditionalPut) genConflictCreate() (*proto.Operation, bool) {
	idx, ks := c.pickExistingKey()
	if ks == nil {
		return c.genGet()
	}

	c.pendingKeyIndex = idx
	c.pendingAction = 3

	uid := uuid.New().String()
	versionId := int64(-1)
	expectConflict := true
	return &proto.Operation{
		Assertion: &proto.Assertion{
			ExpectVersionConflict: &expectConflict,
		},
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:               makeKey(c.name, idx),
				Value:             makeValue(c.name, uid),
				ExpectedVersionId: &versionId,
			},
		},
	}, true
}

// genStaleUpdate: put with a wrong version on an existing key.
func (c *conditionalPut) genStaleUpdate() (*proto.Operation, bool) {
	idx, ks := c.pickExistingKey()
	if ks == nil {
		return c.genGet()
	}

	c.pendingKeyIndex = idx
	c.pendingAction = 4

	uid := uuid.New().String()
	// Use a stale version (current version - 1, or 0 if version is 0)
	staleVersion := ks.versionId - 1
	if staleVersion < 0 {
		staleVersion = ks.versionId + 999999
	}
	expectConflict := true
	return &proto.Operation{
		Assertion: &proto.Assertion{
			ExpectVersionConflict: &expectConflict,
		},
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:               makeKey(c.name, idx),
				Value:             makeValue(c.name, uid),
				ExpectedVersionId: &staleVersion,
			},
		},
	}, true
}

// genDeleteAndRecreate: delete a key, then it will be recreated next cycle.
func (c *conditionalPut) genDeleteAndRecreate() (*proto.Operation, bool) {
	idx, ks := c.pickExistingKey()
	if ks == nil {
		return c.genGet()
	}

	c.pendingKeyIndex = idx
	c.pendingAction = 2

	delete(c.keys, idx)
	return &proto.Operation{
		Operation: &proto.Operation_Delete{
			Delete: &proto.OperationDelete{
				Key: makeKey(c.name, idx),
			},
		},
	}, true
}

// genUnconditionalPut: conditional create on a deleted key to recreate it.
func (c *conditionalPut) genUnconditionalPut() (*proto.Operation, bool) {
	idx := c.pickDeletedKey()
	if idx < 0 {
		// No deleted keys available, fall back to get
		return c.genGet()
	}

	uid := uuid.New().String()
	c.pendingKeyIndex = idx
	c.pendingAction = 0
	c.pendingValue = uid

	versionId := int64(-1) // must not exist
	return &proto.Operation{
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:               makeKey(c.name, idx),
				Value:             makeValue(c.name, uid),
				ExpectedVersionId: &versionId,
			},
		},
	}, true
}

func (c *conditionalPut) pickExistingKey() (int64, *keyState) {
	// Try random keys up to 10 times to find an existing one
	for range 10 {
		idx := rand.Int64N(c.keySpace)
		if ks, ok := c.keys[idx]; ok {
			return idx, ks
		}
	}
	// Linear scan as fallback
	for idx, ks := range c.keys {
		return idx, ks
	}
	return 0, nil
}

func (c *conditionalPut) pickDeletedKey() int64 {
	for range 20 {
		idx := rand.Int64N(c.keySpace)
		if _, ok := c.keys[idx]; !ok {
			return idx
		}
	}
	// All keys exist
	return -1
}

func NewConditionalPut(ctx context.Context, tc *config.TestCaseConfig) Generator {
	currentCtx, cancel := context.WithCancel(ctx)
	logger := slog.With("generator", "conditional-put", "name", tc.Name)
	logger.Info("Starting conditional-put generator")

	keySpace := int64(100)
	if properties := tc.Properties; properties != nil {
		if num, exist := properties[propertiesKeyKeySpace]; exist {
			intVal, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				logger.Error("Failed to parse keySpace property, using default 100", "value", num, "error", err)
			} else {
				keySpace = intVal
			}
		}
	}

	opRate := tc.GetOpRate()
	return &conditionalPut{
		ctx:          currentCtx,
		cancel:       cancel,
		name:         tc.Name,
		logger:       logger,
		duration:     tc.GetDuration(),
		rateLimit:    rate.NewLimiter(rate.Limit(opRate), opRate),
		startTime:    time.Now(),
		keySpace:     keySpace,
		needsCleanup: true,
		keys:         make(map[int64]*keyState),
	}
}

