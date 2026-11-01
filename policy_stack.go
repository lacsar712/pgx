package pgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/internal/stmtcache"
)

// QueryExecOverlay is the query_exec_overlay axis of a [PolicyFrame]. It overrides how the frame's
// [Conn.Query], [Conn.QueryRow], [Conn.Exec], [Tx.Query], [Tx.QueryRow], and [Tx.Exec] calls choose
// their wire execution mode. Unlike a one-off [QueryExecMode] argument, an overlay is mandatory for
// the whole frame: it wins over [ConnConfig.DefaultQueryExecMode] and over any [QueryExecMode] passed
// as a per-call argument.
type QueryExecOverlay int8

const (
	// QueryExecOverlayPin keeps the existing mode selection ([ConnConfig.DefaultQueryExecMode], then
	// any per-call [QueryExecMode] argument) and the existing cache behavior.
	QueryExecOverlayPin QueryExecOverlay = iota

	// QueryExecOverlayForceSimple forces the frame's Query/QueryRow/Exec calls through the simple
	// protocol. Named prepared statements are never used to satisfy the calls, even when the SQL text
	// matches a previously prepared statement name.
	QueryExecOverlayForceSimple

	// QueryExecOverlayForceCacheStatement forces the frame's Query/QueryRow/Exec calls through the
	// CacheStatement path: same-shape SQL is prepared under a stable name once and subsequent
	// executions bind to that named prepared statement.
	QueryExecOverlayForceCacheStatement

	// QueryExecOverlayForceDescribeExec forces the frame's Query/QueryRow/Exec calls through the
	// DescribeExec path: the unnamed prepared statement is described and the query executed in two
	// round trips on every call. The call never silently falls back to CacheStatement.
	QueryExecOverlayForceDescribeExec
)

// TxAdmit is the tx_admit axis of a [PolicyFrame]. It controls whether transactions may be opened
// while the frame is active.
type TxAdmit int8

const (
	// TxAdmitPin keeps the existing Begin behavior.
	TxAdmitPin TxAdmit = iota

	// TxAdmitRejectBegin makes [Conn.Begin], [Conn.BeginTx], nested [Tx.Begin], and the pool's
	// Begin/BeginTx fail with an error wrapping [ErrPolicyBeginRejected] without sending anything to
	// the server.
	TxAdmitRejectBegin

	// TxAdmitForceRollbackOnExit rolls back every real transaction opened while the frame is active
	// when the frame is popped, unless it was already committed or rolled back. It never relies on
	// the caller remembering to commit.
	TxAdmitForceRollbackOnExit
)

// BatchAdmit is the batch_admit axis of a [PolicyFrame]. It controls [Conn.SendBatch] (and, through
// it, [Tx.SendBatch] and the pool's SendBatch) while the frame is active.
type BatchAdmit int8

const (
	// BatchAdmitPin keeps the existing SendBatch behavior.
	BatchAdmitPin BatchAdmit = iota

	// BatchAdmitRejectSend makes SendBatch fail with an error wrapping [ErrPolicyBatchRejected] and
	// leaves the [Batch] and its queued queries untouched; nothing is written to the connection.
	BatchAdmitRejectSend

	// BatchAdmitForcePipeline forces SendBatch through the observable PostgreSQL pipeline path
	// ([pgconn.PgConn.StartPipeline], SendQuery*, Sync and flush), even when the effective exec mode
	// would normally use the non-pipeline simple-protocol or ExecBatch sends.
	BatchAdmitForcePipeline
)

// StmtCacheGate is the stmt_cache_gate axis of a [PolicyFrame]. It controls the connection's
// statement and description caches while the frame is active.
type StmtCacheGate int8

const (
	// StmtCacheGatePin keeps the existing StatementCache/DescriptionCache behavior.
	StmtCacheGatePin StmtCacheGate = iota

	// StmtCacheGateForbidCache makes the frame neither read from nor write to the statement and
	// description caches. Named prepared statements explicitly created inside the frame are
	// deallocated when the frame is popped, so later frames cannot reuse them. A frame with this
	// gate cannot be pushed after a statement has already been prepared or cached; [Conn.PushPolicyFrame]
	// returns an error wrapping [ErrPolicyConflict] in that case (use [Conn.DeallocateAll] first).
	StmtCacheGateForbidCache

	// StmtCacheGateForceDeallocateOnPop observably deallocates, when the frame is popped, every
	// named prepared statement created while the frame was active and removes the frame's statement
	// and description cache entries.
	StmtCacheGateForceDeallocateOnPop
)

// PolicyFrame is one layer of the connection-bound policy stack. The zero value is PolicyFrame{}
// and every axis's zero value is its "pin" setting, i.e. "keep existing behavior".
type PolicyFrame struct {
	QueryExecOverlay QueryExecOverlay
	TxAdmit          TxAdmit
	BatchAdmit       BatchAdmit
	StmtCacheGate    StmtCacheGate
}

// EffectivePolicy is the merged read-only view of the policy stack top together with the
// connection configuration and live connection state.
type EffectivePolicy struct {
	// Frame is the resolved stack-top frame. It is the zero PolicyFrame when the stack is empty.
	Frame PolicyFrame

	// Depth is the number of frames currently pushed on this connection.
	Depth int

	// DefaultQueryExecMode is the configured default; it never changes while frames are pushed.
	DefaultQueryExecMode QueryExecMode

	// QueryExecMode is the mode a query without an explicit per-call [QueryExecMode] argument will
	// take after applying QueryExecOverlay and StmtCacheGate.
	QueryExecMode QueryExecMode

	// StatementCacheCapacity and DescriptionCacheCapacity are the configured cache capacities.
	StatementCacheCapacity   int
	DescriptionCacheCapacity int

	// StatementCacheEnabled and DescriptionCacheEnabled report whether the respective cache exists
	// and is usable by the current frame (false under StmtCacheGateForbidCache).
	StatementCacheEnabled   bool
	DescriptionCacheEnabled bool

	// InTx reports whether the connection is currently inside a transaction.
	InTx bool

	// InBatch reports whether a batch sent on the connection is still open.
	InBatch bool
}

var (
	// ErrPolicyBeginRejected is returned (wrapped) by Begin/BeginTx when a TxAdmitRejectBegin frame
	// is active.
	ErrPolicyBeginRejected = errors.New("pgx: begin rejected by connection policy frame (tx_admit=reject_begin)")

	// ErrPolicyBatchRejected is returned (wrapped) by the BatchResults when a BatchAdmitRejectSend
	// frame is active.
	ErrPolicyBatchRejected = errors.New("pgx: send batch rejected by connection policy frame (batch_admit=reject_send)")

	// ErrPolicyConflict is returned by PushPolicyFrame when a frame cannot be pushed because of the
	// connection's current state, e.g. reject_begin pushed after a transaction already started, a
	// query overlay pushed while a query is in flight, or forbid_cache pushed after statements were
	// already prepared.
	ErrPolicyConflict = errors.New("pgx: policy frame conflicts with current connection state")

	// ErrPolicyStackEmpty is returned when popping a frame from an empty policy stack.
	ErrPolicyStackEmpty = errors.New("pgx: policy stack is empty")

	// ErrPolicyFrameNotTop is returned when trying to pop a frame that is not the stack top anymore.
	ErrPolicyFrameNotTop = errors.New("pgx: policy frame is not the top of the stack")
)

// ActivePolicyFrame is a pushed frame handle. Use Pop (or [Conn.WithPolicyFrame], which pops
// automatically) to remove it.
type ActivePolicyFrame struct {
	stack  *policyStack
	frame  *policyFrame
	popped bool
}

// Pop removes the frame from the connection's stack and runs its exit actions: rolling back
// transactions opened inside a TxAdmitForceRollbackOnExit frame and deallocating prepared
// statements governed by the stmt_cache_gate axis. It is safe to call once; later calls are
// no-ops for the same handle.
func (h *ActivePolicyFrame) Pop(ctx context.Context) error {
	if h == nil || h.stack == nil {
		return ErrPolicyStackEmpty
	}
	return h.stack.popHandle(ctx, h)
}

// PolicyFrameCounters holds observable per-frame counters. It is intended for tests and
// diagnostics.
type PolicyFrameCounters struct {
	QueriesSimple         int64
	QueriesCacheStatement int64
	QueriesCacheDescribe  int64
	QueriesDescribeExec   int64
	QueriesExec           int64
	CopyFrom              int64
	BeginsOpened          int64
	BeginsRejected        int64
	BatchesRejected       int64
	PipelineBatches       int64
	SimpleBatches         int64
	RollbacksOnPop        int64
	DeallocationsOnPop    int64
	Prepares              int64
}

type autoPrepared struct {
	name string
	sql  string
}

type policyFrame struct {
	axis PolicyFrame

	// Real transactions opened while this frame was active.
	tx []*dbTx

	// Keys in Conn.preparedStatements of explicitly named prepared statements created while the
	// frame was active.
	explicitPrepares map[string]struct{}

	// Statement-cache (auto-named) prepared statements created while the frame was active.
	autoPrepares []autoPrepared

	// SQL texts put into the description cache while the frame was active.
	describeSQLs map[string]struct{}

	counters PolicyFrameCounters
}

type policyStack struct {
	conn *Conn

	mu     sync.Mutex
	frames []*policyFrame

	// Number of batches on this connection whose results have not been closed yet.
	batchDepth int
}

func newPolicyStack(c *Conn) *policyStack {
	return &policyStack{conn: c}
}

func (s *policyStack) depth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames)
}

func (s *policyStack) top() *policyFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.topLocked()
}

func (s *policyStack) topLocked() *policyFrame {
	if n := len(s.frames); n > 0 {
		return s.frames[n-1]
	}
	return nil
}

func (s *policyStack) push(frame PolicyFrame) (*ActivePolicyFrame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := s.conn

	// Late-push conflict rules. A frame must never silently govern an operation that already
	// crossed the wire; when such a situation is detected the push is rejected outright.
	if c.pgConn != nil && !c.IsClosed() {
		busy := c.pgConn.IsBusy()
		if busy && frame.QueryExecOverlay != QueryExecOverlayPin {
			return nil, fmt.Errorf("%w: cannot push query_exec_overlay while a query is already in flight", ErrPolicyConflict)
		}
		if busy && frame.BatchAdmit != BatchAdmitPin {
			return nil, fmt.Errorf("%w: cannot push batch_admit while a batch is already being written", ErrPolicyConflict)
		}
		// reject_begin cannot be pushed once a transaction is already open unless that
		// transaction was itself admitted by an outer frame: in that case the new frame only
		// governs begins made after it (such as nested savepoint begins) and never retroactively
		// rejects the outer transaction. With no frame active at push time the transaction
		// predates the whole stack, so the push is rejected rather than silently inheriting the
		// pre-frame state.
		if frame.TxAdmit == TxAdmitRejectBegin && c.pgConn.TxStatus() != 'I' && len(s.frames) == 0 {
			return nil, fmt.Errorf("%w: cannot push tx_admit=reject_begin while a transaction is already open", ErrPolicyConflict)
		}
		if frame.StmtCacheGate == StmtCacheGateForbidCache {
			if len(c.preparedStatements) > 0 ||
				(c.statementCache != nil && c.statementCache.Len() > 0) ||
				(c.descriptionCache != nil && c.descriptionCache.Len() > 0) {
				return nil, fmt.Errorf("%w: cannot push stmt_cache_gate=forbid_cache after statements have been prepared or cached", ErrPolicyConflict)
			}
		}
	}

	f := &policyFrame{
		axis:             frame,
		explicitPrepares: make(map[string]struct{}),
		describeSQLs:     make(map[string]struct{}),
	}
	s.frames = append(s.frames, f)

	return &ActivePolicyFrame{stack: s, frame: f}, nil
}

func (s *policyStack) popHandle(ctx context.Context, h *ActivePolicyFrame) error {
	s.mu.Lock()
	if h.popped {
		s.mu.Unlock()
		return nil
	}
	if len(s.frames) == 0 {
		s.mu.Unlock()
		return ErrPolicyStackEmpty
	}
	if s.frames[len(s.frames)-1] != h.frame {
		s.mu.Unlock()
		return ErrPolicyFrameNotTop
	}
	s.frames = s.frames[:len(s.frames)-1]
	f := h.frame
	h.popped = true
	s.mu.Unlock()

	return f.exit(ctx, s.conn)
}

func (s *policyStack) popTop(ctx context.Context) error {
	s.mu.Lock()
	if len(s.frames) == 0 {
		s.mu.Unlock()
		return ErrPolicyStackEmpty
	}
	f := s.frames[len(s.frames)-1]
	s.frames = s.frames[:len(s.frames)-1]
	s.mu.Unlock()

	return f.exit(ctx, s.conn)
}

// exit runs the frame's exit actions. It executes after the frame has already been unlinked, so
// exit actions observe the outer frame.
func (f *policyFrame) exit(ctx context.Context, c *Conn) (err error) {
	if f.axis.TxAdmit == TxAdmitForceRollbackOnExit {
		for _, tx := range f.tx {
			if !tx.closed && c.pgConn.TxStatus() != 'I' {
				if rbErr := tx.Rollback(ctx); rbErr != nil {
					err = errors.Join(err, fmt.Errorf("policy frame force rollback on exit: %w", rbErr))
				}
				f.counters.RollbacksOnPop++
			}
		}
	}

	switch f.axis.StmtCacheGate {
	case StmtCacheGateForbidCache, StmtCacheGateForceDeallocateOnPop:
		for key := range f.explicitPrepares {
			if _, ok := c.preparedStatements[key]; ok {
				if dErr := c.Deallocate(ctx, key); dErr != nil {
					err = errors.Join(err, fmt.Errorf("policy frame deallocate on pop: %w", dErr))
				}
				f.counters.DeallocationsOnPop++
			}
		}

		if len(f.autoPrepares) > 0 && c.statementCache != nil {
			for _, a := range f.autoPrepares {
				c.statementCache.Invalidate(a.sql)
			}
			// deallocateInvalidatedCachedStatements sends DEALLOCATE for the frame's auto named
			// statements and removes them from both caches and preparedStatements.
			before := f.counters.DeallocationsOnPop
			if dErr := c.deallocateInvalidatedCachedStatements(ctx); dErr != nil {
				err = errors.Join(err, fmt.Errorf("policy frame deallocate on pop: %w", dErr))
			}
			f.counters.DeallocationsOnPop = before + int64(len(f.autoPrepares))
		}

		if len(f.describeSQLs) > 0 && c.descriptionCache != nil {
			for sql := range f.describeSQLs {
				c.descriptionCache.Invalidate(sql)
			}
			// Description invalidation needs no server round trip; drop the marked entries now.
			c.descriptionCache.GetInvalidated()
			c.descriptionCache.RemoveInvalidated()
		}
	}

	return err
}

func (s *policyStack) noteQuery(mode QueryExecMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.topLocked()
	if f == nil {
		return
	}
	switch mode {
	case QueryExecModeSimpleProtocol:
		f.counters.QueriesSimple++
	case QueryExecModeCacheStatement:
		f.counters.QueriesCacheStatement++
	case QueryExecModeCacheDescribe:
		f.counters.QueriesCacheDescribe++
	case QueryExecModeDescribeExec:
		f.counters.QueriesDescribeExec++
	case QueryExecModeExec:
		f.counters.QueriesExec++
	}
}

func (s *policyStack) noteCopyFrom() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.counters.CopyFrom++
	}
}

func (s *policyStack) noteBeginOpened(tx *dbTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.frames {
		f.tx = append(f.tx, tx)
		f.counters.BeginsOpened++
	}
}

func (s *policyStack) noteBeginRejected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.counters.BeginsRejected++
	}
}

func (s *policyStack) noteBatchRejected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.counters.BatchesRejected++
	}
}

func (s *policyStack) notePipelineBatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.counters.PipelineBatches++
	}
}

func (s *policyStack) noteSimpleBatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.counters.SimpleBatches++
	}
}

func (s *policyStack) noteBatchOpen() {
	s.mu.Lock()
	s.batchDepth++
	s.mu.Unlock()
}

func (s *policyStack) noteBatchClosed() {
	s.mu.Lock()
	if s.batchDepth > 0 {
		s.batchDepth--
	}
	s.mu.Unlock()
}

// notePrepare records a prepared statement successfully created on the server while a frame is
// active. psKey is the key in Conn.preparedStatements ("" for unnamed prepares, which are not
// tracked), psName the server-side statement name, sql the prepared SQL text.
func (s *policyStack) notePrepare(psKey, psName, sql string) {
	if psKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.topLocked()
	if f == nil {
		return
	}
	f.counters.Prepares++
	if psName == stmtcache.StatementName(sql) {
		f.autoPrepares = append(f.autoPrepares, autoPrepared{name: psName, sql: sql})
	} else {
		f.explicitPrepares[psKey] = struct{}{}
	}
}

// noteAutoPrepared records a statement-cache style named prepared statement created through a
// batch pipeline (which bypasses Conn.Prepare).
func (s *policyStack) noteAutoPrepared(name, sql string) {
	if name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.topLocked()
	if f == nil {
		return
	}
	for _, a := range f.autoPrepares {
		if a.name == name {
			return
		}
	}
	f.autoPrepares = append(f.autoPrepares, autoPrepared{name: name, sql: sql})
}

func (s *policyStack) noteDescriptionCached(sql string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.topLocked(); f != nil {
		f.describeSQLs[sql] = struct{}{}
	}
}

// noteDeallocate forgets a deallocated prepared statement across all active frames so it is not
// deallocated again on pop.
func (s *policyStack) noteDeallocate(key, serverName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.frames {
		delete(f.explicitPrepares, key)
		filtered := f.autoPrepares[:0]
		for _, a := range f.autoPrepares {
			if a.name != serverName && a.name != key {
				filtered = append(filtered, a)
			}
		}
		f.autoPrepares = filtered
	}
}

// noteDeallocateAll forgets every tracked prepared statement and cache entry, matching a
// Conn.DeallocateAll that removed them all outside frame control.
func (s *policyStack) noteDeallocateAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.frames {
		f.explicitPrepares = map[string]struct{}{}
		f.autoPrepares = nil
		f.describeSQLs = map[string]struct{}{}
	}
}

// effectiveQueryExecMode resolves the query exec mode for an exit, applying the frame's
// query_exec_overlay and stmt_cache_gate. explicit indicates whether the caller passed a per-call
// QueryExecMode argument.
func (c *Conn) effectiveQueryExecMode(frame *policyFrame, explicit bool, argMode QueryExecMode) QueryExecMode {
	mode := c.config.DefaultQueryExecMode
	if explicit {
		mode = argMode
	}
	if frame == nil {
		return mode
	}

	switch frame.axis.QueryExecOverlay {
	case QueryExecOverlayForceSimple:
		mode = QueryExecModeSimpleProtocol
	case QueryExecOverlayForceCacheStatement:
		mode = QueryExecModeCacheStatement
	case QueryExecOverlayForceDescribeExec:
		mode = QueryExecModeDescribeExec
	}

	if frame.axis.StmtCacheGate == StmtCacheGateForbidCache {
		if mode == QueryExecModeCacheStatement || mode == QueryExecModeCacheDescribe {
			mode = QueryExecModeDescribeExec
		}
	}

	return mode
}

// PushPolicyFrame pushes a [PolicyFrame] onto this connection's policy stack and returns a handle
// for popping it. The stack belongs to this [Conn]: it is never shared with another Conn, not even
// a Conn used from the same goroutine, and it is independent of [ConnConfig] and the pool
// configuration. Transactions ([Tx]), batches and copy operations started on the Conn observe the
// same stack.
//
// PushPolicyFrame returns an error wrapping [ErrPolicyConflict] when the frame cannot govern the
// connection in its current state (see the axis documentation). No constructor parameters are
// involved; a connection with an empty stack behaves exactly as before.
func (c *Conn) PushPolicyFrame(ctx context.Context, frame PolicyFrame) (*ActivePolicyFrame, error) {
	return c.policy.push(frame)
}

// PopPolicyFrame pops the top frame of this connection's policy stack. Prefer the handle returned
// by [Conn.PushPolicyFrame] or [Conn.WithPolicyFrame] so frames cannot be popped out of order.
func (c *Conn) PopPolicyFrame(ctx context.Context) error {
	return c.policy.popTop(ctx)
}

// WithPolicyFrame pushes frame, runs fn, and always pops the frame again (including on error or
// panic). Exit actions such as force rollback or deallocate on pop run before WithPolicyFrame
// returns.
func (c *Conn) WithPolicyFrame(ctx context.Context, frame PolicyFrame, fn func(ctx context.Context) error) (err error) {
	active, pushErr := c.PushPolicyFrame(ctx, frame)
	if pushErr != nil {
		return pushErr
	}

	defer func() {
		if p := recover(); p != nil {
			_ = active.Pop(ctx)
			panic(p)
		}
		if popErr := active.Pop(ctx); err == nil {
			err = popErr
		}
	}()

	return fn(ctx)
}

// EffectivePolicy returns the merged view of the stack top, the connection configuration and the
// current transaction/batch state.
func (c *Conn) EffectivePolicy() EffectivePolicy {
	c.policy.mu.Lock()
	depth := len(c.policy.frames)
	batchDepth := c.policy.batchDepth
	var frame *policyFrame
	if depth > 0 {
		frame = c.policy.frames[depth-1]
	}
	c.policy.mu.Unlock()

	var pf PolicyFrame
	if frame != nil {
		pf = frame.axis
	}

	mode := c.effectiveQueryExecMode(frame, false, 0)

	return EffectivePolicy{
		Frame:                    pf,
		Depth:                    depth,
		DefaultQueryExecMode:     c.config.DefaultQueryExecMode,
		QueryExecMode:            mode,
		StatementCacheCapacity:   c.config.StatementCacheCapacity,
		DescriptionCacheCapacity: c.config.DescriptionCacheCapacity,
		StatementCacheEnabled:    c.statementCache != nil && (frame == nil || frame.axis.StmtCacheGate != StmtCacheGateForbidCache),
		DescriptionCacheEnabled:  c.descriptionCache != nil && (frame == nil || frame.axis.StmtCacheGate != StmtCacheGateForbidCache),
		InTx:                     c.pgConn.TxStatus() != 'I',
		InBatch:                  batchDepth > 0,
	}
}

// PolicyFrameCounters returns a snapshot of the frame's observable counters. The counters are part
// of the diagnostic surface and are primarily intended for tests.
func (h *ActivePolicyFrame) Counters() PolicyFrameCounters {
	if h == nil || h.stack == nil {
		return PolicyFrameCounters{}
	}
	h.stack.mu.Lock()
	defer h.stack.mu.Unlock()
	return h.frame.counters
}

// String renders a PolicyFrame for diagnostics.
func (f PolicyFrame) String() string {
	parts := make([]string, 0, 4)
	if f.QueryExecOverlay != QueryExecOverlayPin {
		parts = append(parts, "query_exec_overlay="+f.QueryExecOverlay.String())
	}
	if f.TxAdmit != TxAdmitPin {
		parts = append(parts, "tx_admit="+f.TxAdmit.String())
	}
	if f.BatchAdmit != BatchAdmitPin {
		parts = append(parts, "batch_admit="+f.BatchAdmit.String())
	}
	if f.StmtCacheGate != StmtCacheGatePin {
		parts = append(parts, "stmt_cache_gate="+f.StmtCacheGate.String())
	}
	if len(parts) == 0 {
		return "PolicyFrame{pin}"
	}
	return "PolicyFrame{" + strings.Join(parts, ", ") + "}"
}

// String renders the axis value.
func (o QueryExecOverlay) String() string {
	switch o {
	case QueryExecOverlayPin:
		return "pin"
	case QueryExecOverlayForceSimple:
		return "force_simple"
	case QueryExecOverlayForceCacheStatement:
		return "force_cache_statement"
	case QueryExecOverlayForceDescribeExec:
		return "force_describe_exec"
	default:
		return "unknown"
	}
}

// String renders the axis value.
func (a TxAdmit) String() string {
	switch a {
	case TxAdmitPin:
		return "pin"
	case TxAdmitRejectBegin:
		return "reject_begin"
	case TxAdmitForceRollbackOnExit:
		return "force_rollback_on_exit"
	default:
		return "unknown"
	}
}

// String renders the axis value.
func (a BatchAdmit) String() string {
	switch a {
	case BatchAdmitPin:
		return "pin"
	case BatchAdmitRejectSend:
		return "reject_send"
	case BatchAdmitForcePipeline:
		return "force_pipeline"
	default:
		return "unknown"
	}
}

// String renders the axis value.
func (g StmtCacheGate) String() string {
	switch g {
	case StmtCacheGatePin:
		return "pin"
	case StmtCacheGateForbidCache:
		return "forbid_cache"
	case StmtCacheGateForceDeallocateOnPop:
		return "force_deallocate_on_pop"
	default:
		return "unknown"
	}
}
