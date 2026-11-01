package pgx

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// recordedMsg is a value snapshot of a frontend message taken at receive time. pgproto3 reuses
// decoded message structs, so keeping the pointers would alias the last decoded message.
type recordedMsg struct {
	typeName   string
	query      string
	name       string
	objectType byte
}

// mockScriptedServer is a tiny scriptable PostgreSQL server. It records every frontend message and
// replies through a small default extended/simple protocol responder so policy behavior can be
// observed on the wire without a real database.
type mockScriptedServer struct {
	t        *testing.T
	ln       net.Listener
	serverNc net.Conn

	mu   sync.Mutex
	msgs []recordedMsg

	done chan struct{}
}

func startMockScriptedServer(t *testing.T) *mockScriptedServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &mockScriptedServer{t: t, ln: ln, done: make(chan struct{})}

	go func() {
		defer close(s.done)
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		s.serverNc = nc
		defer nc.Close()
		_ = nc.SetDeadline(time.Now().Add(10 * time.Second))

		backend := pgproto3.NewBackend(nc, nc)

		startup, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		if _, ok := startup.(*pgproto3.StartupMessage); !ok {
			s.t.Errorf("expected startup message, got %T", startup)
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		// simple protocol requires these parameter statuses
		backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: []byte{0, 0, 0, 0}})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		require.NoError(s.t, backend.Flush())

		txStatus := byte('I')
		var lastParse string

		record := func(msg pgproto3.FrontendMessage) {
			rm := recordedMsg{typeName: frontendTypeName(msg)}
			switch m := msg.(type) {
			case *pgproto3.Query:
				rm.query = m.String
			case *pgproto3.Parse:
				rm.name = m.Name
			case *pgproto3.Close:
				rm.name = m.Name
				rm.objectType = m.ObjectType
			}
			s.mu.Lock()
			s.msgs = append(s.msgs, rm)
			s.mu.Unlock()
		}

		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}

			record(msg)

			switch msg := msg.(type) {
			case *pgproto3.Terminate:
				return

			case *pgproto3.Parse:
				lastParse = msg.Query
				backend.Send(&pgproto3.ParseComplete{})

			case *pgproto3.Describe:
				if msg.ObjectType == 'S' {
					if strings.Contains(lastParse, "$1") {
						backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}})
					} else {
						backend.Send(&pgproto3.ParameterDescription{})
					}
				}
				if strings.Contains(strings.ToLower(lastParse), "select") || msg.ObjectType == 'P' {
					backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
						{Name: []byte("n"), DataTypeOID: 23},
					}})
				} else {
					backend.Send(&pgproto3.NoData{})
				}

			case *pgproto3.Bind:
				backend.Send(&pgproto3.BindComplete{})

			case *pgproto3.Execute:
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})

			case *pgproto3.Close:
				backend.Send(&pgproto3.CloseComplete{})

			case *pgproto3.Sync:
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})

			case *pgproto3.Flush:
				// nothing

			case *pgproto3.Query:
				sql := msg.String
				switch {
				case strings.HasPrefix(sql, "begin"):
					txStatus = 'T'
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
				case strings.HasPrefix(sql, "commit"):
					txStatus = 'I'
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
				case strings.HasPrefix(sql, "rollback"):
					txStatus = 'I'
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
				case strings.HasPrefix(sql, "deallocate"):
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("DEALLOCATE")})
				case strings.HasPrefix(sql, "copy "):
					backend.Send(&pgproto3.CopyInResponse{OverallFormat: 1, ColumnFormatCodes: []uint16{1}})
					require.NoError(s.t, backend.Flush())
					// drain copy data until CopyDone
					for {
						m, err := backend.Receive()
						if err != nil {
							return
						}
						record(m)
						switch m.(type) {
						case *pgproto3.CopyDone:
							backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")})
							backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
							goto flushed
						case *pgproto3.CopyFail:
							backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "copy fail"})
							backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
							goto flushed
						}
					}
				default:
					for range strings.Count(sql, ";") + 1 {
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
					}
				}
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
			}

		flushed:
			require.NoError(s.t, backend.Flush())
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		if s.serverNc != nil {
			_ = s.serverNc.Close()
		}
		<-s.done
	})

	return s
}

func frontendTypeName(m pgproto3.FrontendMessage) string {
	switch m.(type) {
	case *pgproto3.Query:
		return "Query"
	case *pgproto3.Parse:
		return "Parse"
	case *pgproto3.Bind:
		return "Bind"
	case *pgproto3.Describe:
		return "Describe"
	case *pgproto3.Execute:
		return "Execute"
	case *pgproto3.Sync:
		return "Sync"
	case *pgproto3.Close:
		return "Close"
	case *pgproto3.CopyData:
		return "CopyData"
	case *pgproto3.CopyDone:
		return "CopyDone"
	case *pgproto3.Terminate:
		return "Terminate"
	default:
		return "other"
	}
}

func (s *mockScriptedServer) dialConfig(defaultMode string) *ConnConfig {
	s.t.Helper()
	_, port, _ := strings.Cut(s.ln.Addr().String(), ":")
	connString := "sslmode=disable host=127.0.0.1 port=" + port
	if defaultMode != "" {
		connString += " default_query_exec_mode=" + defaultMode
	}
	config, err := ParseConfig(connString)
	require.NoError(s.t, err)
	return config
}

func (s *mockScriptedServer) connect(t *testing.T) *Conn {
	t.Helper()
	return s.connectWithMode(t, "")
}

func (s *mockScriptedServer) connectWithMode(t *testing.T, defaultMode string) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := ConnectConfig(ctx, s.dialConfig(defaultMode))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.Close(context.Background())
	})
	return c
}

func (s *mockScriptedServer) recorded() []recordedMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]recordedMsg, len(s.msgs))
	copy(out, s.msgs)
	return out
}

func (s *mockScriptedServer) countTypes() map[string]int {
	counts := make(map[string]int)
	for _, m := range s.recorded() {
		counts[m.typeName]++
	}
	return counts
}

func (s *mockScriptedServer) queries() []string {
	var out []string
	for _, m := range s.recorded() {
		if m.typeName == "Query" {
			out = append(out, m.query)
		}
	}
	return out
}

func (s *mockScriptedServer) closeNames() []string {
	var out []string
	for _, m := range s.recorded() {
		if m.typeName == "Close" && m.objectType == 'S' {
			out = append(out, m.name)
		}
	}
	return out
}

// EffectivePolicy with no frame is exactly the configured view: empty stack == baseline.
func TestPolicyStackEmptyIsBaseline(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "exec")

	eff := c.EffectivePolicy()
	require.Equal(t, 0, eff.Depth)
	require.Equal(t, PolicyFrame{}, eff.Frame)
	require.Equal(t, QueryExecModeExec, eff.DefaultQueryExecMode)
	require.Equal(t, QueryExecModeExec, eff.QueryExecMode)
	require.True(t, eff.StatementCacheEnabled)
	require.True(t, eff.DescriptionCacheEnabled)
	require.False(t, eff.InTx)
	require.False(t, eff.InBatch)

	require.ErrorIs(t, c.PopPolicyFrame(context.Background()), ErrPolicyStackEmpty)
}

// Frames are bound to their Conn: two Conns on the same goroutine never share a stack top, and the
// same holds with concurrent goroutines pushing different overlays.
func TestPolicyStackPerConnIsolation(t *testing.T) {
	t.Parallel()

	s1 := startMockScriptedServer(t)
	s2 := startMockScriptedServer(t)
	c1 := s1.connect(t)
	c2 := s2.connect(t)

	// Same goroutine: push force_simple on c1; c2's view must stay baseline.
	h1, err := c1.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
	require.NoError(t, err)
	require.Equal(t, QueryExecModeSimpleProtocol, c1.EffectivePolicy().QueryExecMode)
	require.Equal(t, 0, c2.EffectivePolicy().Depth)
	require.Equal(t, QueryExecModeCacheStatement, c2.EffectivePolicy().QueryExecMode)

	// Concurrent: each Conn is driven from exactly one goroutine with a different overlay; the
	// stacks must never cross-talk.
	var wg sync.WaitGroup
	start := make(chan struct{})
	proceed := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		h, err := c1.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceDescribeExec})
		if err != nil {
			return
		}
		start <- struct{}{}
		<-proceed
		require.Equal(t, QueryExecModeDescribeExec, c1.EffectivePolicy().QueryExecMode)
		_ = h.Pop(context.Background())
	}()
	go func() {
		defer wg.Done()
		h, err := c2.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
		if err != nil {
			return
		}
		start <- struct{}{}
		<-proceed
		require.Equal(t, QueryExecModeSimpleProtocol, c2.EffectivePolicy().QueryExecMode)
		_ = h.Pop(context.Background())
	}()
	<-start
	<-start
	// while both frames are live, each Conn sees only its own top
	require.Equal(t, QueryExecModeDescribeExec, c1.EffectivePolicy().QueryExecMode)
	require.Equal(t, QueryExecModeSimpleProtocol, c2.EffectivePolicy().QueryExecMode)
	close(proceed)
	wg.Wait()

	require.NoError(t, h1.Pop(context.Background()))
	require.Equal(t, 0, c1.EffectivePolicy().Depth)
	require.Equal(t, 0, c2.EffectivePolicy().Depth)
}

// LIFO nesting: inner pop restores the outer frame; handles cannot pop out of order.
func TestPolicyStackNestedPushPop(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)

	outer, err := c.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
	require.NoError(t, err)
	inner, err := c.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceDescribeExec})
	require.NoError(t, err)

	require.Equal(t, QueryExecModeDescribeExec, c.EffectivePolicy().QueryExecMode)
	require.ErrorIs(t, outer.Pop(context.Background()), ErrPolicyFrameNotTop)
	require.NoError(t, inner.Pop(context.Background()))
	require.Equal(t, QueryExecModeSimpleProtocol, c.EffectivePolicy().QueryExecMode)
	require.NoError(t, outer.Pop(context.Background()))
	require.Equal(t, 0, c.EffectivePolicy().Depth)

	// second Pop of the same handle is a no-op
	require.NoError(t, inner.Pop(context.Background()))
}

// Late pushes must be rejected instead of silently governing an operation already underway.
func TestPolicyStackLatePushConflicts(t *testing.T) {
	t.Parallel()

	t.Run("reject_begin after begin", func(t *testing.T) {
		s := startMockScriptedServer(t)
		c := s.connect(t)
		ctx := context.Background()
		tx, err := c.Begin(ctx)
		require.NoError(t, err)
		_, err = c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitRejectBegin})
		require.ErrorIs(t, err, ErrPolicyConflict)
		require.NoError(t, tx.Rollback(ctx))
	})

	t.Run("forbid_cache after prepare", func(t *testing.T) {
		s := startMockScriptedServer(t)
		c := s.connect(t)
		ctx := context.Background()
		_, err := c.Prepare(ctx, "p", "select 42")
		require.NoError(t, err)
		_, err = c.PushPolicyFrame(ctx, PolicyFrame{StmtCacheGate: StmtCacheGateForbidCache})
		require.ErrorIs(t, err, ErrPolicyConflict)
	})

	t.Run("overlay while busy", func(t *testing.T) {
		s := startMockScriptedServer(t)
		c := s.connect(t)
		// An open pipeline holds the connection busy without sending anything.
		pipeline := c.PgConn().StartPipeline(context.Background())
		t.Cleanup(func() {
			_ = s.serverNc.Close()
		})
		_, err := c.PushPolicyFrame(context.Background(), PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
		require.ErrorIs(t, err, ErrPolicyConflict)
		_, err = c.PushPolicyFrame(context.Background(), PolicyFrame{BatchAdmit: BatchAdmitForcePipeline})
		require.ErrorIs(t, err, ErrPolicyConflict)
		_ = pipeline.Close()
	})
}

// Conn.Exec exit: force_simple goes out as a simple Query with interpolation, no Parse/Bind.
func TestPolicyConnExecForceSimple(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "cache_statement")
	ctx := context.Background()

	require.NoError(t, c.WithPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple}, func(ctx context.Context) error {
		_, err := c.Exec(ctx, "select $1::text", "hi")
		return err
	}))

	var sawSimple bool
	for _, q := range s.queries() {
		if strings.Contains(q, "'hi'") && strings.Contains(q, "select") {
			sawSimple = true
		}
	}
	require.True(t, sawSimple)
	require.NotContains(t, s.countTypes(), "Parse")
}

// Conn.Query exit: force_simple bypasses an explicitly prepared statement with the same name.
func TestPolicyConnQueryForceSimpleBypassesNamedStatement(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	_, err := c.Prepare(ctx, "mystmt", "select 42")
	require.NoError(t, err)

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
	require.NoError(t, err)
	rows, err := c.Query(ctx, "mystmt")
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, frame.Pop(ctx))

	require.Contains(t, s.queries(), "mystmt")
	require.NotContains(t, s.countTypes(), "Bind")
}

// force_cache_statement: same-shape SQL prepares once under a stable name then re-binds.
func TestPolicyConnExecForceCacheStatement(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	// Default exec mode would never prepare; the overlay forces the cache path.
	c := s.connectWithMode(t, "exec")
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceCacheStatement})
	require.NoError(t, err)

	_, err = c.Exec(ctx, "select $1::int", int32(1))
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select $1::int", int32(2))
	require.NoError(t, err)

	parses := 0
	for _, m := range s.recorded() {
		if m.typeName == "Parse" {
			parses++
			require.NotEmpty(t, m.name, "must use a named prepared statement")
			require.True(t, strings.HasPrefix(m.name, "stmtcache_"))
		}
	}
	require.Equal(t, 1, parses, "same-shape query must prepare exactly once")
	require.NoError(t, frame.Pop(ctx))
}

// force_describe_exec: every call does an unnamed Parse round plus execution round (two Syncs).
func TestPolicyConnExecForceDescribeExec(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "cache_statement")
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceDescribeExec})
	require.NoError(t, err)

	_, err = c.Exec(ctx, "select $1::int", int32(1))
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select $1::int", int32(2))
	require.NoError(t, err)

	counts := map[string]int{}
	var unnamedParses int
	for _, m := range s.recorded() {
		counts[m.typeName]++
		if m.typeName == "Parse" {
			require.Empty(t, m.name)
			unnamedParses++
		}
	}
	require.Equal(t, 2, unnamedParses, "describe-exec must describe on every execution")
	require.Equal(t, 4, counts["Sync"], "two round trips per call")
	require.NoError(t, frame.Pop(ctx))
}

// Tx exits read the same stack as Conn exits.
func TestPolicyTxQuerySemantics(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "cache_statement")
	ctx := context.Background()

	tx, err := c.Begin(ctx)
	require.NoError(t, err)

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple})
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "select $1::text", "x")
	require.NoError(t, err)
	rows, qErr := tx.Query(ctx, "select $1::text", "y")
	require.NoError(t, qErr)
	rows.Close()
	require.NoError(t, frame.Pop(ctx))

	require.NoError(t, tx.Rollback(ctx))

	require.Contains(t, strings.Join(s.queries(), "|"), "'x'")
	require.Contains(t, strings.Join(s.queries(), "|"), "'y'")
}

// tx_admit=reject_begin is enforced on Conn and nested Tx begins, testably and without wire traffic.
func TestPolicyRejectBegin(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitRejectBegin})
	require.NoError(t, err)

	_, err = c.Begin(ctx)
	require.ErrorIs(t, err, ErrPolicyBeginRejected)
	_, err = c.BeginTx(ctx, TxOptions{})
	require.ErrorIs(t, err, ErrPolicyBeginRejected)
	require.EqualValues(t, 2, frame.Counters().BeginsRejected)
	require.NoError(t, frame.Pop(ctx))

	// nested begin through a savepoint: the outer transaction is admitted by an outer pin frame,
	// so an inner reject frame governs the nested begin forward.
	pin, err := c.PushPolicyFrame(ctx, PolicyFrame{})
	require.NoError(t, err)
	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	nested, err := c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitRejectBegin})
	require.NoError(t, err)
	_, err = tx.Begin(ctx)
	require.ErrorIs(t, err, ErrPolicyBeginRejected)
	require.NoError(t, nested.Pop(ctx))
	require.NoError(t, pin.Pop(ctx))
	require.NoError(t, tx.Rollback(ctx))

	for _, q := range s.queries() {
		require.False(t, strings.HasPrefix(q, "savepoint"), "no savepoint may be sent when rejected")
	}
}

// force_rollback_on_exit observably rolls back frame-opened transactions at pop time.
func TestPolicyForceRollbackOnExit(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitForceRollbackOnExit})
	require.NoError(t, err)
	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	require.Equal(t, byte('T'), c.PgConn().TxStatus())
	require.NoError(t, frame.Pop(ctx))

	require.Equal(t, byte('I'), c.PgConn().TxStatus())
	require.ErrorIs(t, tx.Rollback(ctx), ErrTxClosed)
	require.EqualValues(t, 1, frame.Counters().RollbacksOnPop)

	var rollbackSeen bool
	for _, q := range s.queries() {
		if q == "rollback" {
			rollbackSeen = true
		}
	}
	require.True(t, rollbackSeen)

	// a committed transaction is not rolled back
	frame2, err := c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitForceRollbackOnExit})
	require.NoError(t, err)
	tx2, err := c.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx2.Commit(ctx))
	require.NoError(t, frame2.Pop(ctx))
	require.EqualValues(t, 0, frame2.Counters().RollbacksOnPop)
}

// batch_admit=reject_send fails without touching the queue or writing anything to the connection.
func TestPolicyRejectSendBatch(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	var b Batch
	b.Queue("select 1")
	b.Queue("select 2")

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{BatchAdmit: BatchAdmitRejectSend})
	require.NoError(t, err)
	br := c.SendBatch(ctx, &b)
	require.ErrorIs(t, br.Close(), ErrPolicyBatchRejected)
	require.Equal(t, 2, b.Len(), "batch queue must not be cleared")
	require.EqualValues(t, 1, frame.Counters().BatchesRejected)
	require.NoError(t, frame.Pop(ctx))

	// Tx.SendBatch reads the same axis.
	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	frame2, err := c.PushPolicyFrame(ctx, PolicyFrame{BatchAdmit: BatchAdmitRejectSend})
	require.NoError(t, err)
	br = tx.SendBatch(ctx, &b)
	require.ErrorIs(t, br.Close(), ErrPolicyBatchRejected)
	require.NoError(t, frame2.Pop(ctx))
	require.NoError(t, tx.Rollback(ctx))

	for _, m := range s.recorded() {
		if m.typeName == "Parse" || m.typeName == "Bind" {
			t.Fatalf("rejected batch must not write to the connection, got %s", m.typeName)
		}
	}
}

// batch_admit=force_pipeline forces the observable pipeline path even in exec default mode.
func TestPolicyForcePipelineBatch(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "exec")
	ctx := context.Background()

	var b Batch
	b.Queue("select $1::int", int32(1))
	b.Queue("select $1::int", int32(2))

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{BatchAdmit: BatchAdmitForcePipeline})
	require.NoError(t, err)
	br := c.SendBatch(ctx, &b)
	_, err = br.Exec()
	require.NoError(t, err)
	_, err = br.Exec()
	require.NoError(t, err)
	require.NoError(t, br.Close())
	require.NoError(t, frame.Pop(ctx))

	counts := s.countTypes()
	require.GreaterOrEqual(t, counts["Parse"], 2, "pipeline describes each query")
	require.GreaterOrEqual(t, counts["Sync"], 2)
	require.Equal(t, 0, counts["Query"], "must not use the simple-protocol batch send")
	require.EqualValues(t, 1, frame.Counters().PipelineBatches)
}

// forbid_cache: cache modes downgrade to unnamed describes, nothing is cached or left prepared.
func TestPolicyForbidCache(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "cache_statement")
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{StmtCacheGate: StmtCacheGateForbidCache})
	require.NoError(t, err)
	eff := c.EffectivePolicy()
	require.False(t, eff.StatementCacheEnabled)
	require.False(t, eff.DescriptionCacheEnabled)
	require.Equal(t, QueryExecModeDescribeExec, eff.QueryExecMode)

	for range 2 {
		_, err := c.Exec(ctx, "select $1::int", int32(1))
		require.NoError(t, err)
	}
	require.NoError(t, frame.Pop(ctx))

	require.Nil(t, c.statementCache.Get("select $1::int"))
	require.Equal(t, 0, c.statementCache.Len())

	var namedParse bool
	for _, m := range s.recorded() {
		if m.typeName == "Parse" && m.name != "" {
			namedParse = true
		}
	}
	require.False(t, namedParse, "no named prepares under forbid_cache")

	// a statement explicitly prepared in the frame must be deallocated at pop
	frame2, err := c.PushPolicyFrame(ctx, PolicyFrame{StmtCacheGate: StmtCacheGateForbidCache})
	require.NoError(t, err)
	_, err = c.Prepare(ctx, "ephemeral", "select 7")
	require.NoError(t, err)
	_, ok := c.preparedStatements["ephemeral"]
	require.True(t, ok)
	require.NoError(t, frame2.Pop(ctx))
	_, ok = c.preparedStatements["ephemeral"]
	require.False(t, ok, "later frames must not reuse frame-created prepared statements")
}

// force_deallocate_on_pop cleans up frame-created named prepares (explicit and auto) observably.
func TestPolicyForceDeallocateOnPop(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "cache_statement")
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{StmtCacheGate: StmtCacheGateForceDeallocateOnPop})
	require.NoError(t, err)
	_, err = c.Prepare(ctx, "explicit_name", "select 5")
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select $1::int", int32(9)) // creates stmtcache_* auto prepare
	require.NoError(t, err)
	require.NoError(t, frame.Pop(ctx))

	require.Equal(t, 0, c.statementCache.Len())
	_, ok := c.preparedStatements["explicit_name"]
	require.False(t, ok)
	require.EqualValues(t, 2, frame.Counters().DeallocationsOnPop)

	closed := s.closeNames()
	require.Contains(t, closed, "explicit_name")
	var foundAuto bool
	for _, name := range closed {
		if strings.HasPrefix(name, "stmtcache_") {
			foundAuto = true
		}
	}
	require.True(t, foundAuto)
}

// CopyFrom reads the frame; in a reject_begin frame no transaction is opened on the wire.
func TestPolicyCopyFromNoHiddenTransaction(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{TxAdmit: TxAdmitRejectBegin})
	require.NoError(t, err)

	n, err := c.CopyFrom(ctx, Identifier{"t"}, []string{"a"}, CopyFromRows([][]any{{int32(1)}}))
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.Equal(t, byte('I'), c.PgConn().TxStatus())
	require.EqualValues(t, 1, frame.Counters().CopyFrom)
	require.EqualValues(t, 0, frame.Counters().BeginsRejected)
	require.NoError(t, frame.Pop(ctx))

	for _, q := range s.queries() {
		require.False(t, strings.HasPrefix(strings.ToLower(q), "begin"))
		require.True(t, strings.HasPrefix(strings.ToLower(q), "copy "))
	}
}

// WithPolicyFrame pops on both error and panic paths.
func TestWithPolicyFrameAlwaysPops(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	boom := errors.New("boom")
	err := c.WithPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple}, func(ctx context.Context) error {
		return boom
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 0, c.EffectivePolicy().Depth)

	func() {
		defer func() {
			require.Equal(t, "panicked", recover())
			require.Equal(t, 0, c.EffectivePolicy().Depth)
		}()
		_ = c.WithPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple}, func(ctx context.Context) error {
			panic("panicked")
		})
	}()
}

// The effective view merges transaction and batch state.
func TestEffectivePolicyTxAndBatchView(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	require.True(t, c.EffectivePolicy().InTx)

	var b Batch
	b.Queue("select 1")
	br := tx.SendBatch(ctx, &b)
	require.True(t, c.EffectivePolicy().InBatch)
	require.NoError(t, br.Close())
	require.False(t, c.EffectivePolicy().InBatch)

	require.NoError(t, tx.Rollback(ctx))
	require.False(t, c.EffectivePolicy().InTx)
}

// With no frame ever pushed, wire behavior is unchanged: default cache_statement prepares once.
func TestPolicyBaselineWireUnchanged(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	_, err := c.Exec(ctx, "select $1::int", int32(1))
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select $1::int", int32(2))
	require.NoError(t, err)

	var parses int
	for _, m := range s.recorded() {
		if m.typeName == "Parse" {
			parses++
		}
	}
	require.Equal(t, 1, parses)
}

// force_cache_statement stays observable for same-shape queries even with no arguments, where
// baseline code would otherwise coerce to simple protocol.
func TestPolicyForceCacheStatementNoArgs(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceCacheStatement})
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select 42")
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select 42")
	require.NoError(t, err)
	require.NoError(t, frame.Pop(ctx))

	var parses int
	var sawQuery bool
	for _, m := range s.recorded() {
		switch m.typeName {
		case "Parse":
			parses++
			require.True(t, strings.HasPrefix(m.name, "stmtcache_"))
		case "Query":
			sawQuery = true
		}
	}
	require.Equal(t, 1, parses)
	require.False(t, sawQuery, "must not fall back to simple protocol")
}

// force_pipeline is observable even with simple_protocol default mode.
func TestPolicyForcePipelineUnderSimpleDefault(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "simple_protocol")
	ctx := context.Background()

	var b Batch
	b.Queue("select $1::int", int32(1))

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{BatchAdmit: BatchAdmitForcePipeline})
	require.NoError(t, err)
	br := c.SendBatch(ctx, &b)
	_, err = br.Exec()
	require.NoError(t, err)
	require.NoError(t, br.Close())
	require.NoError(t, frame.Pop(ctx))

	counts := s.countTypes()
	require.NotZero(t, counts["Parse"])
	require.Equal(t, 0, counts["Query"], "simple-protocol concatenation must not be used")
	require.NotZero(t, counts["Sync"])
	require.EqualValues(t, 1, frame.Counters().PipelineBatches)
}

// forbid_cache downgrades cache_statement batches to the unnamed pipeline path and leaves
// nothing cached behind.
func TestPolicyForbidCacheBatch(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connect(t)
	ctx := context.Background()

	frame, err := c.PushPolicyFrame(ctx, PolicyFrame{StmtCacheGate: StmtCacheGateForbidCache})
	require.NoError(t, err)

	var b Batch
	b.Queue("select $1::int", int32(1))
	b.Queue("select $1::int", int32(2))
	br := c.SendBatch(ctx, &b)
	require.NoError(t, br.Close())
	require.NoError(t, frame.Pop(ctx))

	require.Equal(t, 0, c.statementCache.Len())
	for _, m := range s.recorded() {
		if m.typeName == "Parse" {
			require.Empty(t, m.name, "batch prepares must stay unnamed under forbid_cache")
		}
	}
}

// A per-call QueryExecMode cannot override a forced frame overlay.
func TestPolicyOverlayBeatsPerCallMode(t *testing.T) {
	t.Parallel()
	s := startMockScriptedServer(t)
	c := s.connectWithMode(t, "exec")
	ctx := context.Background()

	require.NoError(t, c.WithPolicyFrame(ctx, PolicyFrame{QueryExecOverlay: QueryExecOverlayForceSimple}, func(ctx context.Context) error {
		_, err := c.Exec(ctx, "select $1::text", QueryExecModeCacheStatement, "z")
		return err
	}))
	require.Contains(t, strings.Join(s.queries(), "|"), "'z'")
	require.NotContains(t, s.countTypes(), "Parse")
}
