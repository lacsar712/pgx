package pgxpool_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// msgSnap is a value snapshot because pgproto3 reuses decoded message structs.
type msgSnap struct {
	typ   string
	query string
}

// runMockBackend answers the minimum extended/simple protocol traffic needed to observe which
// path the client took.
func runMockBackend(t *testing.T, nc net.Conn, mu *sync.Mutex, msgs *[]msgSnap) {
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	backend := pgproto3.NewBackend(nc, nc)

	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: []byte{0, 0, 0, 0}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}

	record := func(m pgproto3.FrontendMessage) {
		s := msgSnap{typ: "other"}
		switch mm := m.(type) {
		case *pgproto3.Query:
			s.typ, s.query = "Query", mm.String
		case *pgproto3.Parse:
			s.typ = "Parse"
		case *pgproto3.Bind:
			s.typ = "Bind"
		case *pgproto3.Sync:
			s.typ = "Sync"
		case *pgproto3.Terminate:
			s.typ = "Terminate"
		}
		mu.Lock()
		*msgs = append(*msgs, s)
		mu.Unlock()
	}

	txStatus := byte('I')
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}
		record(msg)
		switch msg.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Parse:
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			backend.Send(&pgproto3.ParameterDescription{})
			backend.Send(&pgproto3.NoData{})
		case *pgproto3.Bind:
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		case *pgproto3.Close:
			backend.Send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
		case *pgproto3.Query:
			q := msg.(*pgproto3.Query).String
			switch {
			case strings.HasPrefix(q, "begin"):
				txStatus = 'T'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case strings.HasPrefix(q, "rollback"):
				txStatus = 'I'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
			default:
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
		}
		if err := backend.Flush(); err != nil {
			return
		}
	}
}

func newPolicyTestPool(t *testing.T) (*pgxpool.Pool, *sync.Mutex, *[]msgSnap) {
	t.Helper()
	client, server := net.Pipe()
	var mu sync.Mutex
	msgs := make([]msgSnap, 0)
	go runMockBackend(t, server, &mu, &msgs)

	config, err := pgxpool.ParseConfig("host=127.0.0.1 sslmode=disable default_query_exec_mode=cache_statement")
	require.NoError(t, err)
	config.MaxConns = 1
	config.HealthCheckPeriod = time.Hour
	config.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		_ = client.Close()
	})
	return pool, &mu, &msgs
}

// A frame pushed on a Conn checked out of the pool governs that Conn's query and begin exits.
func TestPoolAcquiredConnObeysPolicyStack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, mu, msgs := newPolicyTestPool(t)

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	// force_simple on the acquired physical Conn
	frame, err := c.Conn().PushPolicyFrame(ctx, pgx.PolicyFrame{QueryExecOverlay: pgx.QueryExecOverlayForceSimple})
	require.NoError(t, err)
	_, err = c.Exec(ctx, "select $1::text", "z")
	require.NoError(t, err)

	// reject_begin on the same acquired Conn
	reject, err := c.Conn().PushPolicyFrame(ctx, pgx.PolicyFrame{TxAdmit: pgx.TxAdmitRejectBegin})
	require.NoError(t, err)
	_, err = c.Begin(ctx)
	require.ErrorIs(t, err, pgx.ErrPolicyBeginRejected)
	require.NoError(t, reject.Pop(ctx))

	require.NoError(t, frame.Pop(ctx))
	c.Release()

	mu.Lock()
	got := append([]msgSnap(nil), (*msgs)...)
	mu.Unlock()

	var sawSimple bool
	for _, m := range got {
		if m.typ == "Query" && strings.Contains(m.query, "'z'") {
			sawSimple = true
		}
		require.NotEqual(t, "Parse", m.typ, "force_simple through the pool must not prepare")
	}
	require.True(t, sawSimple, "pool-acquired Conn must read its own policy stack")
}
