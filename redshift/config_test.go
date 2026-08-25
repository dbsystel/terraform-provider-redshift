package redshift

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn simulates a live Redshift connection: it answers
// "SELECT current_user;" (the query GetUsername runs) and nothing else.
// It counts current_user queries on its parent driver, since that query
// runs exactly once per genuine (non-deduped) connection establishment —
// unlike raw driver.Open calls, which also fire on every Ping() re-dial
// forced by MaxIdleConns(0) (a separate, pre-existing behavior, untouched
// here).
type fakeConn struct{ drv *fakeDriver }

func (fakeConn) Prepare(query string) (driver.Stmt, error) { return nil, fmt.Errorf("not implemented") }
func (fakeConn) Close() error                              { return nil }
func (fakeConn) Begin() (driver.Tx, error)                 { return nil, fmt.Errorf("not implemented") }

func (c fakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	atomic.AddInt32(&c.drv.queryCount, 1)
	return &fakeRows{}, nil
}

type fakeRows struct{ done bool }

func (r *fakeRows) Columns() []string { return []string{"current_user"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = "testuser"
	return nil
}

// fakeDriver simulates connection latency so concurrent Connect() calls can
// be proven to run in parallel (different DSNs) or de-duplicate (same DSN).
type fakeDriver struct {
	openCount  int32
	queryCount int32
	delay      time.Duration
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	atomic.AddInt32(&d.openCount, 1)
	time.Sleep(d.delay)
	return fakeConn{drv: d}, nil
}

func newTestClient(t *testing.T, driverName, dsn string, drv driver.Driver) *Client {
	t.Helper()
	sql.Register(driverName, drv)
	cfg := NewConfig(driverName, dsn, "testdb", 1)
	return cfg.NewClient()
}

func TestConnect_DifferentDSNsRunInParallel(t *testing.T) {
	delay := 150 * time.Millisecond
	drv1 := &fakeDriver{delay: delay}
	drv2 := &fakeDriver{delay: delay}
	c1 := newTestClient(t, "fakeredshift_parallel1", "dsn-parallel-1", drv1)
	c2 := newTestClient(t, "fakeredshift_parallel2", "dsn-parallel-2", drv2)

	var wg sync.WaitGroup
	wg.Add(2)
	start := time.Now()
	for _, c := range []*Client{c1, c2} {
		c := c
		go func() {
			defer wg.Done()
			if _, err := c.Connect(); err != nil {
				t.Errorf("Connect() error: %v", err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if elapsed >= 2*delay {
		t.Fatalf("Connect() to different DSNs serialized: took %v, want < %v", elapsed, 2*delay)
	}
}

func TestConnect_SameDSNDeduplicates(t *testing.T) {
	drv := &fakeDriver{delay: 100 * time.Millisecond}
	c := newTestClient(t, "fakeredshift_dedupe", "dsn-dedupe", drv)

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	conns := make([]*DBConnection, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			conn, err := c.Connect()
			if err != nil {
				t.Errorf("Connect() error: %v", err)
				return
			}
			conns[i] = conn
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&drv.queryCount); got != 1 {
		t.Fatalf("GetUsername query ran %d times, want 1 (concurrent Connect() calls for the same DSN should de-dupe to one connection establishment)", got)
	}
	for i, conn := range conns {
		if conn != conns[0] {
			t.Fatalf("goroutine %d got a different *DBConnection than goroutine 0", i)
		}
	}
}
