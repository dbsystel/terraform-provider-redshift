package redshift

import (
	"database/sql"
	"fmt"
	"sync"
)

var (
	dbRegistryLock sync.Mutex
	dbRegistry     = make(map[string]*DBConnection, 1)
	dbConnectLocks = make(map[string]*sync.Mutex, 1)
)

// dsnLock returns the per-DSN mutex that serializes connection
// establishment for a single DSN, without blocking other DSNs.
func dsnLock(dsn string) *sync.Mutex {
	dbRegistryLock.Lock()
	defer dbRegistryLock.Unlock()
	l, ok := dbConnectLocks[dsn]
	if !ok {
		l = &sync.Mutex{}
		dbConnectLocks[dsn] = l
	}
	return l
}

type Config struct {
	DriverName string
	ConnStr    string
	Database   string
	MaxConns   int

	usernameRetrievalMutex *sync.Mutex
	retrievedUsername      string
}

func NewConfig(driverName, connStr, database string, maxConns int) *Config {
	return &Config{
		DriverName: driverName,
		ConnStr:    connStr,
		Database:   database,
		MaxConns:   maxConns,

		usernameRetrievalMutex: &sync.Mutex{},
	}
}

// Client struct holding connection string
type Client struct {
	config Config

	db *sql.DB
}

type DBConnection struct {
	*sql.DB

	client *Client
}

// NewClient returns client config for the specified database.
func (c *Config) NewClient() *Client {
	return &Client{
		config: *c,
	}
}

func (c *Config) GetUsername(db *DBConnection) (string, error) {
	if c.retrievedUsername != "" {
		return c.retrievedUsername, nil
	}
	c.usernameRetrievalMutex.Lock()
	defer c.usernameRetrievalMutex.Unlock()
	if c.retrievedUsername != "" {
		return c.retrievedUsername, nil
	}
	row := db.QueryRow("SELECT current_user;")
	if row.Err() != nil {
		return "", fmt.Errorf("error retrieving current user: %w", row.Err())
	}
	var username string
	if err := row.Scan(&username); err != nil {
		return "", fmt.Errorf("error scanning current user: %w", err)
	}
	c.retrievedUsername = username
	return c.retrievedUsername, nil
}

// Connect returns a copy to an sql.Open()'ed database connection wrapped in a DBConnection struct.
// Callers must return their database resources. Use of QueryRow() or Exec() is encouraged.
// Query() must have their rows.Close()'ed.
func (c *Client) Connect() (*DBConnection, error) {
	dsn := c.config.ConnStr
	driverName := c.config.DriverName

	// A cached connection is trusted without a liveness probe: database/sql
	// itself redials a broken pooled connection on next use, so a dead DSN
	// surfaces as a normal query error to the caller instead of costing an
	// extra timeout here.
	checkExisting := func() (*DBConnection, bool) {
		dbRegistryLock.Lock()
		defer dbRegistryLock.Unlock()
		conn, found := dbRegistry[dsn]
		return conn, found
	}

	conn, ok := checkExisting()
	if ok {
		return conn, nil
	}

	// Serialize connection establishment per-DSN only, so concurrent
	// Connect() calls to different DSNs never block each other.
	l := dsnLock(dsn)
	l.Lock()
	defer l.Unlock()

	// Re-check: another goroutine may have already (re)connected this DSN
	// while we were waiting on the per-DSN lock.
	conn, ok = checkExisting()
	if ok {
		return conn, nil
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("error creating Redshift driver instance (driver: %q): %w", driverName, err)
	}

	// We don't want to retain connection
	// So when we connect on a specific database which might be managed by terraform,
	// we don't keep opened connection in case of the db has to be dropped in the plan.
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(c.config.MaxConns)

	conn = &DBConnection{
		db,
		c,
	}

	_, err = c.config.GetUsername(conn)
	if err != nil {
		return nil, fmt.Errorf("error retrieving username from Redshift database (driver: %q): %w", driverName, err)
	}

	dbRegistryLock.Lock()
	dbRegistry[dsn] = conn
	dbRegistryLock.Unlock()

	return conn, nil
}

func (c *Client) Close() {
	if c.db != nil {
		c.db.Close()
	}
}
