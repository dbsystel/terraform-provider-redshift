package redshift

import (
	"database/sql"
	"fmt"
	"sync"
)

var (
	dbRegistryLock sync.Mutex
	dbRegistry     = make(map[string]*DBConnection, 1)
)

type Config struct {
	DriverName string
	ConnStr    string
	Database   string
	MaxConns   int

	serverlessCheckMutex *sync.Mutex
	isServerless         bool
	checkedForServerless bool

	usernameRetrievalMutex *sync.Mutex
	retrievedUsername      string
}

func NewConfig(driverName, connStr, database string, maxConns int) *Config {
	return &Config{
		DriverName: driverName,
		ConnStr:    connStr,
		Database:   database,
		MaxConns:   maxConns,

		serverlessCheckMutex:   &sync.Mutex{},
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

func (c *Config) IsServerless(db *DBConnection) (bool, error) {
	if c.serverlessCheckMutex == nil {
		c.serverlessCheckMutex = &sync.Mutex{}
	}
	c.serverlessCheckMutex.Lock()
	defer c.serverlessCheckMutex.Unlock()
	if c.checkedForServerless {
		return c.isServerless, nil
	}

	c.checkedForServerless = true

	rows, err := db.Query("SELECT 1 FROM SYS_SERVERLESS_USAGE")
	// No error means we have accessed the view and are running Redshift Serverless
	if err == nil {
		defer rows.Close()
		c.isServerless = true
		return true, nil
	}

	// Insuficcient privileges means we do not have access to this view ergo we run on Redshift classic
	if isPqErrorWithCode(err, pgErrorCodeInsufficientPrivileges) {
		c.isServerless = false
		return false, nil
	}

	return false, err
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
	dbRegistryLock.Lock()
	defer dbRegistryLock.Unlock()

	dsn := c.config.ConnStr
	driverName := c.config.DriverName
	conn, found := dbRegistry[dsn]

	if !found || conn.Ping() != nil {
		db, err := sql.Open(driverName, dsn)
		if err != nil {
			return nil, fmt.Errorf("error creating Redshift driver instance (driver: %q, dsn: %q): %w", driverName, dsn, err)
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
			return nil, fmt.Errorf("error retrieving username from Redshift database (driver: %q, dsn: %q): %w", driverName, dsn, err)
		}

		dbRegistry[dsn] = conn
	}

	return conn, nil
}

func (c *Client) Close() {
	if c.db != nil {
		c.db.Close()
	}
}
