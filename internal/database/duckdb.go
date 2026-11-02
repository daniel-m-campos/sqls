package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/daniel-m-campos/sqls/dialect"
)

func init() {
	RegisterOpen(dialect.DatabaseDriverDuckDB, duckdbOpen)
	RegisterFactory(dialect.DatabaseDriverDuckDB, NewDuckDBRepository)
}

// DuckDB holds an exclusive lock on its database file for as long as the file is
// open, and a read-only open is refused while any other process writes to it.
// A connection held for the whole editing session, as with the other drivers,
// would therefore block the user's own notebooks and CLI sessions, and be blocked
// by them. duckdbConnector opens the file for each database/sql connection and
// closes it with that connection, and duckdbOpen tells database/sql to keep no
// idle connections, so the lock exists only while a query runs.
type duckdbConnector struct {
	dsn string
}

// duckdbConn pairs a connection with the connector (the open database file)
// it came from, so closing the connection also releases the file.
type duckdbConn struct {
	driver.Conn
	connector *duckdb.Connector
}

func (c *duckdbConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connector, err := duckdb.NewConnector(c.dsn, nil)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(ctx)
	if err != nil {
		_ = connector.Close()
		return nil, err
	}
	return &duckdbConn{Conn: conn, connector: connector}, nil
}

func (c *duckdbConnector) Driver() driver.Driver {
	return duckdb.Driver{}
}

func (c *duckdbConn) Close() error {
	err := c.Conn.Close()
	if cerr := c.connector.Close(); err == nil {
		err = cerr
	}
	return err
}

// The optional driver interfaces are forwarded explicitly: embedding driver.Conn
// only promotes Prepare, Close and Begin, and without these database/sql would
// fall back to preparing every statement. driver.ErrSkip tells database/sql to
// use that fallback when the wrapped connection lacks the method.

func (c *duckdbConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *duckdbConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *duckdbConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, opts)
	}
	return c.Conn.Begin() //nolint:staticcheck // database/sql's own fallback
}

// genDuckDBDsn builds the duckdb-go DSN: the file path with the connection
// params as a query string. The file is opened read-only unless the config sets
// params.access_mode, so that browsing the schema never blocks a writer.
func genDuckDBDsn(cfg *DBConfig) string {
	params := url.Values{}
	for k, v := range cfg.Params {
		params.Set(k, v)
	}
	if params.Get("access_mode") == "" {
		params.Set("access_mode", "read_only")
	}
	return cfg.DataSourceName + "?" + params.Encode()
}

func duckdbOpen(cfg *DBConfig) (*DBConnection, error) {
	dsn := genDuckDBDsn(cfg)
	var conn *sql.DB
	if isDuckDBInMemory(cfg.DataSourceName) {
		// An in-memory database lives in its connector: keep one open, or every
		// query would see an empty database.
		connector, err := duckdb.NewConnector(dsn, nil)
		if err != nil {
			return nil, err
		}
		conn = sql.OpenDB(connector)
	} else {
		conn = sql.OpenDB(&duckdbConnector{dsn: dsn})
		// No idle connections: the file is closed (and its lock released) as soon
		// as each query finishes. One open connection at a time: a second
		// connection would be a second open of the same file by this process.
		conn.SetMaxIdleConns(0)
		conn.SetMaxOpenConns(1)
	}
	// Open the file once now so a missing or locked file is reported when the
	// connection is set up rather than at the first completion request.
	if err := conn.PingContext(context.Background()); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &DBConnection{Conn: conn}, nil
}

func isDuckDBInMemory(dataSourceName string) bool {
	return dataSourceName == "" || strings.HasPrefix(dataSourceName, ":memory:")
}

type DuckDBRepository struct {
	Conn *sql.DB
}

func NewDuckDBRepository(conn *sql.DB) DBRepository {
	return &DuckDBRepository{Conn: conn}
}

func (db *DuckDBRepository) Driver() dialect.DatabaseDriver {
	return dialect.DatabaseDriverDuckDB
}

func (db *DuckDBRepository) CurrentDatabase(ctx context.Context) (string, error) {
	row := db.Conn.QueryRowContext(ctx, "SELECT current_database()")
	var database string
	if err := row.Scan(&database); err != nil {
		return "", err
	}
	return database, nil
}

func (db *DuckDBRepository) Databases(ctx context.Context) ([]string, error) {
	return db.queryStrings(ctx, `
	SELECT database_name
	FROM duckdb_databases()
	WHERE NOT internal
	ORDER BY database_name
	`)
}

func (db *DuckDBRepository) CurrentSchema(ctx context.Context) (string, error) {
	row := db.Conn.QueryRowContext(ctx, "SELECT current_schema()")
	var schema string
	if err := row.Scan(&schema); err != nil {
		return "", err
	}
	return schema, nil
}

func (db *DuckDBRepository) Schemas(ctx context.Context) ([]string, error) {
	return db.queryStrings(ctx, `
	SELECT schema_name
	FROM information_schema.schemata
	WHERE catalog_name = current_database()
	  AND schema_name NOT IN ('information_schema', 'pg_catalog')
	ORDER BY schema_name
	`)
}

func (db *DuckDBRepository) SchemaTables(ctx context.Context) (map[string][]string, error) {
	// Views are included: they complete like tables.
	rows, err := db.Conn.QueryContext(ctx, `
	SELECT table_schema, table_name
	FROM information_schema.tables
	WHERE table_catalog = current_database()
	ORDER BY table_schema, table_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := map[string][]string{}
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		tables[schema] = append(tables[schema], table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (db *DuckDBRepository) DescribeDatabaseTable(ctx context.Context) ([]*ColumnDesc, error) {
	return db.describeColumns(ctx, "")
}

func (db *DuckDBRepository) DescribeDatabaseTableBySchema(ctx context.Context, schemaName string) ([]*ColumnDesc, error) {
	return db.describeColumns(ctx, schemaName)
}

// describeColumns lists every column of the current database, or of one schema
// when schemaName is set. Key is "YES" for primary key columns, as in sqlite3.
func (db *DuckDBRepository) describeColumns(ctx context.Context, schemaName string) ([]*ColumnDesc, error) {
	rows, err := db.Conn.QueryContext(ctx, `
	SELECT c.table_schema,
	       c.table_name,
	       c.column_name,
	       c.data_type,
	       c.is_nullable,
	       CASE WHEN k.column_name IS NULL THEN 'NO' ELSE 'YES' END,
	       c.column_default
	FROM information_schema.columns c
	LEFT JOIN (
	    SELECT schema_name, table_name, unnest(constraint_column_names) AS column_name
	    FROM duckdb_constraints()
	    WHERE constraint_type = 'PRIMARY KEY' AND database_name = current_database()
	) k ON k.schema_name = c.table_schema
	   AND k.table_name = c.table_name
	   AND k.column_name = c.column_name
	WHERE c.table_catalog = current_database()
	  AND ($1 = '' OR c.table_schema = $1)
	ORDER BY c.table_schema, c.table_name, c.ordinal_position
	`, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := []*ColumnDesc{}
	for rows.Next() {
		var column ColumnDesc
		err := rows.Scan(
			&column.Schema,
			&column.Table,
			&column.Name,
			&column.Type,
			&column.Null,
			&column.Key,
			&column.Default,
		)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (db *DuckDBRepository) DescribeForeignKeysBySchema(ctx context.Context, schemaName string) ([]*ForeignKey, error) {
	// unnest over the two list columns is positional, so a composite key yields
	// one row per column pair, which is what parseForeignKeys expects.
	rows, err := db.Conn.QueryContext(ctx, `
	SELECT table_name || '_' || constraint_index,
	       table_name,
	       unnest(constraint_column_names),
	       referenced_table,
	       unnest(referenced_column_names)
	FROM duckdb_constraints()
	WHERE constraint_type = 'FOREIGN KEY'
	  AND database_name = current_database()
	  AND schema_name = $1
	ORDER BY 1
	`, schemaName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return parseForeignKeys(rows, schemaName)
}

func (db *DuckDBRepository) Exec(ctx context.Context, query string) (sql.Result, error) {
	return db.Conn.ExecContext(ctx, query)
}

func (db *DuckDBRepository) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return db.Conn.QueryContext(ctx, query)
}

func (db *DuckDBRepository) queryStrings(ctx context.Context, query string) ([]string, error) {
	rows, err := db.Conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}
