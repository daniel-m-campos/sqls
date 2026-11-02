package database

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// newDuckDBTestFile creates a database file with two schemas, a view, primary
// keys and single and composite foreign keys, and returns its path.
func newDuckDBTestFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.duckdb")
	conn, err := duckdbOpen(&DBConfig{
		DataSourceName: path,
		Params:         map[string]string{"access_mode": "read_write"},
	})
	if err != nil {
		t.Fatalf("open read_write: %v", err)
	}
	defer conn.Close()
	statements := []string{
		`CREATE TABLE authors (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, country VARCHAR DEFAULT 'unknown')`,
		`CREATE TABLE books (
			id INTEGER PRIMARY KEY,
			author_id INTEGER REFERENCES authors(id),
			title VARCHAR,
			a INTEGER, b INTEGER, UNIQUE (a, b))`,
		`CREATE TABLE editions (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES books(a, b))`,
		`CREATE VIEW recent_books AS SELECT id, title FROM books`,
		`CREATE SCHEMA archive`,
		`CREATE TABLE archive.books (id INTEGER, title VARCHAR)`,
		`INSERT INTO authors VALUES (1, 'Ann', 'NZ'), (2, 'Bob', DEFAULT)`,
	}
	for _, statement := range statements {
		if _, err := conn.Conn.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return path
}

func newDuckDBTestRepository(t *testing.T) (DBRepository, *DBConnection) {
	t.Helper()
	conn, err := duckdbOpen(&DBConfig{DataSourceName: newDuckDBTestFile(t)})
	if err != nil {
		t.Fatalf("open read_only: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return NewDuckDBRepository(conn.Conn), conn
}

func TestDuckDBGenDsn(t *testing.T) {
	tests := []struct {
		name string
		cfg  *DBConfig
		want string
	}{
		{
			name: "read-only by default",
			cfg:  &DBConfig{DataSourceName: "/data/a.duckdb"},
			want: "/data/a.duckdb?access_mode=read_only",
		},
		{
			name: "params override access mode",
			cfg:  &DBConfig{DataSourceName: "/data/a.duckdb", Params: map[string]string{"access_mode": "read_write", "threads": "2"}},
			want: "/data/a.duckdb?access_mode=read_write&threads=2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genDuckDBDsn(tt.cfg); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDuckDBOpenMissingFile(t *testing.T) {
	_, err := duckdbOpen(&DBConfig{DataSourceName: filepath.Join(t.TempDir(), "missing.duckdb")})
	if err == nil {
		t.Fatal("expected an error for a missing file opened read-only")
	}
}

func TestDuckDBRepository(t *testing.T) {
	ctx := context.Background()
	repo, _ := newDuckDBTestRepository(t)

	database, err := repo.CurrentDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if database != "test" {
		t.Errorf("CurrentDatabase = %q, want %q", database, "test")
	}
	schema, err := repo.CurrentSchema(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if schema != "main" {
		t.Errorf("CurrentSchema = %q, want %q", schema, "main")
	}

	schemas, err := repo.Schemas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"archive", "main"}, schemas); diff != "" {
		t.Errorf("Schemas mismatch (-want +got):\n%s", diff)
	}

	tables, err := repo.SchemaTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantTables := map[string][]string{
		"archive": {"books"},
		"main":    {"authors", "books", "editions", "recent_books"},
	}
	if diff := cmp.Diff(wantTables, tables); diff != "" {
		t.Errorf("SchemaTables mismatch (-want +got):\n%s", diff)
	}

	columns, err := repo.DescribeDatabaseTableBySchema(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*ColumnDesc{}
	for _, column := range columns {
		byName[column.Table+"."+column.Name] = column
	}
	wantColumns := map[string]ColumnDesc{
		"authors.id":      {ColumnBase: ColumnBase{Schema: "main", Table: "authors", Name: "id"}, Type: "INTEGER", Null: "NO", Key: "YES"},
		"authors.name":    {ColumnBase: ColumnBase{Schema: "main", Table: "authors", Name: "name"}, Type: "VARCHAR", Null: "NO", Key: "NO"},
		"authors.country": {ColumnBase: ColumnBase{Schema: "main", Table: "authors", Name: "country"}, Type: "VARCHAR", Null: "YES", Key: "NO", Default: sql.NullString{String: "'unknown'", Valid: true}},
		"books.author_id": {ColumnBase: ColumnBase{Schema: "main", Table: "books", Name: "author_id"}, Type: "INTEGER", Null: "YES", Key: "NO"},
		"recent_books.id": {ColumnBase: ColumnBase{Schema: "main", Table: "recent_books", Name: "id"}, Type: "INTEGER", Null: "YES", Key: "NO"},
	}
	for name, want := range wantColumns {
		got, ok := byName[name]
		if !ok {
			t.Errorf("column %s missing", name)
			continue
		}
		if diff := cmp.Diff(want, *got); diff != "" {
			t.Errorf("column %s mismatch (-want +got):\n%s", name, diff)
		}
	}
	// authors 3 + books 5 + editions 3 + recent_books 2 columns in main.
	if _, ok := byName["books.id"]; !ok || len(columns) != 13 {
		t.Errorf("DescribeDatabaseTableBySchema(main) returned %d columns, want 13", len(columns))
	}
	all, err := repo.DescribeDatabaseTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// main's 13 plus archive.books' 2.
	if len(all) != 15 {
		t.Errorf("DescribeDatabaseTable returned %d columns, want 15", len(all))
	}

	fks, err := repo.DescribeForeignKeysBySchema(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	column := func(table, name string) *ColumnBase { return &ColumnBase{Schema: "main", Table: table, Name: name} }
	wantFks := []*ForeignKey{
		{{column("books", "author_id"), column("authors", "id")}},
		{{column("editions", "a"), column("books", "a")}, {column("editions", "b"), column("books", "b")}},
	}
	if diff := cmp.Diff(wantFks, fks); diff != "" {
		t.Errorf("DescribeForeignKeysBySchema mismatch (-want +got):\n%s", diff)
	}

	rows, err := repo.Query(ctx, "SELECT name FROM authors ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	names, err := ScanRows(rows, 1)
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([][]string{{"Ann"}, {"Bob"}}, names); diff != "" {
		t.Errorf("Query mismatch (-want +got):\n%s", diff)
	}

	if _, err := repo.Exec(ctx, "INSERT INTO authors VALUES (3, 'Cy', 'AU')"); err == nil {
		t.Error("Exec on a read-only connection should fail")
	}
}

// TestDuckDBLockReleased checks that the file is not held between queries:
// after each repository call a separate process can open it for writing.
// DuckDB's file lock is per process, so a second open inside this process would
// prove nothing; the helper is this test binary re-run with a flag.
func TestDuckDBLockReleased(t *testing.T) {
	if os.Getenv("SQLS_DUCKDB_HELPER") != "" {
		return
	}
	ctx := context.Background()
	repo, _ := newDuckDBTestRepository(t)
	path := duckdbTestPath(t, repo)

	calls := map[string]func() error{
		"Schemas":            func() error { _, err := repo.Schemas(ctx); return err },
		"SchemaTables":       func() error { _, err := repo.SchemaTables(ctx); return err },
		"DescribeDatabase":   func() error { _, err := repo.DescribeDatabaseTable(ctx); return err },
		"DescribeForeignKey": func() error { _, err := repo.DescribeForeignKeysBySchema(ctx, "main"); return err },
		"Query": func() error {
			rows, err := repo.Query(ctx, "SELECT 1")
			if err != nil {
				return err
			}
			return rows.Close()
		},
	}
	for name, call := range calls {
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := duckdbWriteFromHelper(t, path); err != nil {
			t.Errorf("after %s another process cannot write to the file: %v", name, err)
		}
	}

	// The inverse: while rows are still open the file is held, so the helper
	// must fail. This guards against the test passing because the lock never
	// existed in the first place.
	rows, err := repo.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdbWriteFromHelper(t, path); err == nil {
		t.Error("another process could write while a query's rows were open")
	}
	rows.Close()
	if err := duckdbWriteFromHelper(t, path); err != nil {
		t.Errorf("after closing the rows another process cannot write: %v", err)
	}
}

// TestDuckDBHelperProcess is the body of the helper process: it opens the file
// for writing and runs one statement. It does nothing in a normal test run.
func TestDuckDBHelperProcess(t *testing.T) {
	if os.Getenv("SQLS_DUCKDB_HELPER") == "" {
		return
	}
	conn, err := duckdbOpen(&DBConfig{
		DataSourceName: os.Getenv("SQLS_DUCKDB_PATH"),
		Params:         map[string]string{"access_mode": "read_write"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Conn.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS probe (i INTEGER)"); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

func duckdbWriteFromHelper(t *testing.T, path string) error {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDuckDBHelperProcess$")
	cmd.Env = append(os.Environ(), "SQLS_DUCKDB_HELPER=1", "SQLS_DUCKDB_PATH="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &helperError{err: err, output: string(out)}
	}
	return nil
}

type helperError struct {
	err    error
	output string
}

func (e *helperError) Error() string { return e.err.Error() + ": " + e.output }

// duckdbTestPath reads the file path back from the open repository, so the
// helper opens the same file the repository uses.
func duckdbTestPath(t *testing.T, repo DBRepository) string {
	t.Helper()
	row := repo.(*DuckDBRepository).Conn.QueryRowContext(context.Background(),
		"SELECT path FROM duckdb_databases() WHERE database_name = current_database()")
	var path string
	if err := row.Scan(&path); err != nil {
		t.Fatal(err)
	}
	return path
}
