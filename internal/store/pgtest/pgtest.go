// Package pgtest provides throwaway Postgres databases for tests.
//
// Resolution order:
//  1. RELAY_TEST_DATABASE_URL, if set (CI uses a service container this way).
//  2. A testcontainers-managed postgres:16-alpine container, if Docker works.
//  3. Otherwise the calling test is skipped with an explanatory message.
//
// Each call to NewDatabase creates a fresh database, so tests are isolated
// and may run in parallel.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// EnvURL names the variable that points tests at an existing Postgres server.
const EnvURL = "RELAY_TEST_DATABASE_URL"

var (
	once      sync.Once
	baseURL   string
	setupErr  error
	container *tcpostgres.PostgresContainer
)

func start() {
	if u := os.Getenv(EnvURL); u != "" {
		baseURL = u
		return
	}
	defer func() {
		// testcontainers panics when no Docker provider can be found at all.
		if r := recover(); r != nil {
			setupErr = fmt.Errorf("%v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("relay"),
		tcpostgres.WithUsername("relay"),
		tcpostgres.WithPassword("relay"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(time.Minute)),
	)
	if err != nil {
		setupErr = err
		return
	}
	container = c
	baseURL, setupErr = c.ConnectionString(ctx, "sslmode=disable")
}

// Main wraps testing.M so that a container started for this package is
// terminated after the tests finish. Use it from TestMain.
func Main(m *testing.M) {
	code := m.Run()
	if container != nil {
		_ = container.Terminate(context.Background())
	}
	os.Exit(code)
}

// NewDatabase returns the URL of a brand-new, empty database, dropped when
// the test ends. It skips the test if no Postgres is available.
func NewDatabase(t testing.TB) string {
	t.Helper()
	if testing.Short() && os.Getenv(EnvURL) == "" {
		t.Skip("skipping Postgres test in -short mode")
	}
	once.Do(start)
	if setupErr != nil {
		t.Skipf("skipping Postgres test: Docker is not available (%v). Set %s to use an existing server.",
			firstLine(setupErr.Error()), EnvURL)
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to test Postgres: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	name := "relay_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), baseURL)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" {
		t.Fatalf("%s must be a postgres:// URL: %v", EnvURL, err)
	}
	u.Path = "/" + name
	return u.String()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
