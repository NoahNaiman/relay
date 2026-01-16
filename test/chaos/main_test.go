//go:build chaos

// Package chaos holds the store-level and end-to-end chaos tests. They run on
// the memory store by default and on Postgres when RELAY_TEST_PG=1.
package chaos

import (
	"context"
	"os"
	"testing"

	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
	"github.com/NoahNaiman/relay/internal/store/pgtest"
	"github.com/NoahNaiman/relay/internal/store/postgres"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

// newStore returns a fresh store: Postgres when RELAY_TEST_PG=1, otherwise memory.
func newStore(t *testing.T) store.Store {
	t.Helper()
	if os.Getenv("RELAY_TEST_PG") != "1" {
		t.Log("store: memory (set RELAY_TEST_PG=1 for Postgres)")
		return memory.New()
	}
	s, err := postgres.Open(context.Background(), pgtest.NewDatabase(t))
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Log("store: postgres")
	t.Cleanup(s.Close)
	return s
}
