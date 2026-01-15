package postgres_test

import (
	"context"
	"testing"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/pgtest"
	"github.com/NoahNaiman/relay/internal/store/postgres"
	"github.com/NoahNaiman/relay/internal/store/storetest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T, clk clock.Clock) store.Store {
		s, err := postgres.Open(context.Background(), pgtest.NewDatabase(t), postgres.WithClock(clk))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	dsn := pgtest.NewDatabase(t)
	for range 2 {
		s, err := postgres.Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
}
