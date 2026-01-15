package memory_test

import (
	"testing"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
	"github.com/NoahNaiman/relay/internal/store/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(_ *testing.T, clk clock.Clock) store.Store {
		return memory.New(memory.WithClock(clk))
	})
}
