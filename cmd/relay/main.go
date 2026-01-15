// Command relay runs the webhook delivery server.
package main

import (
	"fmt"
	"os"

	"github.com/NoahNaiman/relay/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	fmt.Printf("relay configured: grpc=%s http=%s\n", cfg.GRPCAddr, cfg.HTTPAddr)
}
