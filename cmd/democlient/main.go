// Command democlient registers a healthy and a flaky endpoint, enqueues jobs
// to both, and reports delivered, retrying and dead counts as relay works.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
)

type counts struct {
	delivered, retried, retrying, dead int
}

func main() {
	addr := flag.String("addr", "localhost:9090", "relay gRPC address")
	healthyURL := flag.String("healthy-url", "http://receiver-healthy:8081/hook", "URL of the healthy receiver, as seen by relay")
	flakyURL := flag.String("flaky-url", "http://receiver-flaky:8081/hook", "URL of the flaky receiver, as seen by relay")
	secret := flag.String("secret", os.Getenv("RECEIVER_SECRET"), "HMAC secret shared with the receivers")
	jobs := flag.Int("jobs", 200, "total jobs to enqueue, split across both endpoints")
	flakyAttempts := flag.Int("flaky-max-attempts", 2, "max_attempts for the flaky endpoint (low, so some jobs dead-letter)")
	wait := flag.Duration("wait", 90*time.Second, "how long to wait for jobs to settle")
	flag.Parse()
	if err := run(*addr, *healthyURL, *flakyURL, *secret, *jobs, *flakyAttempts, *wait); err != nil {
		fmt.Fprintln(os.Stderr, "democlient:", err)
		os.Exit(1)
	}
}

func run(addr, healthyURL, flakyURL, secret string, jobs, flakyAttempts int, wait time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	c := relayv1.NewRelayServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), wait+30*time.Second)
	defer cancel()

	healthy, err := c.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: healthyURL, Secret: secret, MaxRps: 50})
	if err != nil {
		return fmt.Errorf("register healthy endpoint: %w", err)
	}
	flaky, err := c.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: flakyURL, Secret: secret, MaxRps: 50, MaxAttempts: int32(flakyAttempts)}) //nolint:gosec // small flag value
	if err != nil {
		return fmt.Errorf("register flaky endpoint: %w", err)
	}
	fmt.Printf("registered healthy endpoint %s -> %s\n", healthy.GetEndpoint().GetId(), healthyURL)
	fmt.Printf("registered flaky   endpoint %s -> %s (max_attempts=%d)\n", flaky.GetEndpoint().GetId(), flakyURL, flakyAttempts)

	byEndpoint := map[string][]string{}
	names := map[string]string{healthy.GetEndpoint().GetId(): "healthy", flaky.GetEndpoint().GetId(): "flaky"}
	for i := range jobs {
		ep := healthy.GetEndpoint().GetId()
		if i%2 == 1 {
			ep = flaky.GetEndpoint().GetId()
		}
		r, err := c.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{
			EndpointId:     ep,
			Payload:        fmt.Appendf(nil, `{"event":"order.created","order":%d}`, i),
			IdempotencyKey: fmt.Sprintf("demo-%d-%d", time.Now().Unix(), i),
		})
		if err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
		byEndpoint[ep] = append(byEndpoint[ep], r.GetJob().GetId())
	}
	fmt.Printf("enqueued %d jobs\n\n", jobs)

	deadline := time.Now().Add(wait)
	for {
		total, per, err := tally(ctx, c, byEndpoint)
		if err != nil {
			return err
		}
		fmt.Printf("%s  delivered=%-4d (after retries=%d)  retrying=%-4d dead=%d\n",
			time.Now().Format("15:04:05"), total.delivered, total.retried, total.retrying, total.dead)
		if total.retrying == 0 || time.Now().After(deadline) {
			fmt.Println()
			for ep, n := range per {
				fmt.Printf("%-8s delivered=%-4d retried=%-4d retrying=%-4d dead=%d\n", names[ep], n.delivered, n.retried, n.retrying, n.dead)
			}
			if total.retrying > 0 {
				fmt.Printf("\n%d jobs still retrying after %s\n", total.retrying, wait)
			}
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func tally(ctx context.Context, c relayv1.RelayServiceClient, byEndpoint map[string][]string) (counts, map[string]counts, error) {
	var total counts
	per := map[string]counts{}
	for ep, ids := range byEndpoint {
		var n counts
		for _, id := range ids {
			r, err := c.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
			if err != nil {
				return total, nil, fmt.Errorf("get job: %w", err)
			}
			switch r.GetJob().GetStatus() {
			case relayv1.JobStatus_JOB_STATUS_DELIVERED:
				n.delivered++
				if r.GetJob().GetAttempts() > 1 {
					n.retried++
				}
			case relayv1.JobStatus_JOB_STATUS_DEAD:
				n.dead++
			default:
				n.retrying++
			}
		}
		per[ep] = n
		total.delivered += n.delivered
		total.retried += n.retried
		total.retrying += n.retrying
		total.dead += n.dead
	}
	return total, per, nil
}
