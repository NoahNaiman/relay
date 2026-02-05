// Command loadgen enqueues N jobs at a target rate against a running relay,
// receives the webhooks itself, and reports throughput and
// enqueue-to-delivery latency percentiles.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
	"github.com/NoahNaiman/relay/internal/delivery"
)

type options struct {
	addr        string
	listen      string
	callbackURL string
	n           int
	rate        float64
	concurrency int
	payload     int
	endpoints   int
	timeout     time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", "localhost:9090", "relay gRPC address")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:0", "address for loadgen's built-in webhook receiver")
	flag.StringVar(&o.callbackURL, "callback-url", "", "URL relay should deliver to (default: http://<listen>/hook)")
	flag.IntVar(&o.n, "n", 10000, "number of jobs to enqueue")
	flag.Float64Var(&o.rate, "rate", 1000, "target enqueue rate in jobs/second")
	flag.IntVar(&o.concurrency, "concurrency", 32, "concurrent EnqueueJob calls")
	flag.IntVar(&o.payload, "payload", 512, "payload size in bytes")
	flag.IntVar(&o.endpoints, "endpoints", 4, "number of endpoints to spread jobs across")
	flag.DurationVar(&o.timeout, "timeout", 5*time.Minute, "give up waiting for deliveries after this long")
	flag.Parse()
	r, err := run(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	r.print(os.Stdout, o)
}

// recorder keeps the first enqueue-return and first-receipt time per job.
type recorder struct {
	mu        sync.Mutex
	enqueued  map[string]time.Time
	received  map[string]time.Time
	dups      int
	allDone   chan struct{}
	want      int
	closeOnce sync.Once
}

func (r *recorder) receive(id string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.received[id]; ok {
		r.dups++
		return
	}
	r.received[id] = at
	if len(r.received) == r.want {
		r.closeOnce.Do(func() { close(r.allDone) })
	}
}

type result struct {
	enqueueStart, enqueueEnd, lastDelivery time.Time
	enqueueErrors, delivered, duplicates   int
	latencies                              []time.Duration
}

func run(o options) (result, error) {
	rec := &recorder{enqueued: map[string]time.Time{}, received: map[string]time.Time{}, allDone: make(chan struct{}), want: o.n}
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return result{}, err
	}
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		rec.receive(r.Header.Get(delivery.HeaderJobID), time.Now())
		w.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	if o.callbackURL == "" {
		o.callbackURL = "http://" + ln.Addr().String() + "/hook"
	}

	conn, err := grpc.NewClient(o.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return result{}, err
	}
	defer func() { _ = conn.Close() }()
	client := relayv1.NewRelayServiceClient(conn)
	ctx := context.Background()

	var eps []string
	for range o.endpoints {
		r, err := client.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: o.callbackURL, Secret: "loadgen", MaxRps: 1_000_000})
		if err != nil {
			return result{}, fmt.Errorf("register endpoint: %w", err)
		}
		eps = append(eps, r.GetEndpoint().GetId())
	}

	payload := make([]byte, o.payload)
	for i := range payload {
		payload[i] = 'x'
	}
	var res result
	var errMu sync.Mutex
	tickets := make(chan int)
	var wg sync.WaitGroup
	for range o.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tickets {
				r, err := client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: eps[i%len(eps)], Payload: payload})
				now := time.Now()
				if err != nil {
					errMu.Lock()
					res.enqueueErrors++
					errMu.Unlock()
					continue
				}
				rec.mu.Lock()
				rec.enqueued[r.GetJob().GetId()] = now
				rec.mu.Unlock()
			}
		}()
	}
	interval := time.Duration(float64(time.Second) / o.rate)
	res.enqueueStart = time.Now()
	for i := range o.n {
		// Open-loop pacing: ticket i is released at start + i*interval.
		if d := time.Until(res.enqueueStart.Add(time.Duration(i) * interval)); d > 0 {
			time.Sleep(d)
		}
		tickets <- i
	}
	close(tickets)
	wg.Wait()
	res.enqueueEnd = time.Now()
	if res.enqueueErrors > 0 {
		rec.mu.Lock()
		rec.want = o.n - res.enqueueErrors
		if len(rec.received) >= rec.want {
			rec.closeOnce.Do(func() { close(rec.allDone) })
		}
		rec.mu.Unlock()
	}

	select {
	case <-rec.allDone:
	case <-time.After(o.timeout):
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for id, enq := range rec.enqueued {
		got, ok := rec.received[id]
		if !ok {
			continue
		}
		res.delivered++
		// A delivery can arrive before EnqueueJob's response reaches us; count it as 0.
		res.latencies = append(res.latencies, max(0, got.Sub(enq)))
		if got.After(res.lastDelivery) {
			res.lastDelivery = got
		}
	}
	res.duplicates = rec.dups
	slices.Sort(res.latencies)
	return res, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p*float64(len(sorted))+0.5) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

func (r result) print(w io.Writer, o options) {
	enqDur := r.enqueueEnd.Sub(r.enqueueStart)
	totalDur := r.lastDelivery.Sub(r.enqueueStart)
	ms := func(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }
	fmt.Fprintf(w, "jobs=%d target_rate=%.0f/s concurrency=%d payload=%dB endpoints=%d\n", o.n, o.rate, o.concurrency, o.payload, o.endpoints)
	fmt.Fprintf(w, "enqueue:  %d ok, %d errors in %s -> %.0f jobs/s\n", o.n-r.enqueueErrors, r.enqueueErrors, enqDur.Round(time.Millisecond), float64(o.n-r.enqueueErrors)/enqDur.Seconds())
	if r.delivered > 0 {
		fmt.Fprintf(w, "delivery: %d delivered (%d duplicate receipts) in %s -> %.0f jobs/s end to end\n", r.delivered, r.duplicates, totalDur.Round(time.Millisecond), float64(r.delivered)/totalDur.Seconds())
		fmt.Fprintf(w, "enqueue->delivery latency: p50=%s p90=%s p99=%s max=%s\n",
			ms(percentile(r.latencies, 0.50)), ms(percentile(r.latencies, 0.90)), ms(percentile(r.latencies, 0.99)), ms(r.latencies[len(r.latencies)-1]))
	}
	if missing := o.n - r.enqueueErrors - r.delivered; missing > 0 {
		fmt.Fprintf(w, "WARNING: %d jobs not delivered before the timeout\n", missing)
	}
}
