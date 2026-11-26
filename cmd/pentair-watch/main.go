// Command pentair-watch subscribes to the pool pump's NATS subjects and
// prints each message, to check what gorai-pentair publishes.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")
	subject := flag.String("subject", "gorai.*.pool-pump.>", "Subject to subscribe to")
	flag.Parse()

	nc, err := nats.Connect(*url, nats.Name("pentair-watch"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer nc.Close()

	_, err = nc.Subscribe(*subject, func(m *nats.Msg) {
		var out bytes.Buffer
		if json.Indent(&out, m.Data, "", "  ") != nil {
			out.Reset()
			out.Write(m.Data)
		}
		fmt.Printf("[%s] %s\n%s\n\n", time.Now().Format("15:04:05"), m.Subject, out.String())
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "subscribe:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "watching %s on %s (Ctrl+C to stop)\n", *subject, nc.ConnectedUrl())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
