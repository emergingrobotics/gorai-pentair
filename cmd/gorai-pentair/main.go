// Command gorai-pentair runs the Pentair IntelliFlo 3 pump component
// standalone: it connects to a NATS server (by default the one on this
// host), follows the pump through the Pentair Home cloud and publishes
// readings and events to the bus.
//
// With --do it instead runs one DoCommand against the pump, prints the reply
// as JSON and exits (no NATS needed):
//
//	gorai-pentair --do '{"command":"list_programs"}'
//	gorai-pentair --do '{"command":"start_program","program":"Filter","dry_run":true}'
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/emergingrobotics/gopentair"
	"github.com/emergingrobotics/gorai/pkg/mesh"
	"github.com/emergingrobotics/gorai/pkg/registry"
	"github.com/nats-io/nats.go"
	"golang.org/x/term"

	"github.com/emergingrobotics/gorai-pentair/pump"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	natsURL := flag.String("nats", envOr("NATS_URL", nats.DefaultURL), "NATS server URL (or NATS_URL)")
	robotID := flag.String("robot-id", "robot", "Robot ID: first segment of gorai.<robot-id>.<name>.data")
	name := flag.String("name", "pool-pump", "Component name")
	username := flag.String("username", "", "Pentair Home username (or PENTAIR_USERNAME)")
	password := flag.String("password", "", "Pentair Home password (or PENTAIR_PASSWORD); optional with a cached session, prompted on a terminal if needed")
	device := flag.String("device", "", "Pump device_ref, ID, alias or nickname; auto-detected with one pump (or PENTAIR_DEFAULT_DEVICE)")
	region := flag.String("region", "", "Pentair region, default us (or PENTAIR_REGION)")
	tokenFile := flag.String("token-file", "", `Session cache file; "none" disables (or PENTAIR_TOKEN_FILE; default is the pentair CLI's)`)
	pollInterval := flag.String("poll-interval", "60s", "REST poll / heartbeat interval (minimum 60s)")
	minPublish := flag.String("min-publish-interval", "5s", "Minimum gap between pushed .data messages")
	noWatch := flag.Bool("no-watch", false, "Poll only; don't open the WebSocket")
	doCmd := flag.String("do", "", `Run one DoCommand (JSON) against the pump, print the reply and exit`)
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	noMesh := flag.Bool("no-mesh", false, "Skip mesh service discovery (use when NATS has no JetStream)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gorai-pentair %s (commit %s, built %s)\n", version, commit, buildTime)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	conf := registry.Config{
		"name":                 *name,
		"robot_id":             *robotID,
		"username":             orEnv(*username, "PENTAIR_USERNAME"),
		"password":             orEnv(*password, "PENTAIR_PASSWORD"),
		"device":               orEnv(*device, "PENTAIR_DEFAULT_DEVICE"),
		"region":               orEnv(*region, "PENTAIR_REGION"),
		"token_file":           orEnv(*tokenFile, "PENTAIR_TOKEN_FILE"),
		"poll_interval":        *pollInterval,
		"min_publish_interval": *minPublish,
		"watch":                !*noWatch && *doCmd == "",
	}
	if conf["username"] == "" {
		fmt.Fprintln(os.Stderr, "error: username required (--username or PENTAIR_USERNAME)")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *doCmd != "" {
		os.Exit(runOnce(ctx, conf, *doCmd))
	}

	nc, err := nats.Connect(*natsURL,
		nats.Name("gorai-pentair"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { logger.Warn("NATS disconnected", "error", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { logger.Info("NATS reconnected", "url", c.ConnectedUrl()) }),
	)
	if err != nil {
		logger.Error("connect to NATS", "url", *natsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	logger.Info("connected to NATS", "url", nc.ConnectedUrl())

	comp, err := newPump(ctx, natsDeps{nc}, conf)
	if err != nil {
		logger.Error("create component", "error", err)
		os.Exit(1)
	}
	p := comp

	var reg *mesh.Registration
	if !*noMesh {
		if mc, err := mesh.NewClient(nc, mesh.WithClientLogger(logger)); err != nil {
			logger.Warn("mesh discovery unavailable, continuing without it (--no-mesh to silence)", "error", err)
		} else if reg, err = mc.Register(ctx, mesh.ServiceDescriptor{
			Name:     *name,
			Type:     mesh.TypeComponent,
			Subtype:  "sensor",
			Model:    "pentair-pump",
			RobotID:  *robotID,
			Metadata: map[string]string{"version": version, "device_ref": p.DeviceRef()},
		}); err != nil {
			logger.Warn("mesh registration failed, continuing without it", "error", err)
			reg = nil
		} else {
			logger.Info("registered with mesh service discovery")
		}
	}

	logger.Info("publishing",
		"data", fmt.Sprintf("gorai.%s.%s.data", *robotID, *name),
		"events", fmt.Sprintf("gorai.%s.%s.event", *robotID, *name),
		"device_ref", p.DeviceRef(),
		"poll_interval", *pollInterval)

	<-ctx.Done()
	logger.Info("shutting down")
	if reg != nil {
		reg.Deregister()
	}
	closeCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	if err := p.Close(closeCtx); err != nil {
		logger.Warn("close component", "error", err)
	}
	if err := nc.Drain(); err != nil {
		logger.Warn("drain NATS", "error", err)
	}
}

// newPump creates the component. With no password and no usable cached
// session it asks for the password on the terminal (when there is one) and
// tries once more; the session it gets is cached for the next start.
func newPump(ctx context.Context, deps registry.Dependencies, conf registry.Config) (*pump.Pump, error) {
	comp, err := pump.New(ctx, deps, conf)
	if err != nil && conf["password"] == "" && errors.Is(err, gopentair.ErrReauthRequired) {
		if !term.IsTerminal(int(os.Stdin.Fd())) {
			return nil, fmt.Errorf("%w: set PENTAIR_PASSWORD (or --password), or run once on a terminal to be prompted", err)
		}
		fmt.Fprintf(os.Stderr, "Pentair Home password for %s: ", conf["username"])
		pw, rerr := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if rerr != nil {
			return nil, fmt.Errorf("reading password: %w", rerr)
		}
		conf["password"] = string(pw)
		comp, err = pump.New(ctx, deps, conf)
	}
	if err != nil {
		return nil, err
	}
	return comp.(*pump.Pump), nil
}

// runOnce builds the component without NATS, runs one command and prints
// the reply.
func runOnce(ctx context.Context, conf registry.Config, raw string) int {
	var cmd map[string]any
	if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
		fmt.Fprintln(os.Stderr, "error: --do is not a JSON object:", err)
		return 2
	}
	comp, err := newPump(ctx, nil, conf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	p := comp
	defer p.Close(context.Background())

	reply, err := p.DoCommand(ctx, cmd)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	out, _ := json.MarshalIndent(reply, "", "  ")
	fmt.Println(string(out))
	return 0
}

// natsDeps hands the NATS connection to the component, as the gorai runtime
// does in a robot binary.
type natsDeps struct{ nc *nats.Conn }

func (d natsDeps) Get(name string) (any, error) {
	if name == "nats" {
		return d.nc, nil
	}
	return nil, fmt.Errorf("dependency %q not available", name)
}

func (d natsDeps) GetByType(subtype string) ([]any, error) {
	return nil, fmt.Errorf("no dependencies of type %q", subtype)
}

func orEnv(v, key string) string {
	if v != "" {
		return v
	}
	return os.Getenv(key)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
