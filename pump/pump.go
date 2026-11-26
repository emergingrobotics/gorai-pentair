// Package pump provides a gorai sensor component for Pentair IntelliFlo 3
// (IF31) variable-speed pool pumps.
//
// The component wraps the gopentair client library. It follows the pump's
// state through gopentair's WebSocket Watch and a REST poll (which doubles
// as heartbeat and health check), caches the newest snapshot for Readings(),
// and, when given a NATS connection, publishes:
//
//   - snapshots as JSON on gorai.<robot_id>.<name>.data: on every poll, and
//     on pushed changes (coalesced to at most one per min_publish_interval)
//   - events on gorai.<robot_id>.<name>.event: source_down / source_up,
//     run_change, program_change and alarm.
//
// Fields the cloud did not report are left out, never sent as 0. Fields whose
// scale or units are unverified (pressure, remaining time, program schedule
// fields) are not published. The raw device ID is never published or logged;
// the pump is identified by device_ref, a short one-way hash of it.
package pump

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emergingrobotics/gopentair"
	"github.com/emergingrobotics/gorai/pkg/registry"
	"github.com/emergingrobotics/gorai/pkg/resource"
	"github.com/nats-io/nats.go"
)

const (
	defaultPollInterval       = 60 * time.Second
	minimumPollInterval       = 60 * time.Second
	defaultMinPublishInterval = 5 * time.Second
	defaultName               = "pool-pump"

	// failThreshold is how many consecutive failed polls mark the source down.
	failThreshold = 3

	watchBackoffMin = 5 * time.Second
	watchBackoffMax = 5 * time.Minute
)

// Event kinds and reasons published on the .event subject.
const (
	EventSourceDown    = "source_down"
	EventSourceUp      = "source_up"
	EventRunChange     = "run_change"
	EventProgramChange = "program_change"
	EventAlarm         = "alarm"

	ReasonAPIError       = "api_error"
	ReasonDeviceOffline  = "device_offline"
	ReasonReauthRequired = "reauth_required"
)

func init() {
	registry.RegisterComponent("sensor", "pentair-pump", New)
}

// Config holds parsed and validated configuration for the component.
type Config struct {
	Username           string
	Password           string // optional when the token store holds a session
	Device             string // ID, alias, nickname or device_ref; empty auto-detects
	Region             string
	TokenFile          string // "" = gopentair default file, "none" = memory only
	PollInterval       time.Duration
	MinPublishInterval time.Duration
	Watch              bool
}

// DeviceRef returns the stable, non-secret identifier published for a device:
// the first 12 hex characters of the SHA-256 of its ID.
func DeviceRef(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])[:12]
}

// Pump is a gorai sensor component backed by the Pentair Home cloud.
type Pump struct {
	name   resource.Name
	logger *slog.Logger
	now    func() time.Time

	mu         sync.RWMutex // guards config, client, device, ref, readings, lastUpdate, pollErr
	config     *Config
	client     gopentair.Client
	device     gopentair.Device
	ref        string
	readings   map[string]any
	lastUpdate time.Time
	pollErr    error

	ingestMu      sync.Mutex // serializes ingest/poll bookkeeping; guards the fields below
	errCount      int        // consecutive failed polls
	authFailed    bool
	downReason    string // "" when up
	haveState     bool
	lastRunning   *bool
	lastProgram   int // 0 = none
	lastAlarm     *int64
	lastPublish   time.Time
	lastPublished map[string]any
	pending       bool
	flushTimer    *time.Timer
	watchSource   string

	natsConn   *nats.Conn
	topic      string
	eventTopic string

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// clientFactory builds the Pentair client. Tests replace it.
var clientFactory = func(cfg *Config, logger *slog.Logger) (gopentair.Client, error) {
	opts := []gopentair.Option{
		gopentair.WithUsername(cfg.Username),
		gopentair.WithPassword(cfg.Password),
		gopentair.WithLogger(logger), // gopentair wraps it in its redacting handler
	}
	if cfg.Region != "" {
		r, err := gopentair.RegionByName(cfg.Region)
		if err != nil {
			return nil, err
		}
		opts = append(opts, gopentair.WithRegion(r))
	}
	switch {
	case strings.EqualFold(cfg.TokenFile, "none"):
		opts = append(opts, gopentair.WithTokenStore(gopentair.MemoryTokenStore()))
	case cfg.TokenFile != "":
		opts = append(opts, gopentair.WithTokenStore(gopentair.FileTokenStore(cfg.TokenFile)))
	default:
		opts = append(opts, gopentair.WithDefaultTokenFile())
	}
	return gopentair.NewClient(opts...)
}

// New creates the component, logs in, resolves the pump and starts
// watching and polling.
func New(ctx context.Context, deps registry.Dependencies, conf registry.Config) (any, error) {
	cfg, err := parseConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	logger := componentLogger(conf)

	client, err := clientFactory(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("creating pentair client: %w", err)
	}
	dev, err := resolveDevice(ctx, client, cfg.Device)
	if err != nil {
		return nil, err
	}

	var nc *nats.Conn
	if deps != nil {
		if v, err := deps.Get("nats"); err == nil {
			nc, _ = v.(*nats.Conn)
		}
	}

	p := newPump(conf, cfg, client, dev, nc, time.Now)
	if nc != nil {
		p.logger.Info("NATS publishing enabled", "topic", p.topic, "event_topic", p.eventTopic, "device_ref", p.ref)
	} else {
		p.logger.Warn("NATS connection not available, data will not be published")
	}
	p.start(ctx)
	return p, nil
}

func componentName(conf registry.Config) string {
	if n, _ := conf["name"].(string); n != "" {
		return n
	}
	return defaultName
}

func componentLogger(conf registry.Config) *slog.Logger {
	return slog.Default().With("component", "sensor", "model", "pentair-pump", "name", componentName(conf))
}

// newPump builds the component without starting its loops.
func newPump(conf registry.Config, cfg *Config, client gopentair.Client, dev gopentair.Device, nc *nats.Conn, now func() time.Time) *Pump {
	name := componentName(conf)
	namespace := "gorai"
	if ns, ok := conf["namespace"].(string); ok && ns != "" {
		namespace = ns
	}
	robotID := namespace
	if rid, ok := conf["robot_id"].(string); ok && rid != "" {
		robotID = rid
	}
	return &Pump{
		name:       resource.NewComponentName(namespace, "sensor", name),
		logger:     componentLogger(conf),
		now:        now,
		config:     cfg,
		client:     client,
		device:     dev,
		ref:        DeviceRef(dev.ID),
		natsConn:   nc,
		topic:      fmt.Sprintf("gorai.%s.%s.data", robotID, name),
		eventTopic: fmt.Sprintf("gorai.%s.%s.event", robotID, name),
	}
}

func parseConfig(conf registry.Config) (*Config, error) {
	cfg := &Config{
		PollInterval:       defaultPollInterval,
		MinPublishInterval: defaultMinPublishInterval,
		Watch:              true,
	}
	str := func(key string) string { s, _ := conf[key].(string); return s }

	if cfg.Username = str("username"); cfg.Username == "" {
		return nil, errors.New("username is required")
	}
	// The password may be empty when a cached session exists; without one
	// Login fails with gopentair.ErrReauthRequired.
	cfg.Password = str("password")
	cfg.Device = str("device")
	cfg.TokenFile = str("token_file")

	if cfg.Region = str("region"); cfg.Region != "" {
		if _, err := gopentair.RegionByName(cfg.Region); err != nil {
			return nil, fmt.Errorf("invalid region %q: %w", cfg.Region, err)
		}
	}

	dur := func(key string, def, minimum time.Duration) (time.Duration, error) {
		s := str(key)
		if s == "" {
			return def, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q: %w", key, s, err)
		}
		if d < minimum {
			return 0, fmt.Errorf("%s %s is below minimum %s", key, d, minimum)
		}
		return d, nil
	}
	var err error
	if cfg.PollInterval, err = dur("poll_interval", defaultPollInterval, minimumPollInterval); err != nil {
		return nil, err
	}
	if cfg.MinPublishInterval, err = dur("min_publish_interval", defaultMinPublishInterval, 0); err != nil {
		return nil, err
	}

	switch v := conf["watch"].(type) {
	case nil:
	case bool:
		cfg.Watch = v
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid watch %q: %w", v, err)
		}
		cfg.Watch = b
	default:
		return nil, fmt.Errorf("invalid watch %v: want a boolean", v)
	}
	return cfg, nil
}

// resolveDevice logs in and finds the IntelliFlo 3 named by ref (device_ref,
// ID, alias or nickname), or the account's only IntelliFlo 3 when ref is
// empty. Errors never echo ref, which may be a raw device ID.
func resolveDevice(ctx context.Context, client gopentair.Client, ref string) (gopentair.Device, error) {
	if err := client.Login(ctx); err != nil {
		return gopentair.Device{}, fmt.Errorf("pentair login: %w", err)
	}
	if ref != "" {
		devs, err := client.Devices(ctx)
		if err != nil {
			return gopentair.Device{}, fmt.Errorf("listing devices: %w", err)
		}
		for _, d := range devs {
			if d.Type == gopentair.TypeIntelliFlo3 && strings.EqualFold(DeviceRef(d.ID), ref) {
				return d, nil
			}
		}
	}
	d, err := client.ResolveDevice(ctx, ref, gopentair.TypeIntelliFlo3)
	if err != nil {
		if errors.Is(err, gopentair.ErrAmbiguousDevice) {
			return gopentair.Device{}, fmt.Errorf("resolving pump: %w (set device to a device_ref, alias or nickname)", err)
		}
		return gopentair.Device{}, fmt.Errorf("resolving pump: %w", err)
	}
	if d.Type != gopentair.TypeIntelliFlo3 {
		return gopentair.Device{}, fmt.Errorf("resolving pump: %w: device is a %s, not an IntelliFlo 3", gopentair.ErrUnsupportedDevice, d.Type.Description())
	}
	return *d, nil
}

func (p *Pump) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	p.cancel = cancel

	p.mu.RLock()
	cfg := p.config
	p.mu.RUnlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.pollLoop(ctx, cfg.PollInterval)
	}()
	if cfg.Watch {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.watchLoop(ctx)
		}()
	}
}

func (p *Pump) stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	p.ingestMu.Lock()
	if p.flushTimer != nil {
		p.flushTimer.Stop()
		p.flushTimer = nil
	}
	p.pending = false
	p.ingestMu.Unlock()
}

func (p *Pump) pollLoop(ctx context.Context, interval time.Duration) {
	p.poll(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.poll(ctx)
		}
	}
}

// watchLoop follows WebSocket pushes, restarting the watch with backoff when
// it cannot start. gopentair's own poll fallback is off: pollLoop covers it.
func (p *Pump) watchLoop(ctx context.Context) {
	p.mu.RLock()
	client, id := p.client, p.device.ID
	p.mu.RUnlock()

	backoff := watchBackoffMin
	for ctx.Err() == nil {
		events, err := client.Watch(ctx, []string{id}, gopentair.WithPollFallback(0))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isAuthFailure(err) {
				p.logger.Error("watch: authentication failed, not retrying", "error", err)
				return
			}
			p.logger.Warn("watch failed to start, retrying", "error", err, "in", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, watchBackoffMax)
			continue
		}
		backoff = watchBackoffMin
		p.logger.Debug("watch started")
		for ev := range events {
			if ev.DeviceID != id {
				continue
			}
			p.ingest(ev.State, ev.Source.String(), false)
		}
		if ctx.Err() == nil {
			p.logger.Warn("watch stream ended, restarting")
			if !sleepCtx(ctx, backoff) {
				return
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func isAuthFailure(err error) bool {
	var ce *gopentair.ChallengeError
	return errors.Is(err, gopentair.ErrReauthRequired) ||
		errors.Is(err, gopentair.ErrInvalidCredentials) ||
		errors.As(err, &ce)
}

// poll reads the pump state over REST, publishes it and updates source state.
func (p *Pump) poll(ctx context.Context) {
	p.mu.RLock()
	client, id := p.client, p.device.ID
	p.mu.RUnlock()

	st, err := client.State(ctx, id)
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down
		}
		p.mu.Lock()
		p.pollErr = err
		p.mu.Unlock()

		p.ingestMu.Lock()
		p.errCount++
		auth := isAuthFailure(err)
		if auth {
			p.authFailed = true
		}
		p.logger.Error("poll failed", "error", err, "consecutive", p.errCount)
		p.updateSource(false)
		p.ingestMu.Unlock()

		if auth && p.cancel != nil {
			// A human has to log in again; stop hammering Cognito.
			p.logger.Error("authentication failed; stopping until reconfigured or restarted")
			p.cancel()
		}
		return
	}
	p.mu.Lock()
	p.pollErr = nil
	p.mu.Unlock()
	p.ingest(*st, "poll", true)
}

// ingest decodes a state, emits transition events and publishes the
// snapshot. fromPoll marks a successful REST poll: it resets the error count
// and always publishes (heartbeat). Other sources are de-duplicated and
// rate limited to MinPublishInterval.
func (p *Pump) ingest(st gopentair.DeviceState, source string, fromPoll bool) {
	p.ingestMu.Lock()
	defer p.ingestMu.Unlock()

	p.mu.RLock()
	dev, ref, minPub := p.device, p.ref, p.config.MinPublishInterval
	p.mu.RUnlock()

	now := p.now()
	ps := gopentair.DecodePump(st)
	r := toReadings(ps, dev, ref, source, now)

	p.mu.Lock()
	p.readings = r
	p.lastUpdate = now
	p.mu.Unlock()

	if fromPoll {
		p.errCount = 0
		p.authFailed = false
	} else {
		p.watchSource = source
	}
	p.updateSource(st.Online)
	if st.Online {
		p.transitions(ps, ref, now)
	}

	switch {
	case fromPoll:
		p.publishData(r, now)
	case sameReadings(r, p.lastPublished):
		// Nothing we publish changed.
	case now.Sub(p.lastPublish) >= minPub:
		p.publishData(r, now)
	default:
		p.pending = true
		if p.flushTimer == nil {
			p.flushTimer = time.AfterFunc(minPub-now.Sub(p.lastPublish), p.flush)
		}
	}
}

// flush publishes a snapshot held back by the rate limit.
func (p *Pump) flush() {
	p.ingestMu.Lock()
	defer p.ingestMu.Unlock()
	p.flushTimer = nil
	if !p.pending {
		return
	}
	p.mu.RLock()
	r := p.readings
	p.mu.RUnlock()
	p.publishData(r, p.now())
}

// publishData must hold ingestMu.
func (p *Pump) publishData(r map[string]any, now time.Time) {
	p.pending = false
	p.lastPublish = now
	p.lastPublished = r
	p.publish(p.topic, r)
}

// sameReadings compares two snapshots ignoring ts and source.
func sameReadings(a, b map[string]any) bool {
	if a == nil || b == nil {
		return false
	}
	strip := func(m map[string]any) map[string]any {
		c := maps.Clone(m)
		delete(c, "ts")
		delete(c, "source")
		return c
	}
	return reflect.DeepEqual(strip(a), strip(b))
}

// transitions publishes run_change, program_change and alarm events. Must
// hold ingestMu.
func (p *Pump) transitions(ps gopentair.PumpStatus, ref string, now time.Time) {
	ts := now.UTC().Format(time.RFC3339)
	prog := 0
	if ps.ActiveProgram != nil {
		prog = *ps.ActiveProgram
	}

	runChanged := false
	if ps.PowerW != nil {
		running := ps.Running()
		if p.lastRunning != nil && *p.lastRunning != running {
			runChanged = true
			ev := map[string]any{
				"ts": ts, "device_ref": ref, "kind": EventRunChange,
				"from": runWord(*p.lastRunning), "to": runWord(running),
			}
			if running && prog > 0 {
				ev["program"] = prog
				ev["program_name"] = programName(ps, prog)
			}
			p.logger.Info("pump run state changed", "from", ev["from"], "to", ev["to"])
			p.publish(p.eventTopic, ev)
		}
		p.lastRunning = &running
	}

	// A start or stop already carries the program in run_change.
	if p.haveState && prog != p.lastProgram && !runChanged {
		ev := map[string]any{
			"ts": ts, "device_ref": ref, "kind": EventProgramChange,
			"from": p.lastProgram, "to": prog,
		}
		if prog > 0 {
			ev["program_name"] = programName(ps, prog)
		}
		p.logger.Info("pump program changed", "from", p.lastProgram, "to", prog)
		p.publish(p.eventTopic, ev)
	}
	p.lastProgram = prog

	if ps.AlarmBits != nil {
		bits := *ps.AlarmBits
		if (p.lastAlarm == nil && bits != 0) || (p.lastAlarm != nil && *p.lastAlarm != bits) {
			p.logger.Warn("pump alarm changed", "alarm_bits", bits)
			p.publish(p.eventTopic, map[string]any{
				"ts": ts, "device_ref": ref, "kind": EventAlarm,
				"raised": bits != 0, "alarm_bits": bits,
			})
		}
		p.lastAlarm = &bits
	}
	p.haveState = true
}

func runWord(running bool) string {
	if running {
		return "running"
	}
	return "stopped"
}

func programName(ps gopentair.PumpStatus, n int) string {
	for _, pr := range ps.Programs {
		if pr.Number == n {
			return pr.Name
		}
	}
	return ""
}

// updateSource publishes source_down / source_up when the source state
// changes. online is ignored when the last poll failed. Must hold ingestMu.
func (p *Pump) updateSource(online bool) {
	want := p.downReason
	switch {
	case p.authFailed:
		want = ReasonReauthRequired
	case p.errCount >= failThreshold:
		want = ReasonAPIError
	case p.errCount > 0:
		// Transient failure: keep the current state.
	case !online:
		want = ReasonDeviceOffline
	default:
		want = ""
	}
	if want == p.downReason {
		return
	}
	p.downReason = want

	p.mu.RLock()
	ref := p.ref
	p.mu.RUnlock()
	ev := map[string]any{
		"ts":         p.now().UTC().Format(time.RFC3339),
		"device_ref": ref,
		"kind":       EventSourceUp,
	}
	if want != "" {
		ev["kind"] = EventSourceDown
		ev["reason"] = want
	}
	p.logger.Info("source state changed", "kind", ev["kind"], "reason", want)
	p.publish(p.eventTopic, ev)
}

// toReadings flattens a decoded state into unit-suffixed keys. Values the
// cloud did not report, and fields with unverified scale, are omitted.
func toReadings(ps gopentair.PumpStatus, dev gopentair.Device, ref, source string, now time.Time) map[string]any {
	r := map[string]any{
		"ts":         now.UTC().Format(time.RFC3339),
		"device_ref": ref,
		"online":     ps.Online,
		"source":     source,
	}
	switch {
	case ps.Nickname != "":
		r["nickname"] = ps.Nickname
	case dev.Nickname != "":
		r["nickname"] = dev.Nickname
	}
	if ps.PowerW != nil {
		r["power_w"] = *ps.PowerW
		r["running"] = ps.Running()
	}
	if ps.SpeedPct != nil {
		r["speed_pct"] = *ps.SpeedPct
	}
	if ps.FlowGPM != nil {
		r["flow_gpm"] = *ps.FlowGPM
	}
	if ps.Enabled != nil {
		r["enabled"] = *ps.Enabled
	}
	if ps.AlarmBits != nil {
		r["alarm_bits"] = *ps.AlarmBits
		r["alarm"] = *ps.AlarmBits != 0
	}
	if ps.RSSI != nil {
		r["rssi"] = *ps.RSSI
	}
	if ps.ActiveProgram != nil {
		r["active_program"] = *ps.ActiveProgram
		if n := programName(ps, *ps.ActiveProgram); n != "" {
			r["active_program_name"] = n
		}
	}
	// PressurePSI (s17, scale unverified) and RemainingRaw (s28, units
	// unverified) are deliberately not published.
	programs := make([]map[string]any, 0, len(ps.Programs))
	for _, pr := range ps.Programs {
		m := map[string]any{
			"number":  pr.Number,
			"name":    pr.Name,
			"type":    pr.TypeName,
			"running": pr.Running,
		}
		if pr.Value != nil {
			m["value"] = *pr.Value
		}
		programs = append(programs, m)
	}
	r["programs"] = programs
	return r
}

// publish marshals v to JSON and publishes it. Errors are logged only.
func (p *Pump) publish(topic string, v map[string]any) {
	if p.natsConn == nil {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		p.logger.Error("marshal for NATS", "error", err)
		return
	}
	if err := p.natsConn.Publish(topic, b); err != nil {
		p.logger.Error("publish to NATS", "topic", topic, "error", err)
	}
}

// Name returns the unique resource identifier.
func (p *Pump) Name() resource.Name { return p.name }

// DeviceRef returns the published identifier of the pump.
func (p *Pump) DeviceRef() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ref
}

// Readings returns the newest snapshot plus "data_age_s".
func (p *Pump) Readings(ctx context.Context) (map[string]any, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.readings == nil {
		if p.pollErr != nil {
			return nil, fmt.Errorf("no readings available, last poll error: %w", p.pollErr)
		}
		return nil, errors.New("no readings available yet")
	}
	out := maps.Clone(p.readings)
	out["data_age_s"] = int64(p.now().Sub(p.lastUpdate).Seconds())
	return out, nil
}

// Reconfigure applies new configuration and restarts the loops.
func (p *Pump) Reconfigure(ctx context.Context, deps resource.Dependencies, conf resource.Config) error {
	cfg, err := parseConfig(registry.Config(conf.Attributes))
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	client, err := clientFactory(cfg, p.logger)
	if err != nil {
		return fmt.Errorf("creating pentair client: %w", err)
	}
	dev, err := resolveDevice(ctx, client, cfg.Device)
	if err != nil {
		return err
	}

	p.stop()

	p.ingestMu.Lock()
	p.mu.Lock()
	if dev.ID != p.device.ID {
		p.readings = nil
		p.downReason = ""
		p.haveState = false
		p.lastRunning, p.lastAlarm, p.lastProgram = nil, nil, 0
		p.lastPublished = nil
	}
	p.config, p.client, p.device, p.ref = cfg, client, dev, DeviceRef(dev.ID)
	p.pollErr = nil
	p.errCount = 0
	p.authFailed = false
	p.mu.Unlock()
	p.ingestMu.Unlock()

	p.start(ctx)
	return nil
}

// DoCommand handles ad-hoc commands:
//
//	{"command":"refresh"}
//	{"command":"get_config"}
//	{"command":"get_state"}
//	{"command":"list_programs"}
//	{"command":"start_program","program":2}            // number or name
//	{"command":"start_program","program":"Filter","dry_run":true}
//	{"command":"stop_program","program":2}
//	{"command":"stop"}                                 // whatever is running
//
// Writes go through gopentair's WritePlan / Apply: debounced, confirmed
// against the device state, and never retried after an ambiguous failure.
func (p *Pump) DoCommand(ctx context.Context, cmd map[string]any) (map[string]any, error) {
	p.mu.RLock()
	cfg, client, id, ref := p.config, p.client, p.device.ID, p.ref
	p.mu.RUnlock()

	name, _ := cmd["command"].(string)
	dryRun, _ := cmd["dry_run"].(bool)
	pump := gopentair.NewPump(client, id)

	switch name {
	case "refresh":
		return p.refresh(ctx), nil
	case "get_config":
		region := cfg.Region
		if region == "" {
			region = "us"
		}
		return map[string]any{
			"device_ref":           ref,
			"nickname":             p.device.Name(),
			"region":               region,
			"poll_interval":        cfg.PollInterval.String(),
			"min_publish_interval": cfg.MinPublishInterval.String(),
			"watch":                cfg.Watch,
		}, nil
	case "get_state":
		p.ingestMu.Lock()
		reason, errs, src := p.downReason, p.errCount, p.watchSource
		p.ingestMu.Unlock()
		st := map[string]any{"up": reason == "", "consecutive_errors": errs}
		if reason != "" {
			st["reason"] = reason
		}
		if src != "" {
			st["watch_source"] = src
		}
		return st, nil
	case "list_programs":
		ps, _, err := pump.Status(ctx)
		if err != nil {
			return nil, err
		}
		r := toReadings(ps, p.device, ref, "poll", p.now())
		return map[string]any{"programs": r["programs"]}, nil
	case "start_program", "stop_program":
		prog, err := programArg(cmd)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		var plan gopentair.WritePlan
		if name == "start_program" {
			plan, err = pump.StartPlan(ctx, prog)
		} else {
			plan, err = pump.StopPlan(ctx, prog)
		}
		if err != nil {
			return nil, err
		}
		return p.apply(ctx, client, plan, dryRun)
	case "stop":
		ps, st, err := pump.Status(ctx)
		if err != nil {
			return nil, err
		}
		if ps.ActiveProgram == nil {
			return map[string]any{"status": "ok", "applied": 0, "note": "no program running"}, nil
		}
		plan, err := gopentair.PumpStopPlan(*st, *ps.ActiveProgram)
		if err != nil {
			return nil, err
		}
		return p.apply(ctx, client, plan, dryRun)
	default:
		return nil, fmt.Errorf("unknown command: %q", name)
	}
}

func programArg(cmd map[string]any) (string, error) {
	switch v := cmd["program"].(type) {
	case float64:
		if v != float64(int(v)) {
			return "", fmt.Errorf("%w: program %v is not a whole number", gopentair.ErrInvalidValue, v)
		}
		return strconv.Itoa(int(v)), nil
	case int:
		return strconv.Itoa(v), nil
	case string:
		if strings.TrimSpace(v) == "" {
			break
		}
		return v, nil
	}
	return "", errors.New(`requires "program" (number or name)`)
}

// apply runs a plan (or describes it on dry run) and refreshes afterwards.
func (p *Pump) apply(ctx context.Context, client gopentair.Client, plan gopentair.WritePlan, dryRun bool) (map[string]any, error) {
	if dryRun {
		return map[string]any{"status": "ok", "dry_run": true, "description": plan.Description, "plan": plan.String()}, nil
	}
	p.logger.Info("applying", "plan", plan.Description)
	res, err := client.Apply(ctx, plan, gopentair.WithConfirmDefault())
	if err != nil && !errors.Is(err, gopentair.ErrNotConfirmed) {
		if res != nil && res.Applied > 0 {
			p.refresh(ctx)
		}
		return nil, err
	}
	reply := map[string]any{
		"status":      "ok",
		"description": plan.Description,
		"applied":     res.Applied,
		"confirmed":   res.Confirmed,
	}
	if err != nil {
		reply["status"] = "not_confirmed"
		reply["error"] = err.Error()
	}
	p.refresh(ctx)
	return reply, nil
}

func (p *Pump) refresh(ctx context.Context) map[string]any {
	p.poll(ctx)
	p.mu.RLock()
	err := p.pollErr
	p.mu.RUnlock()
	if err != nil {
		return map[string]any{"status": "error", "error": err.Error()}
	}
	return map[string]any{"status": "ok"}
}

// Close stops the loops. It does not log out: Logout deletes the cached
// session, and keeping it is what lets a restart skip the SRP login. It
// never signs out everywhere, which would also sign out the phone app.
func (p *Pump) Close(ctx context.Context) error {
	p.stop()
	return nil
}

var _ resource.Sensor = (*Pump)(nil)
