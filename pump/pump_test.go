package pump

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emergingrobotics/gopentair"
	"github.com/emergingrobotics/gorai/pkg/registry"
	"github.com/emergingrobotics/gorai/pkg/resource"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const pumpID = "us-east-1:0f1e2d3c-SECRET-DEVICE-ID"

// fakeClient is an in-memory gopentair.Client for one IF31 pump.
type fakeClient struct {
	mu       sync.Mutex
	devices  []gopentair.Device
	fields   map[string]string
	online   bool
	stateErr error
	loginErr error
	applied  []gopentair.WritePlan
	confirm  bool // apply plan effects so Confirm passes
	watches  int
	watchErr error
	watchCh  chan gopentair.Event
	logouts  int
}

func newFake() *fakeClient {
	return &fakeClient{
		devices: []gopentair.Device{
			{ID: pumpID, Type: gopentair.TypeIntelliFlo3, Nickname: "Pool Pump"},
			{ID: "light-1", Type: gopentair.TypeColorSync, Nickname: "Lights"},
		},
		fields: map[string]string{
			"s18": "0", "s19": "0", "s26": "0", "s20": "0", "s25": "1", "s13": "-55",
			"s14": "1", "s17": "1234", "s28": "77", "d27": "Pool Pump",
			"zp1e13": "1", "zp1e2": "Quick Clean", "zp1e5": "2", "zp1e10": "2", "zp1e4": "3000",
			"zp2e13": "1", "zp2e2": "Filter", "zp2e5": "0", "zp2e10": "2", "zp2e4": "1800",
			"zp2e6": "28800", "zp2e7": "14400", "zp2e8": "127",
		},
		online:  true,
		confirm: true,
	}
}

func (f *fakeClient) set(fn func(*fakeClient)) { f.mu.Lock(); fn(f); f.mu.Unlock() }

// running sets power and the active (1-based) program.
func (f *fakeClient) running(prog int, watts string) {
	f.set(func(f *fakeClient) {
		f.fields["s18"] = watts
		f.fields["s14"] = fmt.Sprint(prog - 1)
	})
}

func (f *fakeClient) stateLocked() gopentair.DeviceState {
	st := gopentair.DeviceState{DeviceID: pumpID, Type: gopentair.TypeIntelliFlo3, Online: f.online, Fields: map[string]gopentair.Field{}}
	for k, v := range f.fields {
		st.Fields[k] = gopentair.Field{Code: k, Value: v}
	}
	return st
}

func (f *fakeClient) snapshot() gopentair.DeviceState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stateLocked()
}

func (f *fakeClient) Login(context.Context) error      { return f.loginErr }
func (f *fakeClient) ForceLogin(context.Context) error { return f.loginErr }
func (f *fakeClient) Logout(context.Context, bool) error {
	f.mu.Lock()
	f.logouts++
	f.mu.Unlock()
	return nil
}
func (f *fakeClient) Profile(context.Context) (*gopentair.Profile, error) {
	return &gopentair.Profile{}, nil
}
func (f *fakeClient) Devices(context.Context) ([]gopentair.Device, error) { return f.devices, nil }
func (f *fakeClient) Device(_ context.Context, id string) (*gopentair.Device, error) {
	for i := range f.devices {
		if f.devices[i].ID == id {
			return &f.devices[i], nil
		}
	}
	return nil, gopentair.ErrDeviceNotFound
}

// ResolveDevice mirrors gopentair: exact ID, then nickname among types,
// then the only device of the types.
func (f *fakeClient) ResolveDevice(_ context.Context, ref string, types ...gopentair.DeviceType) (*gopentair.Device, error) {
	match := func(d gopentair.Device) bool {
		for _, t := range types {
			if d.Type == t {
				return true
			}
		}
		return len(types) == 0
	}
	var hits []gopentair.Device
	for _, d := range f.devices {
		if ref != "" && d.ID == ref {
			return &d, nil
		}
		if match(d) && (ref == "" || strings.EqualFold(d.Nickname, ref)) {
			hits = append(hits, d)
		}
	}
	switch len(hits) {
	case 0:
		return nil, gopentair.ErrDeviceNotFound
	case 1:
		return &hits[0], nil
	}
	return nil, gopentair.ErrAmbiguousDevice
}

func (f *fakeClient) State(_ context.Context, id string) (*gopentair.DeviceState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	if id != pumpID {
		return nil, gopentair.ErrDeviceNotFound
	}
	st := f.stateLocked()
	return &st, nil
}
func (f *fakeClient) States(ctx context.Context, ids ...string) ([]gopentair.DeviceState, error) {
	st, err := f.State(ctx, ids[0])
	if err != nil {
		return nil, err
	}
	return []gopentair.DeviceState{*st}, nil
}
func (f *fakeClient) SetFields(context.Context, string, map[string]string, ...gopentair.ApplyOption) (*gopentair.ApplyResult, error) {
	return nil, errors.New("raw writes are not allowed")
}

// Apply records the plan and, when confirm is set, applies the fields and
// a plausible device reaction, then evaluates the plan's Confirm.
func (f *fakeClient) Apply(_ context.Context, p gopentair.WritePlan, _ ...gopentair.ApplyOption) (*gopentair.ApplyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, p)
	res := &gopentair.ApplyResult{Plan: p, Applied: len(p.Steps)}
	if f.confirm {
		for _, s := range p.Steps {
			for k, v := range s.Fields {
				f.fields[k] = v
				if strings.HasSuffix(k, "e10") {
					var n int
					fmt.Sscanf(k, "zp%de10", &n)
					if v == "3" {
						f.fields["s14"], f.fields["s18"] = fmt.Sprint(n-1), "600"
					} else {
						f.fields["s18"] = "0"
					}
				}
			}
		}
	}
	st := f.stateLocked()
	res.State = &st
	if p.Confirm != nil && !p.Confirm(st) {
		return res, &gopentair.NotConfirmedError{Timeout: p.ConfirmTimeout, Last: &st}
	}
	res.Confirmed = true
	return res, nil
}

func (f *fakeClient) Watch(ctx context.Context, ids []string, _ ...gopentair.WatchOption) (<-chan gopentair.Event, error) {
	f.mu.Lock()
	f.watches++
	if f.watchErr != nil {
		err := f.watchErr
		f.mu.Unlock()
		return nil, err
	}
	in := f.watchCh
	f.mu.Unlock()
	out := make(chan gopentair.Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
func (f *fakeClient) Pump(id string) *gopentair.Pump { return gopentair.NewPump(f, id) }
func (f *fakeClient) IntelliConnect(id string) *gopentair.IntelliConnect {
	return gopentair.NewIntelliConnect(f, id)
}
func (f *fakeClient) Light(id string) *gopentair.Light { return gopentair.NewLight(f, id) }

var _ gopentair.Client = (*fakeClient)(nil)

// --- helpers ---

func baseConf() registry.Config {
	return registry.Config{"username": "u", "password": "p", "name": "pool-pump", "robot_id": "home"}
}

func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

type sub struct {
	ch  chan map[string]any
	raw chan []byte
}

func subscribe(t *testing.T, nc *nats.Conn, subj string) sub {
	t.Helper()
	s := sub{ch: make(chan map[string]any, 64), raw: make(chan []byte, 64)}
	if _, err := nc.Subscribe(subj, func(m *nats.Msg) {
		var v map[string]any
		if err := json.Unmarshal(m.Data, &v); err != nil {
			t.Errorf("bad json on %s: %v", m.Subject, err)
		}
		s.raw <- m.Data
		s.ch <- v
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return s
}

func recv(t *testing.T, s sub) map[string]any {
	t.Helper()
	select {
	case v := <-s.ch:
		<-s.raw
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func noMsg(t *testing.T, s sub) {
	t.Helper()
	select {
	case v := <-s.ch:
		t.Fatalf("unexpected message: %v", v)
	case <-time.After(150 * time.Millisecond):
	}
}

// clock is a settable time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newTest builds a component against the fake without starting its loops.
func newTest(t *testing.T, fc *fakeClient, nc *nats.Conn, mod ...func(registry.Config)) (*Pump, *clock) {
	t.Helper()
	conf := baseConf()
	for _, m := range mod {
		m(conf)
	}
	cfg, err := parseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	dev, err := resolveDevice(context.Background(), fc, cfg.Device)
	if err != nil {
		t.Fatal(err)
	}
	c := &clock{t: time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)}
	return newPump(conf, cfg, fc, dev, nc, c.now), c
}

// --- tests ---

func TestParseConfig(t *testing.T) {
	cases := []struct {
		name    string
		mod     func(registry.Config)
		wantErr bool
	}{
		{"ok", func(registry.Config) {}, false},
		{"no username", func(c registry.Config) { delete(c, "username") }, true},
		{"no password (cached session)", func(c registry.Config) { delete(c, "password") }, false},
		{"bad interval", func(c registry.Config) { c["poll_interval"] = "soon" }, true},
		{"short interval", func(c registry.Config) { c["poll_interval"] = "30s" }, true},
		{"long interval", func(c registry.Config) { c["poll_interval"] = "5m" }, false},
		{"zero publish interval", func(c registry.Config) { c["min_publish_interval"] = "0s" }, false},
		{"bad publish interval", func(c registry.Config) { c["min_publish_interval"] = "x" }, true},
		{"bad region", func(c registry.Config) { c["region"] = "mars" }, true},
		{"us region", func(c registry.Config) { c["region"] = "us" }, false},
		{"watch off", func(c registry.Config) { c["watch"] = false }, false},
		{"watch string", func(c registry.Config) { c["watch"] = "false" }, false},
		{"watch junk", func(c registry.Config) { c["watch"] = 3 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConf()
			tc.mod(c)
			_, err := parseConfig(c)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	cfg, _ := parseConfig(baseConf())
	if cfg.PollInterval != 60*time.Second || cfg.MinPublishInterval != 5*time.Second || !cfg.Watch {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestDeviceRefStable(t *testing.T) {
	a, b := DeviceRef(pumpID), DeviceRef(pumpID)
	if a != b || len(a) != 12 {
		t.Fatalf("DeviceRef = %q / %q", a, b)
	}
	if DeviceRef("other") == a {
		t.Fatal("different IDs gave the same ref")
	}
	if strings.Contains(a, "SECRET") {
		t.Fatal("ref leaks the ID")
	}
}

func TestResolveDevice(t *testing.T) {
	ctx := context.Background()
	fc := newFake()
	for _, ref := range []string{"", pumpID, "pool pump", DeviceRef(pumpID), strings.ToUpper(DeviceRef(pumpID))} {
		d, err := resolveDevice(ctx, fc, ref)
		if err != nil || d.ID != pumpID {
			t.Fatalf("ref %q: dev=%v err=%v", ref, d.ID, err)
		}
	}
	if _, err := resolveDevice(ctx, fc, "light-1"); !errors.Is(err, gopentair.ErrUnsupportedDevice) {
		t.Fatalf("light: err = %v", err)
	}
	if _, err := resolveDevice(ctx, fc, "nope"); !errors.Is(err, gopentair.ErrDeviceNotFound) {
		t.Fatalf("nope: err = %v", err)
	}
	fc.devices = append(fc.devices, gopentair.Device{ID: "pump-2-SECRET", Type: gopentair.TypeIntelliFlo3, Nickname: "Spa"})
	_, err := resolveDevice(ctx, fc, "")
	if !errors.Is(err, gopentair.ErrAmbiguousDevice) {
		t.Fatalf("two pumps: err = %v", err)
	}
	if d, err := resolveDevice(ctx, fc, DeviceRef("pump-2-SECRET")); err != nil || d.Nickname != "Spa" {
		t.Fatalf("by ref: %v %v", d, err)
	}
	fc.loginErr = gopentair.ErrInvalidCredentials
	if _, err := resolveDevice(ctx, fc, ""); !errors.Is(err, gopentair.ErrInvalidCredentials) {
		t.Fatalf("login: err = %v", err)
	}
}

func TestPollPublishesReadings(t *testing.T) {
	nc := startNATS(t)
	data := subscribe(t, nc, "gorai.home.pool-pump.data")
	fc := newFake()
	fc.running(2, "850")
	fc.set(func(f *fakeClient) { f.fields["s19"] = "455"; f.fields["s26"] = "382" })
	p, _ := newTest(t, fc, nc)

	p.poll(context.Background())
	r := recv(t, data)

	for k, want := range map[string]any{
		"device_ref": DeviceRef(pumpID), "nickname": "Pool Pump", "online": true, "source": "poll",
		"running": true, "power_w": 850.0, "speed_pct": 45.5, "flow_gpm": 38.2, "enabled": true,
		"alarm": false, "alarm_bits": 0.0, "rssi": -55.0,
		"active_program": 2.0, "active_program_name": "Filter",
	} {
		if r[k] != want {
			t.Errorf("%s = %v, want %v", k, r[k], want)
		}
	}
	for _, k := range []string{"pressure_psi", "remaining_raw", "device_id", "deviceId"} {
		if _, ok := r[k]; ok {
			t.Errorf("%s must not be published", k)
		}
	}
	progs, _ := r["programs"].([]any)
	if len(progs) != 2 {
		t.Fatalf("programs = %v", r["programs"])
	}
	f := progs[1].(map[string]any)
	if f["name"] != "Filter" || f["type"] != "schedule" || f["running"] != true || f["value"] != 1800.0 {
		t.Errorf("program 2 = %v", f)
	}
	for _, k := range []string{"startRaw", "durationRaw", "daysRaw", "start", "duration", "days"} {
		if _, ok := f[k]; ok {
			t.Errorf("program field %s must not be published", k)
		}
	}

	got, err := p.Readings(context.Background())
	if err != nil || got["power_w"] != int64(850) || got["data_age_s"] != int64(0) {
		t.Fatalf("Readings = %v, %v", got, err)
	}
}

func TestMissingFieldsOmitted(t *testing.T) {
	fc := newFake()
	fc.set(func(f *fakeClient) {
		for _, k := range []string{"s18", "s19", "s26", "s20", "s13", "s25"} {
			delete(f.fields, k)
		}
	})
	p, _ := newTest(t, fc, nil)
	p.poll(context.Background())
	r, _ := p.Readings(context.Background())
	for _, k := range []string{"power_w", "running", "speed_pct", "flow_gpm", "alarm", "alarm_bits", "rssi", "enabled"} {
		if v, ok := r[k]; ok {
			t.Errorf("%s = %v, want omitted", k, v)
		}
	}
}

func TestReadingsBeforePoll(t *testing.T) {
	fc := newFake()
	p, _ := newTest(t, fc, nil)
	if _, err := p.Readings(context.Background()); err == nil {
		t.Fatal("want error before first poll")
	}
	fc.stateErr = errors.New("boom")
	p.poll(context.Background())
	if _, err := p.Readings(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceEvents(t *testing.T) {
	nc := startNATS(t)
	ev := subscribe(t, nc, "gorai.home.pool-pump.event")
	fc := newFake()
	p, _ := newTest(t, fc, nc)
	ctx := context.Background()

	p.poll(ctx) // first poll: up, no event
	noMsg(t, ev)

	fc.set(func(f *fakeClient) { f.online = false })
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventSourceDown || e["reason"] != ReasonDeviceOffline || e["device_ref"] != DeviceRef(pumpID) {
		t.Fatalf("offline event = %v", e)
	}
	fc.set(func(f *fakeClient) { f.online = true })
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventSourceUp {
		t.Fatalf("up event = %v", e)
	}

	fc.set(func(f *fakeClient) {
		f.stateErr = &gopentair.APIError{Status: 503, Method: "POST", Path: "/device/states"}
	})
	p.poll(ctx)
	p.poll(ctx)
	noMsg(t, ev) // below threshold
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventSourceDown || e["reason"] != ReasonAPIError {
		t.Fatalf("api error event = %v", e)
	}
	fc.set(func(f *fakeClient) { f.stateErr = nil })
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventSourceUp {
		t.Fatalf("recovery event = %v", e)
	}
	if st, _ := p.DoCommand(ctx, map[string]any{"command": "get_state"}); st["up"] != true || st["consecutive_errors"] != 0 {
		t.Fatalf("get_state = %v", st)
	}
}

func TestReauthStopsLoops(t *testing.T) {
	nc := startNATS(t)
	ev := subscribe(t, nc, "gorai.home.pool-pump.event")
	fc := newFake()
	p, _ := newTest(t, fc, nc)
	fc.stateErr = fmt.Errorf("refresh: %w", gopentair.ErrReauthRequired)

	p.start(context.Background())
	if e := recv(t, ev); e["kind"] != EventSourceDown || e["reason"] != ReasonReauthRequired {
		t.Fatalf("event = %v", e)
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loops still running after auth failure")
	}
	noMsg(t, ev) // published once
	st, _ := p.DoCommand(context.Background(), map[string]any{"command": "get_state"})
	if st["up"] != false || st["reason"] != ReasonReauthRequired {
		t.Fatalf("get_state = %v", st)
	}
	_ = p.Close(context.Background())
}

func TestTransitionEvents(t *testing.T) {
	nc := startNATS(t)
	ev := subscribe(t, nc, "gorai.home.pool-pump.event")
	fc := newFake()
	p, _ := newTest(t, fc, nc)
	ctx := context.Background()

	p.poll(ctx) // baseline: stopped, no alarm
	noMsg(t, ev)

	fc.running(2, "700")
	p.poll(ctx)
	e := recv(t, ev)
	if e["kind"] != EventRunChange || e["from"] != "stopped" || e["to"] != "running" || e["program"] != 2.0 || e["program_name"] != "Filter" {
		t.Fatalf("start = %v", e)
	}
	noMsg(t, ev) // no separate program_change for a start

	fc.running(1, "900")
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventProgramChange || e["from"] != 2.0 || e["to"] != 1.0 || e["program_name"] != "Quick Clean" {
		t.Fatalf("switch = %v", e)
	}

	fc.set(func(f *fakeClient) { f.fields["s20"] = "4" })
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventAlarm || e["raised"] != true || e["alarm_bits"] != 4.0 {
		t.Fatalf("alarm = %v", e)
	}
	p.poll(ctx)
	noMsg(t, ev) // unchanged alarm: no repeat
	fc.set(func(f *fakeClient) { f.fields["s20"] = "0" })
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventAlarm || e["raised"] != false {
		t.Fatalf("alarm clear = %v", e)
	}

	fc.running(1, "0")
	p.poll(ctx)
	if e := recv(t, ev); e["kind"] != EventRunChange || e["to"] != "stopped" {
		t.Fatalf("stop = %v", e)
	}
	noMsg(t, ev)
}

func TestAlarmAtStartup(t *testing.T) {
	nc := startNATS(t)
	ev := subscribe(t, nc, "gorai.home.pool-pump.event")
	fc := newFake()
	fc.fields["s20"] = "2"
	p, _ := newTest(t, fc, nc)
	p.poll(context.Background())
	if e := recv(t, ev); e["kind"] != EventAlarm || e["raised"] != true {
		t.Fatalf("startup alarm = %v", e)
	}
}

func TestWatchCoalescesAndDedupes(t *testing.T) {
	nc := startNATS(t)
	data := subscribe(t, nc, "gorai.home.pool-pump.data")
	fc := newFake()
	p, clk := newTest(t, fc, nc, func(c registry.Config) { c["min_publish_interval"] = "200ms" })

	p.poll(context.Background()) // heartbeat publish at t0
	recv(t, data)

	// Same values pushed: deduplicated.
	p.ingest(fc.snapshot(), "ws", false)
	noMsg(t, data)

	// Changed inside the window: held back, then flushed with the latest.
	fc.running(2, "500")
	p.ingest(fc.snapshot(), "ws", false)
	fc.running(2, "650")
	p.ingest(fc.snapshot(), "ws", false)
	select {
	case v := <-data.ch:
		t.Fatalf("published inside window: %v", v)
	case <-time.After(50 * time.Millisecond):
	}
	r := recv(t, data)
	if r["power_w"] != 650.0 || r["source"] != "ws" {
		t.Fatalf("flushed = %v", r)
	}
	noMsg(t, data)

	// After the window: immediate.
	clk.add(time.Second)
	fc.running(2, "700")
	p.ingest(fc.snapshot(), "ws", false)
	select {
	case r := <-data.ch:
		if r["power_w"] != 700.0 {
			t.Fatalf("immediate = %v", r)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("not published immediately after window")
	}

	// Poll always publishes, even when unchanged (heartbeat).
	p.poll(context.Background())
	recv(t, data)
	p.poll(context.Background())
	recv(t, data)
	p.stop()
}

func TestWatchLoopIngestsPushes(t *testing.T) {
	nc := startNATS(t)
	data := subscribe(t, nc, "gorai.home.pool-pump.data")
	fc := newFake()
	fc.watchCh = make(chan gopentair.Event, 4)
	p, clk := newTest(t, fc, nc, func(c registry.Config) { c["min_publish_interval"] = "0s" })

	p.start(context.Background())
	defer p.Close(context.Background())
	if r := recv(t, data); r["source"] != "poll" {
		t.Fatalf("first = %v", r)
	}
	clk.add(time.Second)
	fc.running(2, "720")
	fc.watchCh <- gopentair.Event{DeviceID: pumpID, State: fc.snapshot(), Source: gopentair.SourceWS}
	fc.watchCh <- gopentair.Event{DeviceID: "someone-else", State: fc.snapshot(), Source: gopentair.SourceWS}
	r := recv(t, data)
	if r["source"] != "ws" || r["power_w"] != 720.0 {
		t.Fatalf("pushed = %v", r)
	}
	noMsg(t, data)
	if st, _ := p.DoCommand(context.Background(), map[string]any{"command": "get_state"}); st["watch_source"] != "ws" {
		t.Fatalf("get_state = %v", st)
	}
}

func TestWatchRetriesAndStopsOnAuth(t *testing.T) {
	fc := newFake()
	fc.watchErr = errors.New("dial failed")
	p, _ := newTest(t, fc, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.watchLoop(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel() // stuck in backoff: cancellation must end it
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchLoop ignored cancel during backoff")
	}

	fc.set(func(f *fakeClient) { f.watchErr = gopentair.ErrReauthRequired })
	done = make(chan struct{})
	go func() { p.watchLoop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchLoop kept retrying after auth failure")
	}
}

func TestDoCommandPrograms(t *testing.T) {
	nc := startNATS(t)
	ev := subscribe(t, nc, "gorai.home.pool-pump.event")
	fc := newFake()
	p, _ := newTest(t, fc, nc)
	ctx := context.Background()
	p.poll(ctx)

	// Dry run sends nothing.
	r, err := p.DoCommand(ctx, map[string]any{"command": "start_program", "program": "filter", "dry_run": true})
	if err != nil || r["dry_run"] != true || !strings.Contains(r["plan"].(string), "zp2e10=3") {
		t.Fatalf("dry run = %v, %v", r, err)
	}
	if len(fc.applied) != 0 {
		t.Fatal("dry run applied a plan")
	}
	if strings.Contains(fmt.Sprint(r), pumpID) {
		t.Fatal("dry run reply leaks the device ID")
	}

	r, err = p.DoCommand(ctx, map[string]any{"command": "start_program", "program": 2.0})
	if err != nil || r["confirmed"] != true || r["applied"] != 2 {
		t.Fatalf("start = %v, %v", r, err)
	}
	if e := recv(t, ev); e["kind"] != EventRunChange || e["to"] != "running" {
		t.Fatalf("start event = %v", e) // refresh after apply publishes the change
	}

	r, err = p.DoCommand(ctx, map[string]any{"command": "stop"})
	if err != nil || r["confirmed"] != true {
		t.Fatalf("stop = %v, %v", r, err)
	}
	if got := fc.applied[len(fc.applied)-1].Steps[0].Fields; got["zp2e10"] != "2" {
		t.Fatalf("stop wrote %v", got)
	}
	r, err = p.DoCommand(ctx, map[string]any{"command": "stop"})
	if err != nil || r["applied"] != 0 {
		t.Fatalf("stop when idle = %v, %v", r, err)
	}

	fc.confirm = false
	r, err = p.DoCommand(ctx, map[string]any{"command": "start_program", "program": "Quick Clean"})
	if err != nil || r["status"] != "not_confirmed" || r["confirmed"] != false {
		t.Fatalf("unconfirmed = %v, %v", r, err)
	}

	for _, bad := range []map[string]any{
		{"command": "start_program"},
		{"command": "start_program", "program": 2.5},
		{"command": "start_program", "program": 9.0},
		{"command": "stop_program", "program": "nope"},
		{"command": "explode"},
	} {
		if _, err := p.DoCommand(ctx, bad); err == nil {
			t.Errorf("%v: want error", bad)
		}
	}

	lp, err := p.DoCommand(ctx, map[string]any{"command": "list_programs"})
	if err != nil || len(lp["programs"].([]map[string]any)) != 2 {
		t.Fatalf("list_programs = %v, %v", lp, err)
	}
	c, _ := p.DoCommand(ctx, map[string]any{"command": "get_config"})
	if c["device_ref"] != DeviceRef(pumpID) || c["region"] != "us" || c["poll_interval"] != "1m0s" {
		t.Fatalf("get_config = %v", c)
	}
	if rr, _ := p.DoCommand(ctx, map[string]any{"command": "refresh"}); rr["status"] != "ok" {
		t.Fatalf("refresh = %v", rr)
	}
}

func TestApplyErrorPropagates(t *testing.T) {
	fc := newFake()
	p, _ := newTest(t, fc, nil)
	p.poll(context.Background())
	p.mu.Lock()
	p.client = &rateLimited{fc}
	p.mu.Unlock()
	_, err := p.DoCommand(context.Background(), map[string]any{"command": "start_program", "program": 1.0})
	if !errors.Is(err, gopentair.ErrRateLimited) {
		t.Fatalf("err = %v", err)
	}
}

type rateLimited struct{ *fakeClient }

func (r *rateLimited) Apply(context.Context, gopentair.WritePlan, ...gopentair.ApplyOption) (*gopentair.ApplyResult, error) {
	return &gopentair.ApplyResult{}, &gopentair.RateLimitError{Key: gopentair.DebounceProgramStart, Wait: 20 * time.Second}
}

// TestNoDeviceIDLeaks drives every publish path and the logs, and checks
// the raw device ID never appears.
func TestNoDeviceIDLeaks(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	nc := startNATS(t)
	all := subscribe(t, nc, "gorai.>")
	fc := newFake()
	p, _ := newTest(t, fc, nc, func(c registry.Config) { c["min_publish_interval"] = "0s" })
	ctx := context.Background()

	p.poll(ctx)
	fc.running(2, "500")
	fc.fields["s20"] = "1"
	p.poll(ctx)
	fc.set(func(f *fakeClient) { f.online = false })
	p.poll(ctx)
	fc.set(func(f *fakeClient) { f.online = true })
	p.ingest(fc.snapshot(), "ws", false)
	_, _ = p.DoCommand(ctx, map[string]any{"command": "stop"})
	r, _ := p.Readings(ctx)
	cfg, _ := p.DoCommand(ctx, map[string]any{"command": "get_config"})

	var got []string
	for {
		select {
		case b := <-all.raw:
			got = append(got, string(b))
			continue
		case <-time.After(200 * time.Millisecond):
		}
		break
	}
	if len(got) < 6 {
		t.Fatalf("only %d messages", len(got))
	}
	got = append(got, logs.String(), fmt.Sprint(r), fmt.Sprint(cfg))
	for _, s := range got {
		if strings.Contains(s, pumpID) || strings.Contains(s, "SECRET") {
			t.Fatalf("device ID leaked: %s", s)
		}
	}
}

func TestReconfigure(t *testing.T) {
	fc := newFake()
	fc.devices = append(fc.devices, gopentair.Device{ID: "pump-2", Type: gopentair.TypeIntelliFlo3, Nickname: "Spa"})
	old := clientFactory
	clientFactory = func(*Config, *slog.Logger) (gopentair.Client, error) { return fc, nil }
	defer func() { clientFactory = old }()

	p, _ := newTest(t, fc, nil, func(c registry.Config) { c["device"] = "Pool Pump"; c["watch"] = false })
	p.poll(context.Background())

	attrs := map[string]any(baseConf())
	attrs["device"] = "Spa"
	attrs["watch"] = false
	if err := p.Reconfigure(context.Background(), nil, resource.Config{Attributes: attrs}); err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	if p.DeviceRef() != DeviceRef("pump-2") {
		t.Fatalf("ref = %s", p.DeviceRef())
	}
	attrs["poll_interval"] = "1s"
	if err := p.Reconfigure(context.Background(), nil, resource.Config{Attributes: attrs}); err == nil {
		t.Fatal("want invalid config error")
	}
}

func TestNewAndCloseKeepSession(t *testing.T) {
	fc := newFake()
	old := clientFactory
	clientFactory = func(*Config, *slog.Logger) (gopentair.Client, error) { return fc, nil }
	defer func() { clientFactory = old }()

	nc := startNATS(t)
	data := subscribe(t, nc, "gorai.home.pool-pump.data")
	comp, err := New(context.Background(), natsDeps{nc}, baseConf())
	if err != nil {
		t.Fatal(err)
	}
	p := comp.(*Pump)
	recv(t, data)
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fc.logouts != 0 {
		t.Fatal("Close must not log out (it would delete the cached session)")
	}
	if _, err := New(context.Background(), nil, registry.Config{"password": "p"}); err == nil {
		t.Fatal("want config error")
	}
}

type natsDeps struct{ nc *nats.Conn }

func (d natsDeps) Get(name string) (any, error) {
	if name == "nats" {
		return d.nc, nil
	}
	return nil, errors.New("missing")
}
func (d natsDeps) GetByType(string) ([]any, error) { return nil, errors.New("missing") }
