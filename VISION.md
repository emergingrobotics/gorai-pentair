# gorai-pentair: Vision

A gorai satellite component that puts a **Pentair IntelliFlo 3 (IF31) variable-speed
pool pump** on the gorai NATS mesh. It is the pump-only sibling of
[`gorai-aquatemp`](https://github.com/emergingrobotics/gorai-aquatemp): same shape, same subjects, same rules, built on
[`gopentair`](https://github.com/emergingrobotics/gopentair) instead of goaquatemp.

## Why

The pool's heat pump is already on the bus. The pump is the other half of the picture:
whether water is moving, how fast, and at what cost in watts. A heat pump reading with
no flow is meaningless, and gorai-aquatemp already guards against that. With the pump on
the bus, a robot, dashboard or rule can see both and act on them, for example "don't
call for heat unless the pump is running a program that moves at least N GPM".

## What it is

| | gorai-aquatemp | gorai-pentair |
|---|---|---|
| SDK | `github.com/emergingrobotics/goaquatemp` | `github.com/emergingrobotics/gopentair` |
| Registry | `sensor` / `aquatemp` | `sensor` / `pentair-pump` |
| Default name | `heat-pump` | `pool-pump` |
| Package | `heatpump/` | `pump/` |
| Binaries | `gorai-aquatemp`, `aquatemp-watch` | `gorai-pentair`, `pentair-watch` |
| Update source | REST poll every 2m (min 30s) | **WebSocket push** via `Client.Watch`, REST poll fallback (min 60s) |
| Device | `device_code`, auto-detect if only one | `device`: ID, alias or nickname via `ResolveDevice(ref, TypeIntelliFlo3)`, auto-detect if only one pump |
| Auth | username/password each start | username/password, then a **cached refresh token** (gopentair token store) so restarts skip SRP |
| Go | 1.25 | 1.26 (gopentair requires it) |

Everything else carries over unchanged: the component publishes only when the runtime
hands it a `nats` dependency, `Readings()` works either way, the standalone binary
registers with mesh discovery unless `--no-mesh`, and `make nats-up / run / watch`
gives the same host-bus workflow.

The one structural difference is push instead of poll. gopentair's `Watch` already
merges WebSocket deltas into a consistent snapshot, reconnects, rotates connections
before token expiry, and falls back to REST polling. The component consumes that
`<-chan Event` (with gopentair's own poll fallback off). Alongside it the component
runs one REST poll every `poll_interval` (default and minimum 60s). The poll is the
heartbeat: it always publishes, so consumers can tell "nothing changed" from "the
publisher is dead". It is also the health check that drives `source_down` /
`source_up`, because `Watch` swallows errors and never reports them.

## Subjects

### `gorai.<robot_id>.<name>.data`

Published on every poll (heartbeat) and on each pushed change. Pushed changes are
de-duplicated (nothing published changed → nothing sent) and coalesced to at most one
per `min_publish_interval`, default 5s, because power and speed jitter while ramping.

| Key | Unit | Source / notes |
|---|---|---|
| `ts` | RFC 3339 | Time of the snapshot (UTC) |
| `device_ref` | | Stable non-secret ID: first 12 hex chars of SHA-256 of the device ID |
| `nickname` | | `d27` |
| `online` | | Device state record |
| `running` | | `PumpStatus.Running()`: power > 0 |
| `power_w` | W | `s18` |
| `speed_pct` | % | `s19` ÷ 10 |
| `flow_gpm` | GPM | `s26` ÷ 10 |
| `pressure_psi` | psi | `s17`. **Held back until the scale is confirmed** (gopentair open question 6: ÷100 or ÷10) |
| `enabled` | | `s25` |
| `active_program`, `active_program_name` | | 1-based; omitted when nothing runs |
| `alarm`, `alarm_bits` | | `s20`; `alarm` is `alarm_bits != 0` |
| `rssi` | raw | `s13`; unit unconfirmed, published as reported |
| `programs` | | `[{number, name, type, value, running}]`; raw start/duration/days omitted until their units are known |
| `source` | | `snapshot`, `ws` or `poll`, from `Event.Source` |

Values the cloud did not report are left out, never sent as `0` (gopentair decodes
missing fields to `nil`; this component keeps that promise on the wire). `Readings()`
returns the same map plus `data_age_s`.

The raw device ID is **not** published. gopentair treats device IDs as sensitive and
never logs them, so the component follows suit. It identifies the pump by
`device_ref`, a short one-way hash that stays stable across restarts and tells pumps
apart on multi-pump accounts, plus `nickname` and the component name. `device_ref` is
also accepted wherever a device can be named (config `device`, `--device`).

### `gorai.<robot_id>.<name>.event`

```json
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"api_error"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"device_offline"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"reauth_required"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_up"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"run_change","from":"stopped","to":"running","program":2,"program_name":"Filter"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"program_change","from":2,"to":4}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"alarm","raised":true,"alarm_bits":4}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"alarm","raised":false,"alarm_bits":0}
```

- `api_error` fires after 3 consecutive failures, as in gorai-aquatemp.
- `reauth_required` is new: the refresh token is dead (`ErrReauthRequired` /
  `ErrInvalidCredentials`) and the component can't recover without a human. It is
  published once and the component stops hammering Cognito.
- `run_change` and `program_change` are the pump's analogue of the heat pump's
  `state_change`.

## DoCommand

| Command | Args | Notes |
|---|---|---|
| `refresh` | | Forces a REST `State` read |
| `get_config` | | `device_ref`, nickname, region, poll interval, publish interval, watch |
| `get_state` | | `{up, reason, consecutive_errors, watch_source}` |
| `list_programs` | | Same shape as `programs` in the payload |
| `start_program` | `program`: number or name; optional `dry_run` | `Pump.Start` → `Apply` |
| `stop_program` | `program`: number or name; optional `dry_run` | `Pump.Stop` → `Apply` |
| `stop` | | Stops whichever program is active; no-op if none |

All writes go through gopentair's `WritePlan` / `Apply`, so they inherit its
behavior: the 30s program-start debounce, confirmation against the observed state
(90s timeout), and **no retry after an ambiguous failure**. The reply always carries
`{applied, confirmed}`, and `dry_run` returns the plan text without sending anything.
As in gorai-aquatemp, the standalone binary does not expose commands on NATS.
Instead, `gorai-pentair --do '<json>'` runs one command and prints the reply.

## Rules

These go into CLAUDE.md when the code lands.

- Omit what the cloud did not report. Never publish a missing value as 0.
- Don't publish a field whose scale or units are unconfirmed (`s17`, `s28`, program
  `e6`/`e7`/`e8`). Promote it once it has been checked against a real pump and
  `gopentair/docs/api-docs.md` marks it verified.
- Device writes go only through `gopentair.Pump` plans. No raw `SetFields` path.
- Never put tokens, credentials, device IDs or email addresses in logs, payloads or
  errors. Pass gopentair a logger wrapped with its redaction.
- `Close` does **not** call `Logout`: gopentair's `Logout` deletes the cached session,
  which would force a fresh SRP login on every restart. Never `everywhere=true`, which
  signs the phone app out too.
- The REST poll fallback never runs faster than 60s.

## Testing

`gopentair/internal/testutil` (the fake cloud) is internal and can't be imported from
another module. That's fine: `gopentair.Client` is an interface and `NewPump(c, id)`
accepts any implementation. The tests use a hand-rolled fake `Client` whose `Watch`
returns a channel the test drives, plus an embedded nats-server, mirroring
`heatpump/aquatemp_test.go`. The cases to cover: missing fields are omitted, coalescing
and heartbeat timing (with an injected clock), each event transition, the
`api_error` threshold, `reauth_required`, `device_ref` stability, and a redaction
test asserting the raw device ID never reaches a published payload.

## Non-goals

- IntelliConnect (PIF0), lights (PLC1), heater, chlorinator. These could become
  sibling components later, not features of this one.
- Editing programs (schedules, speeds, durations): their units are unverified.
- Setting a raw speed or RPM outside a program.
- Local RS-485 / IntelliFlo serial protocol. Cloud only, like gopentair.
- Commands on NATS from the standalone binary (same stance as gorai-aquatemp).

## Milestones

Status as of 2026-11-26: milestones 1–3 and the code half of 4 are implemented and
tested against a fake client. What remains needs a real pump.

1. **Scaffold, read-only.** ✅ go.mod depending on gopentair (first via a local
   `replace`, now the published module),
   `./gorai-pentair` in `go.work`, Makefile, REST `State` poll and `Readings()`.
2. **Push.** ✅ `Watch`-driven publishing with de-duplication, coalescing and the poll
   heartbeat, plus the `source_*`, `run_change`, `program_change` and `alarm` events.
   Standalone binary, `pentair-watch` and mesh registration.
3. **Control.** ✅ `start_program` / `stop_program` / `stop` with `dry_run`; debounce and
   confirmation surfaced in the reply.
4. **Hardening.** ✅ persistent token store (shared with the `pentair` CLI by default),
   `reauth_required` handling, redaction test. ⏳ On a real pump: resolve the `s17`
   scale, confirm `s13` units, and run the week-long soak.

## Done means

- A week of `make run` against the real pump without a re-login, a dropped stream, a
  stale `.data` (heartbeat never misses), or a credential or device ID in the logs.
- `make watch` shows a program start from the Pentair phone app within seconds, as a
  `run_change` event followed by updated `.data`.
- `start_program` through DoCommand starts the pump and the reply reports
  `confirmed: true`.
- gorai-aquatemp and gorai-pentair on the same `robot_id` give a consumer everything it
  needs to decide whether the heat pump's water temperature can be trusted.

## Decisions

1. Registry model is `pentair-pump`; the package is `pump/`.
2. Payloads and events carry `device_ref`, a non-secret hash of the device ID.
3. Whether gopentair's WebSocket rotation holds up over a multi-day run is left to
   testing: the week-long soak in milestone 4 decides, and any fixes go back into
   gopentair.
