# gorai-pentair

A [gorai](https://github.com/emergingrobotics/gorai) satellite component that puts a
Pentair **IntelliFlo 3 (IF31)** variable-speed pool pump on the gorai NATS mesh. It
wraps [gopentair](https://github.com/emergingrobotics/gopentair), follows the pump
through the Pentair Home cloud (WebSocket push plus a REST poll), and publishes its
state as a standard `sensor/pentair-pump` component. It is the pump-only sibling of
[gorai-aquatemp](https://github.com/emergingrobotics/gorai-aquatemp). See [VISION.md](VISION.md) for the design.

## Install

```bash
# Embed the component in a robot binary
go get github.com/emergingrobotics/gorai-pentair@latest

# Standalone publisher, and a subscriber that pretty-prints what it publishes
go install github.com/emergingrobotics/gorai-pentair/cmd/gorai-pentair@latest
go install github.com/emergingrobotics/gorai-pentair/cmd/pentair-watch@latest
```

Releases are tagged `vX.Y.Z`; see the
[releases page](https://github.com/emergingrobotics/gorai-pentair/releases) for what each version pins.

## Quick start: publish to a NATS bus on this host

```bash
make nats-up                       # NATS 2.12 + JetStream in Docker on :4222 (monitor :8222)

export PENTAIR_USERNAME=you@example.com
export PENTAIR_PASSWORD=...        # optional: without it you're prompted once, then the cached session is used
make run                           # watches the pump, polls every 60s, publishes to gorai.home.pool-pump.*

make watch                         # in another shell: pretty-print what is published
make nats-down                     # when finished
```

Point at a different server with `NATS_URL=nats://host:4222 make run`, or run the
binary directly:

```bash
./bin/gorai-pentair --nats nats://localhost:4222 --robot-id home --name pool-pump
```

One-shot commands, without NATS:

```bash
./bin/gorai-pentair --do '{"command":"list_programs"}'
./bin/gorai-pentair --do '{"command":"start_program","program":"Filter","dry_run":true}'
./bin/gorai-pentair --do '{"command":"start_program","program":"Filter"}'
./bin/gorai-pentair --do '{"command":"stop"}'
```

| Flag | Default | Description |
|---|---|---|
| `--nats` | `$NATS_URL` or `nats://localhost:4222` | NATS server URL |
| `--robot-id` | `robot` | First segment of the subjects |
| `--name` | `pool-pump` | Component name |
| `--username` / `--password` | `$PENTAIR_USERNAME` / `$PENTAIR_PASSWORD` | Pentair Home account. The password is optional with a cached session, and prompted for on a terminal when needed |
| `--device` | `$PENTAIR_DEFAULT_DEVICE`, else auto-detect | `device_ref`, device ID, alias or nickname (required if the account has several pumps) |
| `--region` | `$PENTAIR_REGION`, else `us` | Pentair region |
| `--token-file` | `$PENTAIR_TOKEN_FILE`, else the `pentair` CLI's file | Session cache; `none` keeps it in memory |
| `--poll-interval` | `60s` | REST poll and heartbeat. Minimum `60s` |
| `--min-publish-interval` | `5s` | Minimum gap between pushed `.data` messages |
| `--no-watch` | off | Poll only; don't open the WebSocket |
| `--do` | | Run one DoCommand (JSON), print the reply, exit |
| `--no-mesh` | off | Skip mesh registration (NATS without JetStream) |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

The session cache is what lets a restart skip the SRP login. By default it is shared
with the `pentair` CLI, so `pentair login` once and both use it.

## Embedded in a robot binary

```go
import _ "github.com/emergingrobotics/gorai-pentair/pump"
```

```json
{
  "name": "pool-pump",
  "type": "sensor",
  "model": "pentair-pump",
  "attributes": {
    "username": "${PENTAIR_USERNAME}",
    "password": "${PENTAIR_PASSWORD}",
    "poll_interval": "60s"
  }
}
```

Attributes: `username` (required), `password` (optional when a cached session exists), `device`, `region`, `token_file`,
`poll_interval`, `min_publish_interval`, `watch` (default `true`), `robot_id`, `name`.
The component publishes only when the runtime gives it a `nats` dependency;
`Readings()` works either way.

## Subjects

### `gorai.<robot_id>.<name>.data`

Published on every poll (the heartbeat) and on pushed changes. Pushed changes are
de-duplicated and coalesced to at most one per `min_publish_interval`.

| Key | Unit | Notes |
|---|---|---|
| `ts` | RFC 3339 | Snapshot time (UTC) |
| `device_ref` | | Stable non-secret ID: first 12 hex chars of SHA-256 of the device ID |
| `nickname` | | |
| `online` | | |
| `running` | | Power > 0 |
| `power_w` | W | |
| `speed_pct` | % | |
| `flow_gpm` | GPM | |
| `enabled` | | |
| `active_program`, `active_program_name` | | 1-based; omitted when nothing runs |
| `alarm`, `alarm_bits` | | `alarm` is `alarm_bits != 0` |
| `rssi` | raw | Unit unconfirmed |
| `programs` | | `[{number, name, type, value, running}]` |
| `source` | | `poll`, `snapshot`, `ws` |

Fields the cloud does not report are left out, never sent as `0`. Pressure (`s17`),
remaining time (`s28`) and program start/duration/days are not published until their
scale and units are confirmed. The raw device ID is never published or logged.
`Readings()` returns the same map plus `data_age_s`.

### `gorai.<robot_id>.<name>.event`

```json
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"api_error"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"device_offline"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_down","reason":"reauth_required"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"source_up"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"run_change","from":"stopped","to":"running","program":2,"program_name":"Filter"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"program_change","from":2,"to":4,"program_name":"Spa"}
{"ts":"...","device_ref":"3f9a1c0b72de","kind":"alarm","raised":true,"alarm_bits":4}
```

- `api_error` fires after 3 consecutive failed polls.
- `reauth_required` means the saved login is dead. It is published once and the
  component stops polling and watching until it is reconfigured or restarted.
- `run_change` carries the program on a start; `program_change` covers a switch
  between programs while running (and a program number that arrives after the start).
- `alarm` fires when the alarm bits change, and at startup if an alarm is already set.

## DoCommand

| Command | Args |
|---|---|
| `refresh` | |
| `get_config` | |
| `get_state` | |
| `list_programs` | |
| `start_program` | `program`: number or name; optional `dry_run` |
| `stop_program` | `program`: number or name; optional `dry_run` |
| `stop` | optional `dry_run`; stops whatever program is running |

Writes go through gopentair's `WritePlan` / `Apply`: a 30s debounce between program
starts, confirmation against the device state (up to 90s), and no retry after an
ambiguous failure. Replies carry `applied` and `confirmed`; an unconfirmed write
returns `status: "not_confirmed"` rather than an error. Each write is followed by a
refresh. Commands are not exposed on NATS by the standalone binary.

## Local development

The SDK (`github.com/emergingrobotics/gopentair`) is an ordinary go.mod dependency, so this
repo builds standalone. To work on the SDK and this component together, use the
[gorai-all](https://github.com/emergingrobotics/gorai-all) workspace, whose `go.work`
pulls in the local checkouts:

```
gorai-all/
  go.work                  # uses ./gorai, ./gopentair, ./gorai-pentair, ...
  gorai/
  gopentair/               # github.com/emergingrobotics/gopentair
  gorai-pentair/           # this repo
```

```bash
make test-race   # unit tests: fake gopentair.Client + in-process NATS server
make check
```
