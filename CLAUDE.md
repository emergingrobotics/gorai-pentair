# gorai-pentair

Gorai satellite component: a Pentair IntelliFlo 3 (IF31) pool pump as a
`sensor/pentair-pump` component, built on the gopentair SDK (`../../gopentair`, module
path `github.com/emergingrobotics/gopentair`, pulled in by a `replace` in go.mod). Same
pattern as `../gorai-aquatemp`. See VISION.md for the design and README.md for
subjects, payload and commands.

## Layout

- `pump/pump.go`: component (registry `sensor`/`pentair-pump`), poll loop + Watch loop, ingest/coalesce, NATS publish, events, DoCommand
- `pump/pump_test.go`: fake `gopentair.Client` (gopentair's fake cloud is internal) + embedded nats-server
- `cmd/gorai-pentair`: standalone publisher to a NATS server (mesh registration optional); `--do` runs one command
- `cmd/pentair-watch`: subscriber that prints `gorai.*.pool-pump.>`

## Rules

- Omit values the cloud did not report; never publish them as 0.
- Don't publish fields with unconfirmed scale/units (`s17` pressure, `s28`, program `e6`/`e7`/`e8`) until `gopentair/docs/api-docs.md` marks them verified.
- Never publish or log the raw device ID; use `device_ref` (`DeviceRef`). Don't echo a user-supplied device ref in errors either (it may be an ID). `TestNoDeviceIDLeaks` guards this.
- Device writes only through `gopentair.Pump` plans and `Apply`. No raw `SetFields`. Dry runs are handled in the component, never sent to `Apply`.
- Never call `Logout` from `Close` (it deletes the cached session); never `Logout(ctx, true)`.
- The REST poll is the heartbeat and the health signal (`Watch` doesn't surface errors). Minimum 60s.
- Auth failures (`ErrReauthRequired`, `ErrInvalidCredentials`, `ChallengeError`) stop the loops; don't retry them.
- Build/test: `make check`, `make test-race`. Host bus: `make nats-up`, `make run`, `make watch`.
