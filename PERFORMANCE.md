# Kaillera server performance

Latest runs: 2026-06-17 KST, local machine.

This benchmark answers two practical questions:

- How much server CPU and RSS are used as concurrent games increase?
- Does the lockstep game loop keep returning merged frames at roughly 60 fps?

Use `cadence_fps` for FPS comparisons. It is calculated from received frame
timestamps during the run. The raw observe JSON also contains `overall_fps` and
`throughput_fps`, but this document intentionally uses cadence only so the table
stays focused on game-output pace.

## Method

Run from the `kaillera-tester` repo:

```bash
OUT=/tmp/kaillera-perf-$(date +%Y%m%d-%H%M%S) ./scripts/perf_compare.sh
```

The script builds `cmd/observe`, starts a fresh server process for each
row/repetition, records process CPU/RSS with `pidstat`, and stores raw
`observe` JSON plus a generated `summary.md`.

Defaults used for these runs:

| item | value |
| --- | --- |
| concurrent games | `1 2 4` |
| repetitions | `2` |
| players per game | `3` |
| measured frames | `1000` |
| pace | `60 fps` |
| injected latency | p1 one-way `16.05 ms`, p2/p3 `0 ms` |
| timeout | `4000 ms` |
| direlera-rs | `./target/release/direlera-rs` |
| EmuLinker-K JVM | `-Xms64m -Xmx256m -XX:+UseSerialGC -XX:+AlwaysPreTouch` |
| EmuLinker-K warmup | one discarded same-concurrency 1000-frame run |
| EmuLinker-K address | `127.0.0.1:27999` |
| original kaillerasrv | `kaillerasrv-0.86/kaillerasrv`, port `28888` |

For the EmuLinker-K local comparison, the script copies EmuLinker-K's `conf`
directory into the run artifact and sets `server.maxUserNameLength=0` there,
because observe's generated usernames can be longer than EmuLinker-K's default
30-byte limit. The original EmuLinker-K config is not modified.

For original `kaillerasrv 0.86`, observe performs `HELLO` per client. That is
the expected legacy behavior because the original server returns a fresh
temporary game port for each connecting client.

## Results

Source artifacts:

- `kaillera-tester/perf-runs/20260617-224844-all-tailfix`

| server | concurrent games | avg CPU % | peak RSS MiB | completed games | min cadence FPS | max p95 ms | max p99 ms | max observed max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direlera-rs | 1 | 1.45 | 12.01 | 2/2 | 59.9 | 34 | 34 | 39 |
| direlera-rs | 2 | 2.65 | 13.86 | 4/4 | 59.9 | 34 | 35 | 45 |
| direlera-rs | 4 | 4.25 | 15.02 | 8/8 | 60.0 | 34 | 35 | 38 |
| EmuLinker-K | 1 | 7.90 | 222.00 | 2/2 | 59.9 | 34 | 34 | 37 |
| EmuLinker-K | 2 | 10.97 | 222.72 | 4/4 | 59.9 | 34 | 35 | 40 |
| EmuLinker-K | 4 | 13.65 | 247.56 | 8/8 | 59.9 | 51 | 51 | 57 |
| original kaillerasrv 0.86 | 1 | 0.58 | 2.05 | 2/2 | 60.0 | 34 | 34 | 38 |
| original kaillerasrv 0.86 | 2 | 1.19 | 2.02 | 4/4 | 59.9 | 34 | 34 | 37 |
| original kaillerasrv 0.86 | 4 | 3.06 | 2.02 | 8/8 | 59.9 | 34 | 35 | 42 |

## Interpretation

All three servers completed every measured game and kept received-frame cadence
near 60 fps. Earlier local runs showed lower direlera-rs `overall_fps` because
the tester waited for a final receive timeout after the target frames had
already been collected; the tester now cancels helper receive waits when all
players reach the target frame count.

The original `kaillerasrv 0.86` is the smallest by far: roughly `2 MiB` RSS in
this local run. That is useful as a historical baseline, but it is a stripped
32-bit Linux binary from 2002.

`direlera-rs` is heavier than the original server but still lightweight:
roughly `12-15 MiB` RSS and lower CPU than EmuLinker-K in this run. Its main
tradeoff is not raw footprint versus the original binary; it is maintainability,
modern Rust code, observability, and easier protocol hardening.

EmuLinker-K has a larger fixed JVM memory footprint, around `222-248 MiB` RSS
here. It remains a strong behavioral reference, especially for legacy protocol
compatibility and startup-delay behavior.

The RTT columns are lockstep merged-reply timings, not pure server handler
latency. For input-delay behavior, inspect each raw JSON file's
`players[].lag_histogram`, `players[].observed_warmup_frames`, and
`observed.advertised_delays`.

## Reading FPS Fields

- `cadence_fps`: best quick answer for "is the game loop returning frames near
  60 fps?"
- `overall_fps` / `throughput_fps`: completed frames divided by the target-frame
  receive span. These should now stay close to cadence in healthy completed
  runs.
- `total_elapsed_ms` and `tail_wait_ms`: debug fields for tester overhead after
  target frames have been collected.

Prefer `cadence_fps` in user-facing summaries.
