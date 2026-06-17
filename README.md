# direlera-rs

**direlera-rs** is a Rust-based server that uses the Kaillera protocol to facilitate online multiplayer for emulators.

> ⚠️ **Experimental Project**: This is an early-stage experimental project. Stability and user experience have not been thoroughly tested or optimized yet. Use at your own risk.

## What is Kaillera?

Kaillera is a network protocol that enables online multiplayer gaming in emulators. Developed in the late 1990s, it has been widely used in various emulators such as MAME, Project64, and Snes9x. Through Kaillera, users can play retro games together in real-time over the internet.

## Why This Project?

Direlera-rs is an experimental attempt to reimplement the Kaillera server protocol using modern tools:

- **Learning**: Exploring Rust's async I/O and network programming capabilities
- **Protocol Analysis**: Better understanding of the Kaillera protocol through implementation
- **Transparency**: Providing Wireshark dissector for protocol analysis and debugging
- **Modernization**: Experimenting with a Rust-based implementation of the legacy protocol

## Current Features

- Kaillera 0.83 protocol implementation (basic)
- Multi-room game hosting
- Global chat and in-game chat
- Ping calculation
- TOML configuration file
- Wireshark protocol dissector (Lua)
- EUC-KR encoding support

## Why Run direlera-rs?

direlera-rs is built for small, low-overhead Kaillera hosting with a modern,
maintainable Rust codebase. In local `kaillera-tester observe` runs, it kept
game-output cadence near 60 fps while using much less memory than EmuLinker-K:

| server | 4 concurrent games | peak RSS | avg CPU | game cadence |
| --- | ---: | ---: | ---: | ---: |
| direlera-rs | 8/8 completed | 15.02 MiB | 4.25% | ~60 fps |
| EmuLinker-K | 8/8 completed | 247.56 MiB | 13.65% | ~60 fps |
| original kaillerasrv 0.86 | 8/8 completed | 2.02 MiB | 3.06% | ~60 fps |

The original 2002 `kaillerasrv` is still extremely small, but it is a legacy
32-bit binary. See [PERFORMANCE.md](PERFORMANCE.md) for the measurement method
and an important note about using `cadence_fps` for FPS comparisons.

## Getting Started

### Option 1: Simple Linux Install

This path does not require Docker. It is meant for a basic Ubuntu or Debian VPS.
Copy and paste the commands in order.

```bash
sudo apt update
sudo apt install -y curl git build-essential

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

git clone https://github.com/caromdreamer/direlera-rs.git
cd direlera-rs
cp config.toml.example config.toml
./start.sh --build
```

The server keeps running in the background. Logs are written to `direlera.log`.

Stop the server:

```bash
./stop.sh
```

Update later:

```bash
cd direlera-rs
./stop.sh
git pull
./start.sh --build
```

### Option 2: Build from Source Manually

Use this path when you are not on Ubuntu/Debian or want each step separated.

#### Step 1: Install system dependencies

**Ubuntu / Debian:**

```bash
sudo apt update
sudo apt install -y curl git build-essential
```

**Fedora / RHEL / CentOS:**

```bash
sudo dnf install -y curl git gcc
```

**Arch Linux:**

```bash
sudo pacman -S --needed curl git base-devel
```

#### Step 2: Install Rust

Rust is installed through a tool called `rustup`. Run the following command, press **Enter** when prompted, and let the installer finish:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After the installer completes, reload your shell environment so that the `cargo` command becomes available:

```bash
source "$HOME/.cargo/env"
```

Verify the installation:

```bash
cargo --version   # should print something like: cargo 1.XX.X
```

#### Step 3: Clone, configure, and build

```bash
git clone https://github.com/caromdreamer/direlera-rs.git
cd direlera-rs
cp config.toml.example config.toml
cargo build --release
```

The build may take a few minutes on the first run. When it finishes, the binary is at `target/release/direlera-rs`.

#### Step 4: Run the server

```bash
./start.sh
```

Stop it later:

```bash
./stop.sh
```

### Option 3: Docker

Docker is optional. Skip this section unless you already know you want to use
Docker.

Run the published image with Docker Compose:

```bash
git clone https://github.com/caromdreamer/direlera-rs.git
cd direlera-rs
cp config.toml.example config.toml
docker compose up -d
docker compose logs -f direlera
```

Build a local image instead:

```bash
docker build -t direlera-rs:local .
docker run --rm \
  -p 8080:8080/udp \
  -p 27888:27888/udp \
  -p 9091:9091 \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  direlera-rs:local
```

## Configuration

The server reads `config.toml` from the current working directory. Start from
`config.toml.example` and edit only the settings you need.

Most server owners should review these first:

| Config key | Default | Meaning |
| --- | --- | --- |
| `main_port` | `8080` | Main Kaillera UDP port used after the initial handshake. |
| `control_port` | `27888` | Kaillera discovery, ping, and initial connection UDP port. |
| `welcome_message` | example text | Message shown to clients when they connect. |
| `disable_output_cache` | `false` | Send full `GAME_DATA` instead of cache references for maximum client compatibility. |
| `metrics_enabled` | `false` | Expose Prometheus metrics on `metrics_port`. |
| `master_list.enabled` | `false` | Announce the server to configured public master lists. |
| `master_list.server_name` | example text | Server name shown on master lists. |
| `master_list.server_location` | `US` | Short location code shown on master lists. |
| `tracing.level` | `info` | Log level. Use `debug` or `trace` only while troubleshooting. |

Environment variables can be used in `config.toml`:

```toml
main_port = ${DIRELERA_MAIN_PORT}
server_id = "${DIRELERA_SERVER_ID}"
```

### LAN-only connection policy

direlera-rs currently accepts only Kaillera connection type `1` (`LAN`). Clients
that try to log in with `2` through `6` are rejected before they enter the
lobby. This keeps the current server behavior simple and predictable while the
sync model is being hardened.

## Ports

The server listens on the following ports by default:

- `27888/udp`: control/discovery/ping port. Most Kaillera clients start here.
- `8080/udp`: main game/lobby protocol port returned by the control handshake.
- `9091/tcp`: optional Prometheus metrics endpoint when `metrics_enabled = true`.

For a public server, open at least UDP `27888` and UDP `8080` on your firewall or
cloud security group.

## Wireshark Dissector Setup

The included Wireshark dissector allows you to analyze Kaillera protocol packets.

### Installation Steps

1. **Find Wireshark Plugin Directory**

   In Wireshark: `Help → About Wireshark → Folders → Personal Lua Plugins`

   Common paths:

   - **Windows**: `%APPDATA%\Wireshark\plugins\`
   - **Linux**: `~/.local/lib/wireshark/plugins/`
   - **macOS**: `~/.wireshark/plugins/` or `/Applications/Wireshark.app/Contents/PlugIns/wireshark/`

2. **Copy the Dissector**

   ```bash
   # Windows (PowerShell)
   Copy-Item kaillera.lua "$env:APPDATA\Wireshark\plugins\"

   # Linux/macOS
   cp kaillera.lua ~/.local/lib/wireshark/plugins/
   ```

3. **Restart Wireshark**

   After restarting Wireshark, the Kaillera protocol will be automatically recognized.

4. **Usage**

   - Start capturing on UDP ports 27888 and 8080
   - Use filter: `kaillera` to display only Kaillera packets

## How It Works

For a detailed explanation of the Kaillera game synchronization protocol, including:

- Game Data (0x12) and Game Cache (0x13) packet behavior
- Per-player caching mechanisms
- Frame synchronization with mixed connection types
- Frame interleaving algorithm
- Preemptive padding for multi-delay synchronization

See **[GAME_SYNC_PROTOCOL.md](GAME_SYNC_PROTOCOL.md)** - This document describes the actual protocol behavior discovered through reverse engineering and packet analysis with Wireshark.

## Feedback

This project is still experimental. Please report bugs, compatibility problems,
or server operation issues on [GitHub Issues](https://github.com/caromdreamer/direlera-rs/issues).

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.

## References

- [Kaillera Official Website](http://www.kaillera.com/)
- [EmuLinker-K](https://github.com/hopskipnfall/EmuLinker-K) - Similar Kotlin implementation
- [Protocol Documentation](protocol.txt) - Detailed Kaillera protocol documentation
