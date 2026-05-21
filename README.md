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

## Getting Started

### Option 1: Download Pre-built Binary (Recommended — No Rust needed)

Download the latest binary directly from the [Releases page](https://github.com/yourusername/direlera-rs/releases) and run it:

```bash
# Download (replace vX.Y.Z with the latest version)
wget https://github.com/yourusername/direlera-rs/releases/download/vX.Y.Z/direlera-rs-linux-x86_64.tar.gz
tar -xzf direlera-rs-linux-x86_64.tar.gz
cd direlera-rs
./direlera-rs
```

---

### Option 2: Build from Source

If no pre-built binary is available for your platform, or you prefer to build yourself, follow the steps below.
You do **not** need any prior Rust knowledge — just copy and paste the commands.

#### Step 1 — Install system dependencies

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

#### Step 2 — Install the Rust toolchain

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

#### Step 3 — Clone and build

```bash
git clone https://github.com/yourusername/direlera-rs.git
cd direlera-rs
cargo build --release
```

The build may take a few minutes on the first run. When it finishes, the binary is at `target/release/direlera-rs`.

#### Step 4 — Run the server

```bash
./target/release/direlera-rs
```

---

### Default Ports

The server listens on the following ports by default:

- **Control Port**: 27888 UDP (initial connection and ping)
- **Game Port**: 8080 UDP (game logic)

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

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

Quick summary:

1. Check existing issues or create a new one
2. Create a feature branch from the `develop` branch
3. Commit your changes and submit a PR to the `develop` branch

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.

## References

- [Kaillera Official Website](http://www.kaillera.com/)
- [EmuLinker-K](https://github.com/sysfce2/EmuLinker-K) - Similar Kotlin implementation
- [Protocol Documentation](protocol.txt) - Detailed Kaillera protocol documentation

## Contact

Please report bugs or feature requests on [GitHub Issues](https://github.com/yourusername/direlera-rs/issues).
