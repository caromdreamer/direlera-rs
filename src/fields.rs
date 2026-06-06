// Structured logging field names.
//
// Logging policy (keep field cardinality low and queries stable):
//   1. Session context  -> the long-lived `session` SPAN (session_manager.rs).
//      Identity that holds for the whole session is recorded onto that span at
//      the points where it becomes known or changes — login (user_name/user_id/
//      connection_type/session_id), ACK (ping), join/quit (game_id) — and every
//      child handler event inherits it. Handlers do NOT re-stamp these.
//   2. Queryable values  -> EVENT fields. Per-event values worth filtering,
//      aggregating, or alerting on (counts, types, status, error). Kept below.
//   3. Everything else   -> the message string via `format!`. One-off details
//      and identifiers of *other* entities (a kicked user, a previous session,
//      a byte length) are narrative, not dimensions — they belong in the text.

#![allow(dead_code)]

// --- Session context (session span) fields -------------------------------
// Recorded on the `session` span, not on individual events. The span
// declaration (session_manager.rs) and the recorders use these names as string
// literals; the constants document the schema. `player_id` is the exception —
// it is game-scoped and recorded on the game_data/game_cache handler spans.
pub const ADDR: &str = "addr";
pub const USER_NAME: &str = "user_name";
pub const USER_ID: &str = "user_id";
pub const CONNECTION_TYPE: &str = "connection_type";
pub const PING: &str = "ping";
pub const SESSION_ID: &str = "session_id";
pub const GAME_ID: &str = "game_id";
pub const PLAYER_ID: &str = "player_id";

// --- Event fields (queryable per-event values) ---------------------------
pub const PORT: &str = "port";
pub const PACKET_SIZE: &str = "packet_size";
pub const PLAYER_COUNT: &str = "player_count";
pub const GAME_NAME: &str = "game_name";
pub const GAME_STATUS: &str = "game_status";
pub const MESSAGE_TYPE: &str = "message_type";
pub const MESSAGE_NUMBER: &str = "message_number";
pub const ERROR: &str = "error";
pub const CONFIG_SOURCE: &str = "config_source";
