CREATE TABLE junction_state (
    junction_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE processed_events (
    junction_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at REAL NOT NULL,
    PRIMARY KEY (junction_id, event_id)
);

CREATE TABLE audit_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    junction_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    timestamp REAL NOT NULL,
    direction TEXT,
    previous_state TEXT,
    new_state TEXT,
    command_id TEXT,
    detail_json TEXT
);

CREATE INDEX audit_records_junction_id_id
    ON audit_records (junction_id, id DESC);

CREATE TABLE controller_commands (
    junction_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    signals_json TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (junction_id, command_id)
);
