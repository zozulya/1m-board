CREATE TABLE IF NOT EXISTS board (
    row INT NOT NULL,
    col INT NOT NULL,
    color TEXT NOT NULL,
    PRIMARY KEY (row, col)
);

CREATE TABLE IF NOT EXISTS user_last_update (
    user_id TEXT PRIMARY KEY,
    last_update BIGINT NOT NULL
);
