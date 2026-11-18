"""SQLite 存储层:单连接 + 可重入锁,所有请求序列化执行。

单连接配合显式 BEGIN IMMEDIATE / COMMIT / ROLLBACK,保证上传、发布、
读取交错时,每个请求看到的都是同一个已提交状态。
"""
from __future__ import annotations

import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    station      TEXT    NOT NULL,
    round        INTEGER NOT NULL,
    segment_no   INTEGER NOT NULL,
    start_ms     INTEGER NOT NULL,          -- UTC 起点,epoch 毫秒,须按 10ms 对齐
    n            INTEGER NOT NULL,          -- 样点数
    samples      TEXT    NOT NULL,          -- JSON 整数数组
    content_hash TEXT    NOT NULL,          -- 起点+振幅序列的散列,用于同内容判重
    created_at   TEXT    NOT NULL,
    PRIMARY KEY (station, round, segment_no)
);
CREATE INDEX IF NOT EXISTS idx_segments_station_range
    ON segments (station, start_ms);

CREATE TABLE IF NOT EXISTS windows (
    station         TEXT    NOT NULL,
    window_start_ms INTEGER NOT NULL,       -- UTC 整 10 秒窗起点,epoch 毫秒
    state           TEXT    NOT NULL,       -- 'active' | 'stale'
    samples         TEXT,                   -- 发布时的 1000 个整数样点(JSON)
    rounds          TEXT,                   -- 每个样点的来源轮次(JSON)
    version         INTEGER NOT NULL,
    published_at    TEXT,
    PRIMARY KEY (station, window_start_ms)
);

CREATE TABLE IF NOT EXISTS trigger_rounds (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT    NOT NULL,
    status          TEXT    NOT NULL,       -- 'current' | 'invalid' | 'superseded'
    stations        TEXT    NOT NULL,       -- 请求台站(JSON 数组)
    from_ms         INTEGER NOT NULL,       -- 对齐后的扫描起点(含)
    to_ms           INTEGER NOT NULL,       -- 对齐后的扫描终点(不含)
    threshold       INTEGER NOT NULL,       -- 振幅门槛(|振幅| >= 门槛)
    sources         TEXT    NOT NULL,       -- 扫描范围内全部窗的状态/版本快照(JSON)
    result          TEXT    NOT NULL,       -- 事件、未知窗、统计与参数(JSON)
    invalidated_at  TEXT                    -- 来源失效时刻
);
CREATE INDEX IF NOT EXISTS idx_trigger_rounds_status
    ON trigger_rounds (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_rounds_one_current
    ON trigger_rounds (status) WHERE status = 'current';
"""


class Database:
    def __init__(self, path: str):
        # isolation_level=None:关闭隐式事务,事务完全由 core 层显式控制
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.lock = threading.RLock()
        with self.lock:
            self.conn.executescript(SCHEMA)

    def close(self):
        with self.lock:
            self.conn.close()
