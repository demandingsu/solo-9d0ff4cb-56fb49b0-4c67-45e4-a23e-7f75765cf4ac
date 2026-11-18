"""多站触发汇总:从各台站已发布的现役整窗中提取越线起点并跨站归并候选。

业务约定:
- 仅使用已发布且现役(active)的整 10 秒窗;缺窗(stale/未发布)记为未知,
  未知窗既不产生触发,也不允许触发跨窗延续——但必须出现在结果中,
  不能被当作"无事件"。
- 触发点 = |振幅| >= 门槛 的连续越线段的**起点**。越线可跨相邻 10 秒窗
  延续(同一个触发只记一次起点,来源窗列出所有跨过的窗);遇到未知窗则
  连续性中断,未知窗在结果中单列。
- 跨站归并:按触发时刻排序,候选内首尾触发时刻相差不得超过 2000 ms,且
  至少来自两个不同台站。每个触发只归入一个候选,不会被重复归入。
- 每轮读取、计算、落库在同一个 BEGIN IMMEDIATE 事务中完成,上传/发布
  交错时结果与来源版本来自同一已提交状态。
- 轮次结果留存;来源窗被补传标过期或重新发布(状态/版本变化)时,现行轮
  标记 invalid,旧轮仍可查看但不能作为现行结果;只有新一轮计算成功才替换
  现行。
"""
from __future__ import annotations

import json

from . import core

CLUSTER_GAP_MS = 2_000
MAX_STATIONS_PER_ROUND = 100
MAX_WINDOWS_PER_ROUND = core.MAX_WINDOWS_PER_QUERY
MAX_WINDOW_SCANS = 20_000   # 台站数 × 窗口数上限,避免单轮长时间占用
MAX_ROUNDS_LISTED = 50
EDGE_SAMPLES = 100          # 边界窗用于跨范围连续性判断的样点数


class TriggerRejected(Exception):
    """触发汇总请求参数不合法(HTTP 400);不产生轮次、不改变现行结果。"""


def _dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


def _align_range(from_ms: int, to_ms: int):
    lo = from_ms - from_ms % core.WINDOW_MS
    hi = to_ms if to_ms % core.WINDOW_MS == 0 else \
        to_ms + (core.WINDOW_MS - to_ms % core.WINDOW_MS)
    if hi <= lo:
        raise TriggerRejected("时段终点必须大于起点")
    n = (hi - lo) // core.WINDOW_MS
    if n > MAX_WINDOWS_PER_ROUND:
        raise TriggerRejected(
            f"单轮扫描窗口数超过上限 {MAX_WINDOWS_PER_ROUND}(当前 {n})")
    return lo, hi


def _normalize_stations(stations) -> list:
    if stations is None:
        return []
    if not isinstance(stations, (list, tuple)):
        raise TriggerRejected("stations 必须是台站名数组")
    out = []
    for s in stations:
        if not isinstance(s, str) or not core._STATION_TOKEN.match(s):
            raise TriggerRejected(f"台站名非法: {s!r}")
        if s not in out:
            out.append(s)
    return out


# ---------------------------------------------------------------- 单站扫描

def _scan_station(station: str, lo: int, hi: int, winmap: dict, threshold: int):
    """扫描单站,返回 (detections, unknown_windows)。

    触发起点只取落在 [lo, hi) 内的;范围前后各一个 10 秒窗用于识别越线
    是否从范围外延续进来 / 延续出去,其样点不单独产生触发。
    """
    detections, unknown = [], []
    run = None
    prev_ms = None

    sequence = [(lo - core.WINDOW_MS, "edge_before")]
    sequence += [(w, "observed") for w in range(lo, hi, core.WINDOW_MS)]
    sequence.append((hi, "edge_after"))

    def flush():
        nonlocal run
        if run is not None and lo <= run["start_ms"] < hi:
            detections.append(run)
        run = None

    for wstart, role in sequence:
        row = winmap.get(wstart)
        unusable = row is None or row["state"] != "active"
        # 边界窗仅为连续性服务:范围外若缺失/过期,不影响范围内结果,
        # 因此不列入未知窗清单;范围内窗口一律如实记录。
        if unusable:
            if lo <= wstart < hi:
                unknown.append({
                    "station": station,
                    "window_start_ms": wstart,
                    "start_utc": core.ms_to_iso(wstart),
                    "state": "stale" if row is not None else "missing",
                    "role": "observed",
                })
            flush()
            prev_ms = None
            continue

        samples = json.loads(row["samples"])
        if role == "edge_before":
            indices = range(core.WINDOW_SAMPLES - EDGE_SAMPLES,
                            core.WINDOW_SAMPLES)
        elif role == "edge_after":
            indices = range(0, EDGE_SAMPLES)
        else:
            indices = range(core.WINDOW_SAMPLES)

        for i in indices:
            t = wstart + i * core.SAMPLE_MS
            over = abs(samples[i]) >= threshold
            if over:
                if run is not None and prev_ms == t - core.SAMPLE_MS:
                    run["last_ms"] = t
                else:
                    flush()
                    run = {"station": station, "start_ms": t,
                           "last_ms": t, "source_windows": []}
                sw = run["source_windows"]
                if not sw or sw[-1]["window_start_ms"] != wstart:
                    sw.append({"window_start_ms": wstart,
                               "start_utc": core.ms_to_iso(wstart),
                               "version": row["version"],
                               "role": role})
            else:
                flush()
            prev_ms = t

    flush()
    for d in detections:
        d["start_utc"] = core.ms_to_iso(d["start_ms"])
        d["last_utc"] = core.ms_to_iso(d["last_ms"])
    return detections, unknown


# ---------------------------------------------------------------- 跨站归并

def _cluster_detections(detections: list):
    """按触发时刻排序切簇,簇内首尾相差 <= 2000 ms 且至少两站才算候选。"""
    ordered = sorted(detections, key=lambda d: (d["start_ms"], d["station"]))
    groups, cur = [], []
    for d in ordered:
        if cur and d["start_ms"] - cur[0]["start_ms"] > CLUSTER_GAP_MS:
            groups.append(cur)
            cur = []
        cur.append(d)
    if cur:
        groups.append(cur)

    candidates, dropped = [], 0
    for group in groups:
        stations = sorted({d["station"] for d in group})
        if len(stations) < 2:
            dropped += 1
            continue
        first, last = group[0]["start_ms"], group[-1]["start_ms"]
        candidates.append({
            "event_time_ms": first,
            "event_time_utc": core.ms_to_iso(first),
            "span_ms": last - first,
            "stations": stations,
            "triggers": group,
        })
    candidates.sort(key=lambda c: c["event_time_ms"])
    for i, c in enumerate(candidates, 1):
        c["event_no"] = i
    return candidates, dropped


# ---------------------------------------------------------------- 轮次存取

def _summary(row) -> dict:
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "stations": json.loads(row["stations"]),
        "from_ms": row["from_ms"],
        "from_utc": core.ms_to_iso(row["from_ms"]),
        "to_ms": row["to_ms"],
        "to_utc": core.ms_to_iso(row["to_ms"]),
        "threshold": row["threshold"],
        "invalidated_at": row["invalidated_at"],
    }


def _detail(row) -> dict:
    detail = _summary(row)
    detail["result"] = json.loads(row["result"])
    detail["sources"] = json.loads(row["sources"])
    return detail


def run_trigger_round(db, stations, from_ms, to_ms, threshold) -> dict:
    """执行一轮汇总;成功提交后才替换现行结果,任何失败回滚且旧轮不变。"""
    if not isinstance(threshold, int) or isinstance(threshold, bool):
        raise TriggerRejected("振幅门槛必须是整数")
    if threshold < 0:
        raise TriggerRejected("振幅门槛必须为非负整数")
    if from_ms is None or to_ms is None:
        raise TriggerRejected("必须给出 from_ms 与 to_ms")
    lo, hi = _align_range(int(from_ms), int(to_ms))
    selected = _normalize_stations(stations)

    with db.lock:
        conn = db.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not selected:
                selected = [r["station"] for r in conn.execute(
                    "SELECT station FROM segments UNION "
                    "SELECT station FROM windows ORDER BY 1").fetchall()]
            if not selected:
                raise TriggerRejected("没有可汇总的台站")
            if len(selected) > MAX_STATIONS_PER_ROUND:
                raise TriggerRejected(f"单轮台站数超过上限 {MAX_STATIONS_PER_ROUND}")
            scans = len(selected) * ((hi - lo) // core.WINDOW_MS + 2)
            if scans > MAX_WINDOW_SCANS:
                raise TriggerRejected(
                    f"单轮扫描量(台站×窗口)超过上限 {MAX_WINDOW_SCANS}"
                    f"(当前 {scans}),请缩小台站或时段范围")

            # 读取范围含前后边界窗;同一事务内读取,来源版本与计算一致
            rows = conn.execute(
                "SELECT station, window_start_ms, state, samples, version "
                "FROM windows WHERE window_start_ms>=? AND window_start_ms<=? "
                "ORDER BY station, window_start_ms",
                (lo - core.WINDOW_MS, hi)).fetchall()
            winmaps = {s: {} for s in selected}
            for r in rows:
                if r["station"] in winmaps:
                    winmaps[r["station"]][r["window_start_ms"]] = r

            all_detections, unknown = [], []
            sources = {}
            for station in selected:
                winmap = winmaps[station]
                dets, unk = _scan_station(station, lo, hi, winmap, threshold)
                all_detections.extend(dets)
                unknown.extend(unk)
                # 记录范围内每个窗(含缺失)的状态与版本快照:
                # 缺失窗后续被发布同样视为来源变化。
                sources[station] = [
                    [w, winmap[w]["state"], winmap[w]["version"]]
                    if w in winmap else [w, "missing", 0]
                    for w in range(lo - core.WINDOW_MS, hi + core.WINDOW_MS,
                                   core.WINDOW_MS)
                ]

            candidates, dropped = _cluster_detections(all_detections)
            unknown.sort(key=lambda u: (u["station"], u["window_start_ms"],
                                        u["role"]))
            result = {
                "candidates": candidates,
                "unknown_windows": unknown,
                "stations_with_unknown": sorted({
                    u["station"] for u in unknown if u["role"] == "observed"}),
                "stats": {
                    "station_count": len(selected),
                    "window_count": (hi - lo) // core.WINDOW_MS,
                    "detections": len(all_detections),
                    "candidates": len(candidates),
                    "dropped_single_station_groups": dropped,
                    "unknown_windows": len(unknown),
                },
            }
            now = core._utc_now_iso()
            conn.execute(
                "UPDATE trigger_rounds SET status='superseded' "
                "WHERE status='current'")
            cur = conn.execute(
                "INSERT INTO trigger_rounds(created_at,status,stations,from_ms,"
                "to_ms,threshold,sources,result) VALUES(?,?,?,?,?,?,?,?)",
                (now, "current", _dumps(selected), lo, hi, threshold,
                 _dumps(sources), _dumps(result)))
            conn.commit()
            row = conn.execute("SELECT * FROM trigger_rounds WHERE id=?",
                               (cur.lastrowid,)).fetchone()
            return _detail(row)
        except BaseException:
            conn.rollback()
            raise


def invalidate_on_change(conn, touched) -> None:
    """来源窗状态/版本变化时,把现行轮标为 invalid(不影响历史轮查看)。

    touched: {(station, window_start_ms)}。只在同一事务内调用。
    """
    row = conn.execute(
        "SELECT id, sources FROM trigger_rounds WHERE status='current'"
    ).fetchone()
    if row is None or not touched:
        return
    sources = json.loads(row["sources"])
    broken = False
    for station, wstart in touched:
        recorded = None
        for w, state, version in sources.get(station, []):
            if w == wstart:
                recorded = (state, version)
                break
        if recorded is None:
            continue
        cur = conn.execute(
            "SELECT state, version FROM windows WHERE station=? "
            "AND window_start_ms=?", (station, wstart)).fetchone()
        now = ("missing", 0) if cur is None else (cur["state"], cur["version"])
        if now != recorded:
            broken = True
            break
    if broken:
        conn.execute(
            "UPDATE trigger_rounds SET status='invalid', invalidated_at=? "
            "WHERE id=?", (core._utc_now_iso(), row["id"]))


def get_round(db, round_id: int):
    with db.lock:
        row = db.conn.execute("SELECT * FROM trigger_rounds WHERE id=?",
                              (round_id,)).fetchone()
    return None if row is None else _detail(row)


def current_round(db):
    with db.lock:
        row = db.conn.execute(
            "SELECT * FROM trigger_rounds WHERE status='current' "
            "ORDER BY id DESC LIMIT 1").fetchone()
    return None if row is None else _detail(row)


def list_rounds(db, limit: int = 20) -> list:
    limit = max(1, min(int(limit), MAX_ROUNDS_LISTED))
    with db.lock:
        rows = db.conn.execute(
            "SELECT * FROM trigger_rounds ORDER BY id DESC LIMIT ?",
            (limit,)).fetchall()
    out = []
    for row in rows:
        item = _summary(row)
        item["stats"] = json.loads(row["result"]).get("stats", {})
        out.append(item)
    return out
