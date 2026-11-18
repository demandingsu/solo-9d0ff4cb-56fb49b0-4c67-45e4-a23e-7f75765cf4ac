"""多站触发汇总自测:门槛/起点提取、跨窗延续、未知窗、2 秒归并、
去重、换版失效、失败不替换、交错一致性。"""
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

W0 = 1_700_000_000_000          # 整 10 秒对齐
WINDOW = 10_000
TRIGGER_THRESHOLD = 40


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------- 辅助

def samples(points=None, base=0):
    arr = [base] * 1000
    for i, v in (points or {}).items():
        arr[i] = v
    return arr


def fill(client, station, wstart, rnd, points=None, base=0):
    """上传一个整窗片段;片段号按窗口偏移区分,避免同轮同号冲突。"""
    no = (wstart - W0) // WINDOW
    seg = samples(points, base)
    body = f"{station},{wstart},{rnd},{no},{' '.join(map(str, seg))}"
    r = client.post("/api/upload", content=body)
    assert r.status_code == 200, r.text
    return r


def pub(client, station, wstart):
    r = client.post("/api/publish",
                    json={"station": station, "window_start_ms": wstart})
    assert r.status_code == 200, r.text
    return r.json()


def run_round(client, stations, lo, hi, threshold=TRIGGER_THRESHOLD):
    return client.post("/api/triggers/run", json={
        "stations": stations, "from_ms": lo, "to_ms": hi,
        "threshold": threshold})


def current(client):
    return client.get("/api/triggers/rounds/current")


def stations_of(candidate):
    return sorted(candidate["stations"])


def detections_of(detail):
    return [t for c in detail["result"]["candidates"] for t in c["triggers"]]


# ---------------------------------------------------------------- 基础归并

def test_two_stations_merge_and_list_times_and_versions(client):
    fill(client, "STA1", W0, 1, {100: 50, 101: 50, 102: 50})
    fill(client, "STA2", W0, 1, {150: -60})
    pub(client, "STA1", W0)
    pub(client, "STA2", W0)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["status"] == "current"
    assert d["threshold"] == TRIGGER_THRESHOLD

    assert len(d["result"]["candidates"]) == 1
    c = d["result"]["candidates"][0]
    assert stations_of(c) == ["STA1", "STA2"]
    assert c["event_time_ms"] == W0 + 1000
    assert c["span_ms"] == 500
    assert c["triggers"][0]["start_ms"] == W0 + 1000
    assert c["triggers"][1]["start_ms"] == W0 + 1500
    for t in c["triggers"]:
        assert t["source_windows"][0]["version"] == 1
        assert t["source_windows"][0]["window_start_ms"] == W0
    # 连续越线只取起点,不按样点重复
    assert d["result"]["stats"]["detections"] == 2
    assert d["result"]["stations_with_unknown"] == []


def test_threshold_filters_and_negative_amplitude_counts(client):
    fill(client, "STA1", W0, 1, {100: 39, 200: -39})
    fill(client, "STA2", W0, 1, {150: 39})
    pub(client, "STA1", W0)
    pub(client, "STA2", W0)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW, threshold=40)
    assert r.status_code == 200
    assert r.json()["result"]["candidates"] == []
    assert r.json()["result"]["stats"]["detections"] == 0

    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW, threshold=39)
    assert r.status_code == 200
    c = r.json()["result"]["candidates"][0]
    assert stations_of(c) == ["STA1", "STA2"]


def test_run_start_recorded_not_every_point(client):
    fill(client, "STA1", W0, 1, {300: 90, 301: 90, 302: 90, 303: 90})
    pub(client, "STA1", W0)
    fill(client, "STA2", W0, 1, {400: 90})
    pub(client, "STA2", W0)
    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    c = r.json()["result"]["candidates"][0]
    sta1 = [t for t in c["triggers"] if t["station"] == "STA1"]
    assert len(sta1) == 1
    assert sta1[0]["start_ms"] == W0 + 3000
    assert sta1[0]["last_ms"] == W0 + 3030


# ---------------------------------------------------------------- 跨窗延续

def test_trigger_continues_across_10s_window_once(client):
    # STA1 越线从 W0 末尾延续到 W1 开头:只算一个触发,来源窗列两个版本
    fill(client, "STA1", W0, 1, {i: 70 for i in range(950, 1000)})
    fill(client, "STA1", W0 + WINDOW, 1,
         {i: 70 for i in range(0, 100)})
    fill(client, "STA2", W0 + WINDOW, 1, {120: 70})
    for w in (W0, W0 + WINDOW):
        pub(client, "STA1", w)
    pub(client, "STA2", W0 + WINDOW)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + 2 * WINDOW)
    assert r.status_code == 200
    d = r.json()
    assert len(d["result"]["candidates"]) == 1
    c = d["result"]["candidates"][0]
    assert c["event_time_ms"] == W0 + 9500
    sta1 = [t for t in c["triggers"] if t["station"] == "STA1"]
    assert len(sta1) == 1, "跨窗越线不得拆成两个触发"
    src = sta1[0]["source_windows"]
    assert [s["window_start_ms"] for s in src] == [W0, W0 + WINDOW]
    assert [s["version"] for s in src] == [1, 1]
    assert sta1[0]["last_ms"] == W0 + WINDOW + 990
    # 该触发只属于这一条候选
    assert sum(t["station"] == "STA1" for t in detections_of(d)) == 1


def test_range_start_inside_continuation_is_not_a_new_start(client):
    """越线从范围前窗外延续进来:不得把范围首点当新起点。"""
    fill(client, "STA1", W0, 1, {999: 70})
    fill(client, "STA1", W0 + WINDOW, 1, {0: 70, 1: 70, 2: 0})
    fill(client, "STA2", W0 + WINDOW, 1, {50: 70})
    pub(client, "STA1", W0)
    pub(client, "STA1", W0 + WINDOW)
    pub(client, "STA2", W0 + WINDOW)

    # 从 W1 开始扫描:STA1 的越线在 W1 首点延续,起点在范围外 -> 无触发;
    # STA2 的越线起点在范围内但只有一站 -> 不形成候选
    r = run_round(client, ["STA1", "STA2"], W0 + WINDOW, W0 + 2 * WINDOW)
    d = r.json()
    assert d["result"]["stats"]["detections"] == 1
    assert d["result"]["candidates"] == []
    assert d["result"]["stats"]["dropped_single_station_groups"] == 1
    # 范围前窗过期/缺失不影响范围内结论,也不误报未知
    assert all(u["window_start_ms"] >= W0 + WINDOW
               for u in d["result"]["unknown_windows"])


def test_unknown_edge_before_does_not_pollute_range(client):
    # 范围前窗完全缺失:范围内有正常触发,不应多出未知窗噪声
    fill(client, "STA1", W0 + WINDOW, 1, {100: 70})
    fill(client, "STA2", W0 + WINDOW, 1, {150: 70})
    pub(client, "STA1", W0 + WINDOW)
    pub(client, "STA2", W0 + WINDOW)
    d = run_round(client, ["STA1", "STA2"],
                  W0 + WINDOW, W0 + 2 * WINDOW).json()
    assert len(d["result"]["candidates"]) == 1
    assert d["result"]["unknown_windows"] == []
    assert d["result"]["stations_with_unknown"] == []


def test_cross_window_continuation_stops_at_unknown_window(client):
    fill(client, "STA1", W0, 1, {i: 70 for i in range(900, 1000)})
    # W1 未发布(未知),W2 开头继续越线
    fill(client, "STA1", W0 + 2 * WINDOW, 1,
         {i: 70 for i in range(0, 100)})
    fill(client, "STA2", W0, 1, {950: 70})
    pub(client, "STA1", W0)
    pub(client, "STA1", W0 + 2 * WINDOW)
    pub(client, "STA2", W0)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + 3 * WINDOW)
    assert r.status_code == 200
    d = r.json()
    # W0 的触发与 STA2 归并;W2 的触发因跨过未知窗不能与前者串联,
    # 且只有 STA1 一站 -> 不形成候选
    assert len(d["result"]["candidates"]) == 1
    c = d["result"]["candidates"][0]
    assert c["event_time_ms"] == W0 + 9000
    assert stations_of(c) == ["STA1", "STA2"]
    # STA1 在 W0 的触发归并;W2 的触发因跨过未知窗不能与前者串联,
    # 且只有 STA1 一站 -> 不形成候选。STA2 在 W0 的触发也参与归并。
    assert d["result"]["stats"]["detections"] == 3
    assert d["result"]["stats"]["dropped_single_station_groups"] == 1

    unknown = d["result"]["unknown_windows"]
    w1 = [u for u in unknown
          if u["station"] == "STA1" and u["window_start_ms"] == W0 + WINDOW]
    assert len(w1) == 1 and w1[0]["state"] == "missing"
    assert "STA1" in d["result"]["stations_with_unknown"]


# ---------------------------------------------------------------- 未知窗语义

def test_missing_windows_reported_not_as_no_event(client):
    # STA1 完全没有已发布窗:整段都算未知,不能当作"无事件"
    r = run_round(client, ["STA1"], W0, W0 + 2 * WINDOW)
    assert r.status_code == 200
    d = r.json()
    assert d["result"]["candidates"] == []
    observed = [u for u in d["result"]["unknown_windows"]
                if u["role"] == "observed"]
    assert len(observed) == 2
    assert all(u["state"] == "missing" for u in observed)
    assert d["result"]["stations_with_unknown"] == ["STA1"]

    # 现行轮仍是有效结果,但明确标出未知来源
    cur = current(client)
    assert cur.status_code == 200
    assert cur.json()["result"]["stations_with_unknown"] == ["STA1"]


def test_stale_window_is_unknown_and_never_used(client):
    fill(client, "STA1", W0, 1, {100: 90})
    pub(client, "STA1", W0)
    fill(client, "STA1", W0, 2, {100: 91})   # 补传 -> 现役窗过期
    fill(client, "STA2", W0, 1, {110: 90})
    pub(client, "STA2", W0)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert r.status_code == 200
    d = r.json()
    assert d["result"]["candidates"] == []
    # STA2 的触发不来自 STA1 的过期窗,单站簇不形成候选
    assert d["result"]["stats"]["detections"] == 1
    assert all(t["station"] != "STA1" for t in detections_of(d))
    assert d["result"]["stations_with_unknown"] == ["STA1"]
    u = [u for u in d["result"]["unknown_windows"]
         if u["station"] == "STA1" and u["role"] == "observed"][0]
    assert u["state"] == "stale"


# ---------------------------------------------------------------- 2 秒归并边界

def test_span_exactly_2000ms_allowed_2010ms_not(client):
    fill(client, "STA1", W0, 1, {100: 90})
    fill(client, "STA2", W0, 1, {300: 90})   # +2000 ms
    pub(client, "STA1", W0)
    pub(client, "STA2", W0)
    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    c = r.json()["result"]["candidates"]
    assert len(c) == 1 and c[0]["span_ms"] == 2000

    # 距离 2010 ms -> 不应串在一起,各自单站 -> 无候选
    fill(client, "STA3", W0, 1, {100: 90})
    fill(client, "STA4", W0, 1, {301: 90})
    pub(client, "STA3", W0)
    pub(client, "STA4", W0)
    r = run_round(client, ["STA3", "STA4"], W0, W0 + WINDOW)
    assert r.status_code == 200
    assert r.json()["result"]["candidates"] == []


def test_no_trigger_reused_between_candidates(client):
    # A(t0) B(t0+1500) 归并;C(t0+3000) 超出首尾 2 秒,单独成簇被丢弃
    fill(client, "STA1", W0, 1, {100: 90})
    fill(client, "STA2", W0, 1, {250: 90})
    fill(client, "STA3", W0, 1, {400: 90})
    for st in ("STA1", "STA2", "STA3"):
        pub(client, st, W0)
    r = run_round(client, ["STA1", "STA2", "STA3"], W0, W0 + WINDOW)
    d = r.json()
    assert len(d["result"]["candidates"]) == 1
    assert stations_of(d["result"]["candidates"][0]) == ["STA1", "STA2"]
    assert d["result"]["stats"]["detections"] == 3
    assert d["result"]["stats"]["dropped_single_station_groups"] == 1
    starts = [t["start_ms"] for t in detections_of(d)]
    assert len(starts) == len(set(starts)) == 2   # 每个触发只出现一次


def test_repeated_trigger_windows_not_duplicated(client):
    # STA1 的越线段从 W0 中段起连续贯穿三个窗,仍只出现一次且来源窗完整
    segments = [
        {i: 80 for i in range(500, 1000)},   # W0 后半
        {i: 80 for i in range(1000)},        # W1 整窗
        {i: 80 for i in range(0, 500)},      # W2 前半
    ]
    for k, pts in enumerate(segments):
        fill(client, "STA1", W0 + k * WINDOW, 1, pts)
        pub(client, "STA1", W0 + k * WINDOW)
    fill(client, "STA2", W0, 1, {600: 80})
    pub(client, "STA2", W0)

    r = run_round(client, ["STA1", "STA2"], W0, W0 + 3 * WINDOW)
    d = r.json()
    sta1 = [t for t in detections_of(d) if t["station"] == "STA1"]
    assert len(sta1) == 1
    assert sta1[0]["start_ms"] == W0 + 5000
    assert [s["window_start_ms"] for s in sta1[0]["source_windows"]] == [
        W0, W0 + WINDOW, W0 + 2 * WINDOW]
    assert len(d["result"]["candidates"]) == 1
    # STA1 与 STA2 均只出现一次
    assert [t["station"] for t in d["result"]["candidates"][0]["triggers"]] == [
        "STA1", "STA2"]


# ---------------------------------------------------------------- 轮次留存与失效

def _publish_pair(client, rnd=1, points=None):
    fill(client, "STA1", W0, rnd, points or {100: 90})
    fill(client, "STA2", W0, rnd, {110: 90})
    pub(client, "STA1", W0)
    pub(client, "STA2", W0)


def test_round_history_and_invalidation_on_backfill(client):
    _publish_pair(client)
    rid = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW).json()["id"]
    assert current(client).json()["id"] == rid

    # 补传高轮次使 STA1 现役窗过期 -> 旧轮标 known invalid
    fill(client, "STA1", W0, 2, {100: 91})
    assert current(client).status_code == 404
    old = client.get(f"/api/triggers/rounds/{rid}")
    assert old.status_code == 200
    od = old.json()
    assert od["status"] == "invalid"
    assert od["invalidated_at"]
    # 旧结果内容仍可追溯,来源快照仍是当时的版本
    assert len(od["result"]["candidates"]) == 1
    snap = {w: (st, v) for w, st, v in od["sources"]["STA1"]}
    assert snap[W0] == ("active", 1)
    # 快照覆盖范围含前后边界窗,缺失窗也记为 missing
    assert snap[W0 - WINDOW] == ("missing", 0)

    # 来源仍失效时重算失败不得产生新现行轮
    bad = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW, threshold=-1)
    assert bad.status_code == 400
    assert current(client).status_code == 404

    # 重新发布 + 重算成功才替换
    pub(client, "STA1", W0)
    new = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert new.status_code == 200
    assert new.json()["id"] != rid
    assert current(client).json()["id"] == new.json()["id"]
    assert client.get(f"/api/triggers/rounds/{rid}").json()["status"] == "invalid"


def test_republish_invalidates_current(client):
    _publish_pair(client)
    run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert current(client).status_code == 200
    pub(client, "STA1", W0)          # 换版:版本 1 -> 2
    assert current(client).status_code == 404
    rounds = client.get("/api/triggers/rounds").json()["rounds"]
    assert rounds[0]["status"] == "invalid"
    # 成功重算后替换
    assert run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW).status_code == 200
    assert current(client).status_code == 200


def test_missing_source_becoming_published_invalidates_current(client):
    """来源快照中的缺窗后来被发布,同样属于来源失效。"""
    fill(client, "STA1", W0, 1, {100: 90})
    pub(client, "STA1", W0)
    fill(client, "STA2", W0, 1, {0: 0})
    rid = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW).json()["id"]
    assert current(client).json()["result"]["stations_with_unknown"] == ["STA2"]

    pub(client, "STA2", W0)          # 缺窗 -> 现役:来源变化
    assert current(client).status_code == 404
    assert client.get(f"/api/triggers/rounds/{rid}").json()["status"] == "invalid"
    # 重算后才恢复现行,并消除未知窗
    r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert r.status_code == 200
    assert r.json()["result"]["stations_with_unknown"] == []
    assert current(client).json()["id"] == r.json()["id"]


def test_failed_run_keeps_current_when_sources_unchanged(client):
    _publish_pair(client)
    rid = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW).json()["id"]
    for bad in (
        {"stations": ["STA1"], "from_ms": W0, "to_ms": W0,
         "threshold": 40},                          # 空时段
        {"stations": ["STA1"], "from_ms": W0, "to_ms": W0 + WINDOW,
         "threshold": -5},                          # 负门槛
        {"stations": ["STA1"], "from_ms": W0,
         "to_ms": W0 + 720 * WINDOW + WINDOW, "threshold": 40},  # 超窗口上限
        {"stations": ["BAD STATION"], "from_ms": W0,
         "to_ms": W0 + WINDOW, "threshold": 40},    # 非法台站
        {"stations": ["NO_SUCH_STA"], "from_ms": W0,
         "to_ms": W0 + WINDOW, "threshold": 40},    # 未知台站:整段未知
    ):
        r = client.post("/api/triggers/run", json=bad)
        if bad["stations"] == ["NO_SUCH_STA"]:
            # 未知台站不是错误:整段记为未知,不是"无事件"
            assert r.status_code == 200
            assert r.json()["result"]["stations_with_unknown"] == ["NO_SUCH_STA"]
            continue
        assert r.status_code == 400, (bad, r.text)
    assert current(client).json()["id"] != rid   # 上面的有效轮已替换 rid
    assert len(client.get("/api/triggers/rounds").json()["rounds"]) == 2


def test_rounds_listed_newest_first_and_detail_traceable(client):
    _publish_pair(client)
    id1 = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW).json()["id"]
    id2 = run_round(client, ["STA1"], W0, W0 + WINDOW, threshold=10).json()["id"]
    rows = client.get("/api/triggers/rounds").json()["rounds"]
    assert [r["id"] for r in rows] == [id2, id1]
    assert rows[0]["status"] == "current"
    assert rows[1]["status"] == "superseded"
    assert client.get(f"/api/triggers/rounds/{id1}").json()["status"] == "superseded"
    assert client.get("/api/triggers/rounds/99999").status_code == 404


# ---------------------------------------------------------------- 交错一致性

def test_rounds_consistent_under_interleaved_publish(client):
    """发布反复换版、汇总连续运行:每轮结果与来源版本须来自同一提交状态。"""
    fill(client, "STA1", W0, 1, {100: 90})
    fill(client, "STA2", W0, 1, {120: 90})
    pub(client, "STA1", W0)
    pub(client, "STA2", W0)

    errors = []
    stop = threading.Event()

    def writer():
        try:
            for _ in range(20):
                pub(client, "STA1", W0)
                pub(client, "STA2", W0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"writer: {e}")
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                r = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
                if r.status_code != 200:
                    errors.append(f"reader: {r.status_code} {r.text}")
                    continue
                d = r.json()
                for c in d["result"]["candidates"]:
                    for t in c["triggers"]:
                        for sw in t["source_windows"]:
                            snap = {w: (st, v) for w, st, v in
                                    d["sources"][t["station"]]}
                            assert snap[sw["window_start_ms"]] == (
                                "active", sw["version"])
                for station, rows in d["sources"].items():
                    for _, state, _version in rows:
                        assert state in ("active", "stale", "missing")
        except Exception as e:  # noqa: BLE001
            errors.append(f"reader: {e}")

    threads = [threading.Thread(target=writer)] + \
              [threading.Thread(target=reader) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    # 收尾:写者停止后重算,应为现行有效轮
    final = run_round(client, ["STA1", "STA2"], W0, W0 + WINDOW)
    assert final.status_code == 200
    assert current(client).json()["id"] == final.json()["id"]
