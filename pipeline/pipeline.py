#!/usr/bin/env python3
"""Correctness-first DuckDB implementation of the Shopping Sessions rules."""

from __future__ import annotations

import glob
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


class Timer:
    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.last = self.started

    def mark(self, name: str) -> None:
        now = time.perf_counter()
        log(f"stage {name}: {now - self.last:.6f}s (total {now - self.started:.6f}s)")
        self.last = now


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_line(line: bytes, count: int) -> list[bytes | None] | None:
    """Parse precisely the deliberately small CSV dialect in rules.md."""
    fields = line.split(b",")
    if len(fields) != count:
        return None
    parsed: list[bytes | None] = []
    for field in fields:
        if b'"' in field:
            if len(field) < 2 or field[0] != 34 or field[-1] != 34 or b'"' in field[1:-1]:
                return None
            field = field[1:-1]
        parsed.append(field if field else None)
    return parsed


def csv_safe(field: bytes | None) -> bytes:
    """Protect CR from DuckDB's CSV newline normalization."""
    if field is None:
        return b""
    return field.replace(b"%", b"%25").replace(b"\r", b"%0D")


def normalize_csv(
    inputs: list[str], output: Path, header: bytes, count: int, required: tuple[int, ...] = ()
) -> int:
    """Drop malformed rows and write quote-free, DuckDB-safe CSV."""
    rows = 0
    with output.open("wb", buffering=1024 * 1024) as dst:
        dst.write(header + b"\n")
        for filename in inputs:
            with open(filename, "rb", buffering=1024 * 1024) as src:
                src.readline()
                for raw in src:
                    if raw.endswith(b"\n"):
                        raw = raw[:-1]
                    parsed = parse_line(raw, count)
                    if parsed is None or any(parsed[index] is None for index in required):
                        continue
                    dst.write(b",".join(csv_safe(field) for field in parsed) + b"\n")
                    rows += 1
    return rows


def prepare_inputs(folder: Path) -> dict[str, Path]:
    folder.mkdir(parents=True, exist_ok=True)
    specs = {
        "links": (sorted(glob.glob(os.path.join(DATA, "links", "*.csv"))), b"user_id_a,user_id_b", 2, (0, 1)),
        "events": (sorted(glob.glob(os.path.join(DATA, "events", "*.csv"))), b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", 7, (0,)),
        "clicks": (sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv"))), b"click_id,user_id,campaign_id,ts", 4, (0,)),
        "products": ([os.path.join(DATA, "products.csv")], b"product_id,category", 2, ()),
        "campaigns": ([os.path.join(DATA, "campaigns.csv")], b"campaign_id,channel", 2, ()),
    }
    paths: dict[str, Path] = {}
    for name, (inputs, header, count, required) in specs.items():
        path = folder / f"{name}.csv"
        normalize_csv(inputs, path, header, count, required)
        paths[name] = path
    return paths


def csv_scan(path: Path, columns: dict[str, str]) -> str:
    definitions = ", ".join(f"'{name}': '{kind}'" for name, kind in columns.items())
    return (
        f"read_csv({sql_string(str(path))}, header=true, quote='', escape='', "
        f"nullstr='', columns={{{definitions}}})"
    )


def byte_less(left: str, right: str) -> bool:
    return left.encode("utf-8") < right.encode("utf-8")


def build_identity_map(con: duckdb.DuckDBPyConnection, links_scan: str, output: Path) -> int:
    parent: dict[str, str] = {}

    def find(value: str) -> str:
        root = value
        while parent[root] != root:
            root = parent[root]
        while parent[value] != value:
            nxt = parent[value]
            parent[value] = root
            value = nxt
        return root

    cursor = con.execute(f"SELECT decode_field(user_id_a), decode_field(user_id_b) FROM {links_scan}")
    while True:
        rows = cursor.fetchmany(100_000)
        if not rows:
            break
        for left, right in rows:
            if left not in parent:
                parent[left] = left
            if right not in parent:
                parent[right] = right
            a, b = find(left), find(right)
            if a != b:
                if byte_less(b, a):
                    a, b = b, a
                parent[b] = a

    with output.open("wb", buffering=1024 * 1024) as dst:
        dst.write(b"user_id,person\n")
        for user_id in parent:
            person = find(user_id)
            dst.write(csv_safe(user_id.encode()) + b"," + csv_safe(person.encode()) + b"\n")
    return len(parent)


def main() -> None:
    timer = Timer()
    scratch_root = Path(SCRATCH)
    if not scratch_root.is_dir():
        scratch_root = Path(tempfile.gettempdir())
    work = Path(tempfile.mkdtemp(prefix="shopping-sessions-", dir=scratch_root))
    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
    con.execute(f"SET temp_directory = {sql_string(str(work / 'duckdb_tmp'))}")
    con.execute("SET preserve_insertion_order = false")
    # DuckDB deliberately keeps empty VARCHAR CSV fields as empty strings. The
    # challenge dialect defines both an unquoted empty field and "" as null.
    con.execute("""
        CREATE MACRO decode_field(value) AS
            CASE WHEN value IS NULL OR value = '' THEN NULL
                 ELSE replace(replace(value, '%0D', chr(13)), '%25', '%') END
    """)
    con.execute("""
        CREATE MACRO parse_ts(value) AS (
            CASE
                WHEN regexp_full_match(value, '[0-9]+') THEN try_cast(value AS BIGINT)
                WHEN regexp_full_match(value, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z')
                    THEN epoch_ms(try_strptime(value, '%Y-%m-%dT%H:%M:%SZ'))
                WHEN regexp_full_match(value, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-]([01][0-9]|2[0-3]):[0-5][0-9]')
                    THEN epoch_ms(try_strptime(substr(value, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                         - (CASE substr(value, 20, 1) WHEN '+' THEN 1 ELSE -1 END)
                           * (try_cast(substr(value, 21, 2) AS BIGINT) * 60
                              + try_cast(substr(value, 24, 2) AS BIGINT)) * 60000
                ELSE NULL
            END
        )
    """)

    prepared = prepare_inputs(work)
    timer.mark("exact_csv")
    links_scan = csv_scan(prepared["links"], {"user_id_a": "VARCHAR", "user_id_b": "VARCHAR"})
    identity_file = work / "identity.csv"
    identities = build_identity_map(con, links_scan, identity_file)
    identity_scan = csv_scan(identity_file, {"user_id": "VARCHAR", "person": "VARCHAR"})
    con.execute(f"CREATE TEMP TABLE identity AS SELECT decode_field(user_id) user_id, decode_field(person) person FROM {identity_scan}")
    timer.mark(f"identity[{identities}]")

    products_scan = csv_scan(prepared["products"], {"product_id": "VARCHAR", "category": "VARCHAR"})
    campaigns_scan = csv_scan(prepared["campaigns"], {"campaign_id": "VARCHAR", "channel": "VARCHAR"})
    con.execute(f"CREATE TEMP TABLE products AS SELECT decode_field(product_id) product_id, decode_field(category) category FROM {products_scan}")
    con.execute(f"CREATE TEMP TABLE campaigns AS SELECT decode_field(campaign_id) campaign_id, decode_field(channel) channel FROM {campaigns_scan}")
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as source:
        manifest = json.load(source)
    window_start, window_end = con.execute("SELECT parse_ts(?), parse_ts(?)", [manifest["window_start"], manifest["window_end"]]).fetchone()
    if window_start is None or window_end is None:
        raise ValueError("invalid manifest window")

    events_scan = csv_scan(prepared["events"], {"event_id": "VARCHAR", "user_id": "VARCHAR", "ts": "VARCHAR", "ingest_ts": "VARCHAR", "event_type": "VARCHAR", "product_id": "VARCHAR", "price_cents": "VARCHAR"})
    con.execute(f"""
        CREATE TEMP TABLE event_kept AS
        SELECT * EXCLUDE (dedup_rank) FROM (
            SELECT decode_field(event_id) event_id, decode_field(user_id) user_id,
                   decode_field(ts) ts_text, decode_field(ingest_ts) ingest_text,
                   decode_field(event_type) event_type, decode_field(product_id) product_id,
                   decode_field(price_cents) price_text,
                   row_number() OVER (PARTITION BY decode_field(event_id) ORDER BY decode_field(ingest_ts)) dedup_rank
            FROM {events_scan}
        ) WHERE dedup_rank = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE valid_events AS
        SELECT coalesce(i.person, e.user_id) person, e.ts_ms, e.event_type,
               CASE WHEN e.event_type = 'purchase' THEN e.price_cents ELSE 0 END price_cents,
               coalesce(p.category, 'unknown') category
        FROM (
            SELECT *, parse_ts(ts_text) ts_ms, parse_ts(ingest_text) ingest_ms,
                   CASE WHEN regexp_full_match(price_text, '[0-9]+') THEN try_cast(price_text AS BIGINT) END price_cents
            FROM event_kept
        ) e
        LEFT JOIN identity i ON i.user_id = e.user_id
        LEFT JOIN products p ON p.product_id = e.product_id
        WHERE e.user_id IS NOT NULL AND e.product_id IS NOT NULL
          AND e.ts_ms >= {window_start} AND e.ts_ms < {window_end}
          AND e.event_type IN ('view', 'cart', 'purchase')
          AND (e.event_type != 'purchase' OR e.price_cents BETWEEN 1 AND 10000000)
          AND e.ingest_ms - e.ts_ms <= {DAY_MS}
    """)
    timer.mark("events_parse_dedup_validate")

    con.execute(f"""
        CREATE TEMP TABLE session_events AS
        WITH distinct_times AS (SELECT DISTINCT person, ts_ms FROM valid_events),
        boundaries AS (
            SELECT *, CASE WHEN lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) IS NULL
                         OR ts_ms - lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) > {GAP_MS}
                       THEN 1 ELSE 0 END begins FROM distinct_times
        ), numbered AS (
            SELECT *, sum(begins) OVER (PARTITION BY person ORDER BY ts_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) session_no
            FROM boundaries
        )
        SELECT e.*, n.session_no FROM valid_events e JOIN numbered n USING (person, ts_ms)
    """)
    con.execute(f"""
        CREATE TEMP TABLE sessions AS
        SELECT person, session_no, min(ts_ms) session_start,
               cast(make_timestamp_ms(min(ts_ms)) AS DATE) AS day,
               count(*) event_count, sum(price_cents) revenue_cents,
               count_if(event_type = 'purchase') > 0 purchasing
        FROM session_events GROUP BY person, session_no
    """)
    timer.mark("sessionize")

    con.execute("""
        CREATE TEMP TABLE user_daily_final AS
        WITH totals AS (
            SELECT person user_id, day, count(*) sessions, sum(event_count) events, sum(revenue_cents) revenue_cents
            FROM sessions GROUP BY person, day
        ), category_revenue AS (
            SELECT s.person user_id, s.day, e.category, sum(e.price_cents) category_cents
            FROM session_events e JOIN sessions s USING (person, session_no)
            WHERE e.event_type = 'purchase' GROUP BY s.person, s.day, e.category
        ), top_categories AS (
            SELECT user_id, day, category top_category FROM category_revenue
            QUALIFY row_number() OVER (PARTITION BY user_id, day ORDER BY category_cents DESC, category ASC) = 1
        )
        SELECT t.user_id, t.day, t.sessions, t.events, t.revenue_cents, c.top_category
        FROM totals t LEFT JOIN top_categories c USING (user_id, day)
    """)
    timer.mark("user_daily")

    clicks_scan = csv_scan(prepared["clicks"], {"click_id": "VARCHAR", "user_id": "VARCHAR", "campaign_id": "VARCHAR", "ts": "VARCHAR"})
    con.execute(f"""
        CREATE TEMP TABLE click_kept AS
        SELECT * EXCLUDE (dedup_rank) FROM (
            SELECT decode_field(click_id) click_id, decode_field(user_id) user_id,
                   decode_field(campaign_id) campaign_id, decode_field(ts) ts_text,
                   row_number() OVER (PARTITION BY decode_field(click_id)) dedup_rank
            FROM {clicks_scan}
        ) WHERE dedup_rank = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE valid_clicks AS
        SELECT coalesce(i.person, c.user_id) person, c.click_id, c.campaign_id, c.ts_ms
        FROM (SELECT *, parse_ts(ts_text) ts_ms FROM click_kept) c
        LEFT JOIN identity i ON i.user_id = c.user_id
        JOIN campaigns k ON k.campaign_id = c.campaign_id
        WHERE c.user_id IS NOT NULL AND c.ts_ms >= {window_start - WEEK_MS} AND c.ts_ms < {window_end}
    """)
    timer.mark("clicks_parse_dedup_validate")

    con.execute(f"""
        CREATE TEMP TABLE click_candidates AS
        SELECT s.person, s.session_no, s.day, c.ts_ms, c.click_id, c.campaign_id
        FROM sessions s JOIN valid_clicks c ON c.person = s.person
          AND c.ts_ms <= s.session_start AND c.ts_ms >= s.session_start - {WEEK_MS}
    """)
    con.execute("""
        CREATE TEMP TABLE attributed_sessions AS
        WITH credited AS (
            SELECT person, session_no, campaign_id FROM click_candidates
            QUALIFY row_number() OVER (PARTITION BY person, session_no ORDER BY ts_ms DESC, click_id ASC) = 1
        )
        SELECT s.*, coalesce(c.campaign_id, 'organic') campaign_id
        FROM sessions s LEFT JOIN credited c USING (person, session_no)
    """)
    con.execute("""
        CREATE TEMP TABLE campaign_daily_final AS
        WITH credited AS (
            SELECT campaign_id, day, count(*) sessions, count_if(purchasing) purchasing_sessions,
                   sum(revenue_cents) revenue_cents
            FROM attributed_sessions GROUP BY campaign_id, day
        ), assisted AS (
            SELECT campaign_id, day, count(*) assisted_sessions
            FROM (SELECT DISTINCT person, session_no, day, campaign_id FROM click_candidates)
            GROUP BY campaign_id, day
        ), combined AS (
            SELECT coalesce(c.campaign_id, a.campaign_id) campaign_id, coalesce(c.day, a.day) AS day,
                   coalesce(c.sessions, 0) sessions, coalesce(c.purchasing_sessions, 0) purchasing_sessions,
                   coalesce(c.revenue_cents, 0) revenue_cents, coalesce(a.assisted_sessions, 0) assisted_sessions
            FROM credited c FULL OUTER JOIN assisted a USING (campaign_id, day)
        )
        SELECT x.campaign_id, CASE WHEN x.campaign_id = 'organic' THEN 'organic' ELSE k.channel END channel,
               x.day, x.sessions, x.purchasing_sessions, x.revenue_cents, x.assisted_sessions
        FROM combined x LEFT JOIN campaigns k USING (campaign_id)
    """)
    timer.mark("attribution_campaign_daily")

    user_out, campaign_out = Path(OUT) / "user_daily", Path(OUT) / "campaign_daily"
    user_out.mkdir(parents=True, exist_ok=True)
    campaign_out.mkdir(parents=True, exist_ok=True)
    # sum() over BIGINT yields HUGEINT, which DuckDB writes to Parquet as DOUBLE: pin every
    # integer column to BIGINT (and day to DATE) at the boundary.
    con.execute(f"""
        COPY (SELECT user_id::VARCHAR AS user_id, day::DATE AS day, sessions::BIGINT AS sessions,
                     events::BIGINT AS events, revenue_cents::BIGINT AS revenue_cents,
                     top_category::VARCHAR AS top_category FROM user_daily_final)
        TO {sql_string(str(user_out / 'part-0.parquet'))} (FORMAT parquet)
    """)
    con.execute(f"""
        COPY (SELECT campaign_id::VARCHAR AS campaign_id, channel::VARCHAR AS channel, day::DATE AS day,
                     sessions::BIGINT AS sessions, purchasing_sessions::BIGINT AS purchasing_sessions,
                     revenue_cents::BIGINT AS revenue_cents, assisted_sessions::BIGINT AS assisted_sessions
              FROM campaign_daily_final)
        TO {sql_string(str(campaign_out / 'part-0.parquet'))} (FORMAT parquet)
    """)
    timer.mark("parquet_write")
    con.close()
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
