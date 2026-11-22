use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrd};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 604_800_000;
const PRICE_MAX: u64 = 10_000_000;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// ------------------------------------------------------------------ hashing

#[inline] fn fmix64(mut x: u64) -> u64 {
    x ^= x >> 33; x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33; x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}
/// Deterministic word-at-a-time hash with a murmur3 finalizer (all output bits mixed).
#[derive(Default)]
struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline] fn finish(&self) -> u64 { fmix64(self.0) }
    #[inline] fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) { let mut b = [0u8; 8]; b[..c.len()].copy_from_slice(c); self.write_u64(u64::from_le_bytes(b)); }
    }
    #[inline] fn write_u8(&mut self, x: u8) { self.write_u64(x as u64) }
    #[inline] fn write_u32(&mut self, x: u32) { self.write_u64(x as u64) }
    #[inline] fn write_usize(&mut self, x: usize) { self.write_u64(x as u64) }
    #[inline] fn write_isize(&mut self, x: isize) { self.write_u64(x as u64) }
    #[inline] fn write_u64(&mut self, x: u64) { self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x517c_c1b7_2722_0a95); }
    #[inline] fn write_u128(&mut self, x: u128) { self.write_u64(x as u64); self.write_u64((x >> 64) as u64); }
}
type DMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;
fn dmap<K, V>(cap: usize) -> DMap<K, V> { DMap::with_capacity_and_hasher(cap, BuildHasherDefault::default()) }
#[inline] fn fast_hash<T: Hash + ?Sized>(value: &T) -> u64 { let mut h = FastHasher::default(); value.hash(&mut h); h.finish() }

const PARTS: usize = 64;
/// partition of an ID: hash bits 40..46, disjoint from hashbrown's bucket (low) and tag (top 7) bits
#[inline] fn part_of(id: Id) -> usize { ((fast_hash(&id) >> 40) as usize) & (PARTS - 1) }

// ------------------------------------------------------------------ IDs
//
// An ID of <= 15 bytes is packed big-endian into the top 15 bytes of a u128 with its length in the
// low byte, so u128 order is byte order (a shorter prefix sorts first) and u128 equality is byte
// equality. A longer ID is interned: low byte 0xFF, the rest an index into an append-only table;
// equality is still exact (one index per distinct byte string) and order falls back to the bytes.
type Id = u128;
const LONG: u128 = 0xFF;
#[inline] fn pack(b: &[u8]) -> Id { let mut buf = [0u8; 16]; buf[..b.len()].copy_from_slice(b); buf[15] = b.len() as u8; u128::from_be_bytes(buf) }
#[inline] fn is_long(id: Id) -> bool { id & 0xFF == LONG }

const ISHARDS: usize = 64;
#[derive(Default)]
struct IShard { map: DMap<Box<[u8]>, u64>, items: Vec<Box<[u8]>> }
struct Ids { shards: Vec<Mutex<IShard>>, any_long: AtomicBool }
impl Ids {
    fn new() -> Self { Ids { shards: (0..ISHARDS).map(|_| Mutex::new(IShard::default())).collect(), any_long: AtomicBool::new(false) } }
    #[inline] fn id(&self, b: &[u8]) -> Id { if b.len() <= 15 { pack(b) } else { self.intern(b, true).unwrap() } }
    /// the ID of `b` if it was ever interned (lookups must not grow the table)
    #[inline] fn find(&self, b: &[u8]) -> Option<Id> { if b.len() <= 15 { Some(pack(b)) } else { self.intern(b, false) } }
    fn intern(&self, b: &[u8], insert: bool) -> Option<Id> {
        let s = (fast_hash(b) as usize) & (ISHARDS - 1);
        let mut g = self.shards[s].lock().unwrap();
        let local = match g.map.get(b) {
            Some(&i) => i,
            None if insert => { let i = g.items.len() as u64; g.items.push(b.into()); g.map.insert(b.into(), i); self.any_long.store(true, AtOrd::Relaxed); i }
            None => return None,
        };
        Some((((local * ISHARDS as u64 + s as u64) as u128) << 8) | LONG)
    }
    fn bytes_into(&self, id: Id, out: &mut Vec<u8>) {
        out.clear();
        if !is_long(id) { let len = (id & 0xFF) as usize; out.extend_from_slice(&id.to_be_bytes()[..len]); return; }
        let g = (id >> 8) as u64; let (s, i) = ((g % ISHARDS as u64) as usize, (g / ISHARDS as u64) as usize);
        out.extend_from_slice(&self.shards[s].lock().unwrap().items[i]);
    }
    fn bytes(&self, id: Id) -> Vec<u8> { let mut v = Vec::new(); self.bytes_into(id, &mut v); v }
    #[inline] fn cmp(&self, a: Id, b: Id) -> Ordering {
        if !is_long(a) && !is_long(b) { a.cmp(&b) } else { self.bytes(a).cmp(&self.bytes(b)) }
    }
    fn has_long(&self) -> bool { self.any_long.load(AtOrd::Relaxed) }
}

// ------------------------------------------------------------------ identity

#[derive(Default)]
struct UnionFind { parent: DMap<Id, Id> }
impl UnionFind {
    fn find(&mut self, key: Id) -> Id {
        let mut root = key;
        loop { match self.parent.get(&root) { Some(&p) if p != root => root = p, _ => break } }
        let mut cur = key;
        while cur != root { let next = self.parent[&cur]; self.parent.insert(cur, root); cur = next; }
        root
    }
    fn union(&mut self, ids: &Ids, a: Id, b: Id) {
        self.parent.entry(a).or_insert(a); self.parent.entry(b).or_insert(b);
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb { let (small, large) = if ids.cmp(ra, rb) == Ordering::Less { (ra, rb) } else { (rb, ra) }; self.parent.insert(large, small); }
    }
    /// point every key straight at its root, so root() can be read-only (and shared by threads)
    fn flatten(&mut self) { let keys: Vec<Id> = self.parent.keys().copied().collect(); for k in keys { self.find(k); } }
    #[inline] fn root(&self, key: Id) -> Id { self.parent.get(&key).copied().unwrap_or(key) }
}

// ------------------------------------------------------------------ records

/// one parsed copy of an event line; validity bits computed from this copy alone
#[derive(Clone, Copy)]
struct EvRec { eid: Id, user: Id, ts: i64, ingest: i64, price: u32, cat: u32, flags: u8, kind: u8 }
/// a surviving event: price (24 bits) | category (32 bits) << 24 | kind << 56
#[derive(Clone, Copy)]
struct Event { person: Id, ts: i64, v: u64 }
impl Event {
    #[inline] fn price(&self) -> i64 { (self.v & 0xff_ffff) as i64 }
    #[inline] fn cat(&self) -> u32 { (self.v >> 24) as u32 }
    #[inline] fn kind(&self) -> u8 { (self.v >> 56) as u8 }
}
#[derive(Clone, Copy)]
struct Click { person: Id, click_id: Id, ts: i64, campaign: u32 }
#[derive(Clone, Copy)]
struct Session { start: i64, day: i32, revenue: i64, purchasing: bool }
struct UserRow { user: Id, day: i32, sessions: i64, events: i64, revenue: i64, top: Option<u32> }
#[derive(Clone, Default)]
struct CampaignAcc { sessions: i64, purchasing: i64, revenue: i64, assisted: i64 }

fn parse_fields<const N: usize>(line: &[u8]) -> Option<[Option<&[u8]>; N]> {
    if line.is_empty() { return None; }
    let mut out: [Option<&[u8]>; N] = [None; N];
    let mut count = 0;
    for raw in line.split(|&b| b == b',') {
        if count == N { return None; }
        let value = if raw.contains(&b'"') {
            if raw.len() < 2 || raw[0] != b'"' || raw[raw.len() - 1] != b'"'
                || raw[1..raw.len() - 1].contains(&b'"') { return None; }
            &raw[1..raw.len() - 1]
        } else { raw };
        out[count] = if value.is_empty() { None } else { Some(value) };
        count += 1;
    }
    (count == N).then_some(out)
}

fn csv_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|x| x.to_str()) == Some("csv") { paths.push(path); }
        }
    }
    paths.sort();
    Ok(paths)
}

fn for_rows(path: &Path, mut f: impl FnMut(&[u8])) -> Result<()> {
    let data = fs::read(path)?;
    let mut start = data.iter().position(|&b| b == b'\n').map_or(data.len(), |i| i + 1);
    while start < data.len() {
        let end = data[start..].iter().position(|&b| b == b'\n').map_or(data.len(), |i| start + i);
        f(&data[start..end]);
        start = if end < data.len() { end + 1 } else { end };
    }
    Ok(())
}

fn digits(bytes: &[u8]) -> bool { !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit) }
fn parse_price(bytes: &[u8]) -> Option<u32> {
    if !digits(bytes) { return None; }
    let significant = bytes.iter().position(|&b| b != b'0').map_or(&bytes[bytes.len()..], |i| &bytes[i..]);
    if significant.len() > 8 { return None; }
    let value = significant.iter().fold(0u32, |a, &b| a * 10 + (b - b'0') as u32);
    (value >= 1 && value <= PRICE_MAX as u32).then_some(value)
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400); let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}
fn parse_ts(b: &[u8]) -> Option<i64> {
    if digits(b) {
        let significant = b.iter().position(|&x| x != b'0').map_or(&b[b.len()..], |i| &b[i..]);
        let mut value = 0i64;
        for &x in significant {
            match value.checked_mul(10).and_then(|v| v.checked_add((x - b'0') as i64)) {
                Some(v) => value = v, None => return Some(i64::MAX),
            }
        }
        return Some(value);
    }
    if b.len() != 20 && b.len() != 25 { return None; }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' { return None; }
    let two = |i: usize| -> Option<i64> {
        let x = &b[i..i + 2];
        digits(x).then(|| ((x[0] - b'0') as i64) * 10 + (x[1] - b'0') as i64)
    };
    if !digits(&b[0..4]) { return None; }
    let y = b[0..4].iter().fold(0i64, |a, &x| a * 10 + (x - b'0') as i64);
    let (m, d, h, mi, s) = (two(5)?, two(8)?, two(11)?, two(14)?, two(17)?);
    if !(1..=12).contains(&m) || h > 23 || mi > 59 || s > 59 { return None; }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [31, if leap {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if d < 1 || d > mdays[m as usize - 1] { return None; }
    let offset = if b.len() == 20 {
        if b[19] != b'Z' { return None; } 0
    } else {
        if !matches!(b[19], b'+' | b'-') || b[22] != b':' { return None; }
        let (oh, om) = (two(20)?, two(23)?); if oh > 23 || om > 59 { return None; }
        let value = (oh * 60 + om) * 60_000; if b[19] == b'-' { -value } else { value }
    };
    Some(days_from_civil(y, m, d) * DAY + ((h * 60 + mi) * 60 + s) * 1000 - offset)
}

fn json_string(data: &[u8], name: &[u8]) -> Option<Vec<u8>> {
    let pos = data.windows(name.len()).position(|w| w == name)? + name.len();
    let rest = &data[pos..]; let colon = rest.iter().position(|&b| b == b':')?;
    let rest = &rest[colon + 1..]; let first = rest.iter().position(|&b| b == b'"')?;
    let rest = &rest[first + 1..]; let last = rest.iter().position(|&b| b == b'"')?;
    Some(rest[..last].to_vec())
}

fn write_batch(path: PathBuf, schema: Arc<Schema>, batch: RecordBatch) -> Result<()> {
    let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    writer.write(&batch)?; writer.close()?; Ok(())
}

//
// Every file is cut into CHUNK-byte ranges. A line belongs to the range holding its first byte; a
// line starts at a byte that follows a '\n' (byte 0 is the header and never a data line). So each
// line is read exactly once, whatever the chunk size or thread count, and the result equals
// for_rows(): no empty trailing line after a final '\n', a last line without '\n' is kept.
const CHUNK: u64 = 8 << 20;
/// SHOPPING_CHUNK overrides the chunk size (tests use tiny chunks to put boundaries everywhere)
fn chunk_size() -> u64 { env::var("SHOPPING_CHUNK").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(CHUNK) }
struct Chunk { file: usize, start: u64, end: u64 }

fn plan_chunks(paths: &[PathBuf]) -> Result<Vec<Chunk>> {
    let mut out = Vec::new();
    let size = chunk_size();
    for (file, p) in paths.iter().enumerate() {
        let len = fs::metadata(p)?.len();
        let mut s = 0;
        while s < len { let e = (s + size).min(len); out.push(Chunk { file, start: s, end: e }); s = e; }
    }
    Ok(out)
}

fn read_at_full(f: &File, buf: &mut Vec<u8>, from: u64, len: usize) -> std::io::Result<()> {
    let old = buf.len(); buf.resize(old + len, 0);
    let mut done = 0;
    while done < len {
        let n = f.read_at(&mut buf[old + done..], from + done as u64)?;
        if n == 0 { buf.truncate(old + done); break; }
        done += n;
    }
    Ok(())
}

fn chunk_rows(f: &File, file_len: u64, c: &Chunk, buf: &mut Vec<u8>, mut row: impl FnMut(&[u8])) -> Result<()> {
    buf.clear();
    let base = if c.start == 0 { 0 } else { c.start - 1 };          // one byte back: was it a '\n'?
    read_at_full(f, buf, base, (c.end - base) as usize)?;
    let first_nl = match buf.iter().position(|&b| b == b'\n') { Some(i) => i, None => return Ok(()) };
    let mut pos = first_nl + 1;                                       // buffer index of a line start
    let owned_end = (c.end - base) as usize;                          // starts < this are ours
    while pos < owned_end {
        match buf[pos..].iter().position(|&b| b == b'\n') {
            Some(i) => { row(&buf[pos..pos + i]); pos += i + 1; }
            None => {
                // the line runs past the chunk: read on until its '\n' or the end of the file
                let mut at = base + buf.len() as u64;
                let scan_from = buf.len();
                let mut nl = None;
                let mut from = scan_from;
                while at < file_len {
                    let want = (64 << 10).min((file_len - at) as usize);
                    let before = buf.len();
                    read_at_full(f, buf, at, want)?;
                    if buf.len() == before { break; }
                    at = base + buf.len() as u64;
                    if let Some(i) = buf[from..].iter().position(|&b| b == b'\n') { nl = Some(from + i); break; }
                    from = buf.len();
                }
                let end = nl.unwrap_or(buf.len());
                if end > pos || nl.is_some() { row(&buf[pos..end]); }
                break;
            }
        }
    }
    Ok(())
}

/// Runs `work(chunk_rows)` over every chunk of `paths` on `threads` threads. Each thread gets its
/// own state from `init` and hands it to `done` at the end.
fn parallel_rows<S: Send>(paths: &[PathBuf], threads: usize, init: impl Fn() -> S + Sync,
                          row: impl Fn(&mut S, &[u8]) + Sync, done: impl Fn(S) + Sync) -> Result<()> {
    let chunks = plan_chunks(paths)?;
    let files: Vec<File> = paths.iter().map(File::open).collect::<std::io::Result<_>>()?;
    let lens: Vec<u64> = files.iter().map(|f| f.metadata().map(|m| m.len())).collect::<std::io::Result<_>>()?;
    let next = AtomicUsize::new(0);
    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                let mut st = init();
                let mut buf = Vec::with_capacity(chunk_size().min(CHUNK) as usize + (64 << 10));
                loop {
                    let i = next.fetch_add(1, AtOrd::Relaxed);
                    if i >= chunks.len() { break; }
                    let c = &chunks[i];
                    if let Err(e) = chunk_rows(&files[c.file], lens[c.file], c, &mut buf, |line| row(&mut st, line)) {
                        errors.lock().unwrap().push(e.to_string()); break;
                    }
                }
                done(st);
            });
        }
    });
    let errors = errors.into_inner().unwrap();
    if let Some(e) = errors.first() { return Err(e.clone().into()); }
    Ok(())
}

/// Runs `f(i, &mut state)` for i in 0..n on `threads` threads, each with its own `init()` state;
/// returns the states.
fn parallel_for<S: Send>(n: usize, threads: usize, init: impl Fn() -> S + Sync, f: impl Fn(usize, &mut S) + Sync) -> Vec<S> {
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let hs: Vec<_> = (0..threads).map(|_| s.spawn(|| {
            let mut st = init();
            loop { let i = next.fetch_add(1, AtOrd::Relaxed); if i >= n { break; } f(i, &mut st); }
            st
        })).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}


fn take_all<T>(m: &Mutex<Vec<Vec<T>>>) -> Vec<T> {
    let vs = std::mem::take(&mut *m.lock().unwrap());
    if vs.len() == 1 { return vs.into_iter().next().unwrap(); }
    let n = vs.iter().map(Vec::len).sum(); let mut out = Vec::with_capacity(n); for v in vs { out.extend(v); } out
}

fn main() -> Result<()> {
    let total = Instant::now(); let mut last = total;
    let mark = |name: &str, last: &mut Instant| { let now = Instant::now(); eprintln!("stage {name}: {:.6}s (total {:.6}s)", now.duration_since(*last).as_secs_f64(), now.duration_since(total).as_secs_f64()); *last = now; };
    let args: Vec<String> = env::args().collect();
    let data = PathBuf::from(env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or_else(|| "/data".into()));
    let output = PathBuf::from(env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or_else(|| "/output".into()));
    let threads = env::var("SHOPPING_THREADS").ok().and_then(|v| v.parse::<usize>().ok())
        .or_else(|| thread::available_parallelism().ok().map(|n| n.get())).unwrap_or(1).clamp(1, 64);
    eprintln!("threads {threads}");
    let manifest = fs::read(data.join("manifest.json"))?;
    let ws = parse_ts(&json_string(&manifest, b"window_start").ok_or("missing window_start")?).ok_or("bad window_start")?;
    let we = parse_ts(&json_string(&manifest, b"window_end").ok_or("missing window_end")?).ok_or("bad window_end")?;
    let ids = Ids::new(); let ids = &ids;

    // ---- lookups
    let mut categories: Vec<Vec<u8>> = Vec::new(); let mut category_index: DMap<Vec<u8>, u32> = dmap(64); let mut products: DMap<Id, u32> = dmap(1 << 16);
    for_rows(&data.join("products.csv"), |line| if let Some(f) = parse_fields::<2>(line) { if let (Some(p), Some(c)) = (f[0], f[1]) {
        let idx = match category_index.get(c) { Some(&i) => i, None => { let i = u32::try_from(categories.len()).expect("too many categories"); categories.push(c.to_vec()); category_index.insert(c.to_vec(), i); i } };
        products.insert(ids.id(p), idx); } })?;
    let unknown = u32::try_from(categories.len()).map_err(|_| "too many categories")?; categories.push(b"unknown".to_vec());
    let mut campaigns: DMap<Id, u32> = dmap(4096); let mut campaign_ids: Vec<Vec<u8>> = Vec::new(); let mut channels: Vec<Vec<u8>> = Vec::new();
    for_rows(&data.join("campaigns.csv"), |line| if let Some(f) = parse_fields::<2>(line) { if let (Some(id), Some(ch)) = (f[0], f[1]) {
        let idx = u32::try_from(campaign_ids.len()).expect("too many campaigns"); campaigns.insert(ids.id(id), idx); campaign_ids.push(id.to_vec()); channels.push(ch.to_vec()); } })?;
    if campaign_ids.len() >= u32::MAX as usize { return Err("too many campaigns".into()); }
    mark("lookups", &mut last);

    // ---- identity
    let mut uf = UnionFind::default();
    for path in csv_files(&data.join("links"))? { for_rows(&path, |line| if let Some(f) = parse_fields::<2>(line) { if let (Some(a), Some(b)) = (f[0], f[1]) { uf.union(ids, ids.id(a), ids.id(b)); } })?; }
    uf.flatten(); let uf = &uf;
    mark("identity", &mut last);

    // ---- events 1: parse every copy into thread-local partitions by event_id, no locks. Only
    // malformed lines (and lines with no event_id) are dropped here (rules.md events 1)
    let event_paths = csv_files(&data.join("events"))?;
    let event_bytes: u64 = event_paths.iter().map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0)).sum();
    let per_part = ((event_bytes / 70) as usize / (threads * PARTS)) + 64;
    let products_r = &products;
    let parsed: Mutex<Vec<Vec<Vec<EvRec>>>> = Mutex::new(Vec::new());
    parallel_rows(&event_paths, threads,
        || (0..PARTS).map(|_| Vec::with_capacity(per_part)).collect::<Vec<Vec<EvRec>>>(),
        |local, line| {
            let Some(f) = parse_fields::<7>(line) else { return; }; let Some(eid) = f[0] else { return; };
            let ingest = f[3].and_then(parse_ts).unwrap_or(i64::MAX);
            let kind = match f[4] { Some(b"view") => 1, Some(b"cart") => 2, Some(b"purchase") => 3, _ => 0 };
            let mut flags = 0u8;
            let user = if let Some(u) = f[1] { flags |= 1; ids.id(u) } else { 0 };
            let ts = if let Some(t) = f[2].and_then(parse_ts) { flags |= 2; t } else { i64::MAX };
            if kind != 0 { flags |= 4; }
            let cat = if let Some(p) = f[5] { flags |= 8; ids.find(p).and_then(|k| products_r.get(&k).copied()).unwrap_or(unknown) } else { unknown };
            let price = if kind == 3 { if let Some(p) = f[6].and_then(parse_price) { flags |= 16; p } else { 0 } } else { flags |= 16; 0 };
            let eid = ids.id(eid);
            local[part_of(eid)].push(EvRec { eid, user, ts, ingest, price, cat, flags, kind });
        },
        |local| parsed.lock().unwrap().push(local))?;
    let by_part: Vec<Mutex<Vec<Vec<EvRec>>>> = (0..PARTS).map(|_| Mutex::new(Vec::new())).collect();
    for t in parsed.into_inner().unwrap() { for (p, v) in t.into_iter().enumerate() { by_part[p].lock().unwrap().push(v); } }
    mark("events_parse", &mut last);

    // ---- events 2-4, per event_id partition: keep the min-ingest copy of each event_id (invalid
    // copies compete too), then validate it and route survivors to their person's partition
    let ev_by_person: Vec<Mutex<Vec<Vec<Event>>>> = (0..PARTS).map(|_| Mutex::new(Vec::new())).collect();
    parallel_for(PARTS, threads, || (), |p, _| {
        let srcs = std::mem::take(&mut *by_part[p].lock().unwrap());
        let n: usize = srcs.iter().map(Vec::len).sum();
        let mut best: DMap<Id, (u32, u32)> = dmap(n);
        for (si, src) in srcs.iter().enumerate() {
            for (ri, r) in src.iter().enumerate() {
                match best.entry(r.eid) {
                    std::collections::hash_map::Entry::Occupied(mut o) => { let (a, b) = *o.get(); if r.ingest < srcs[a as usize][b as usize].ingest { o.insert((si as u32, ri as u32)); } }
                    std::collections::hash_map::Entry::Vacant(v) => { v.insert((si as u32, ri as u32)); }
                }
            }
        }
        let mut out: Vec<Vec<Event>> = (0..PARTS).map(|_| Vec::new()).collect();
        for &(a, b) in best.values() {
            let r = &srcs[a as usize][b as usize];
            if r.flags != 31 || r.ts < ws || r.ts >= we { continue; }
            if r.ingest.saturating_sub(r.ts) > DAY { continue; }
            let person = uf.root(r.user);
            out[part_of(person)].push(Event { person, ts: r.ts, v: r.price as u64 | (r.cat as u64) << 24 | (r.kind as u64) << 56 });
        }
        drop(best); drop(srcs);
        for (q, v) in out.into_iter().enumerate() { if !v.is_empty() { ev_by_person[q].lock().unwrap().push(v); } }
    });
    drop(by_part);
    mark("events_dedup_validate", &mut last);

    // ---- clicks: copies are identical (rules.md clicks 2), so validating each copy and removing
    // equal click_ids afterwards equals dedup-then-validate. A click's copies share user, person
    // and ts, so they land in one partition and sort next to each other.
    let click_paths = csv_files(&data.join("clicks"))?;
    let campaigns_r = &campaigns;
    let cparsed: Mutex<Vec<Vec<Vec<Click>>>> = Mutex::new(Vec::new());
    parallel_rows(&click_paths, threads,
        || (0..PARTS).map(|_| Vec::new()).collect::<Vec<Vec<Click>>>(),
        |local, line| {
            let Some(f) = parse_fields::<4>(line) else { return; };
            let (Some(id), Some(user), Some(camp), Some(tsb)) = (f[0], f[1], f[2], f[3]) else { return; };
            let Some(ts) = parse_ts(tsb) else { return; };
            if ts < ws - WEEK || ts >= we { return; }
            let Some(&campaign) = ids.find(camp).and_then(|k| campaigns_r.get(&k)) else { return; };
            let person = uf.root(ids.id(user));
            local[part_of(person)].push(Click { person, click_id: ids.id(id), ts, campaign });
        },
        |local| cparsed.lock().unwrap().push(local))?;
    let cl_by_person: Vec<Mutex<Vec<Vec<Click>>>> = (0..PARTS).map(|_| Mutex::new(Vec::new())).collect();
    for t in cparsed.into_inner().unwrap() { for (q, v) in t.into_iter().enumerate() { if !v.is_empty() { cl_by_person[q].lock().unwrap().push(v); } } }
    mark("clicks_parse_validate", &mut last);

    // ---- per person partition: sessionize, user_daily rows, credit and assists
    let min_day = ws.div_euclid(DAY) as i32; let max_day = (we - 1).div_euclid(DAY) as i32; let nday = (max_day - min_day + 1) as usize;
    let organic = u32::MAX; let ncamp = campaign_ids.len();
    let ai = |c: u32, d: i32| (if c == organic { ncamp } else { c as usize }) * nday + (d - min_day) as usize;
    let categories_r = &categories;
    let long = ids.has_long();
    let states = parallel_for(PARTS, threads,
        || (vec![CampaignAcc::default(); (ncamp + 1) * nday], Vec::<UserRow>::new(), Scratch::default()),
        |q, (acc, user_rows, sc)| {
            let mut events = take_all(&ev_by_person[q]);
            if long { events.sort_unstable_by(|a, b| ids.cmp(a.person, b.person).then(a.ts.cmp(&b.ts))); }
            else { events.sort_unstable_by_key(|e| (e.person, e.ts)); }
            let mut sessions = Vec::new(); let mut people = Vec::new();
            sessionize(&events, categories_r, user_rows, &mut sessions, &mut people, sc);
            drop(events);
            let mut clicks = take_all(&cl_by_person[q]);
            if long { clicks.sort_unstable_by(|a, b| ids.cmp(a.person, b.person).then(a.ts.cmp(&b.ts)).then(ids.cmp(a.click_id, b.click_id))); }
            else { clicks.sort_unstable_by_key(|c| (c.person, c.ts, c.click_id)); }
            clicks.dedup_by(|a, b| a.click_id == b.click_id);
            attribute(ids, &people, &sessions, &clicks, acc, &ai, organic, sc);
        });
    mark("sessionize_attribute", &mut last);
    let mut acc = vec![CampaignAcc::default(); (ncamp + 1) * nday];
    let mut user_rows = Vec::new();
    for (a, rows, _) in states { for (x, y) in acc.iter_mut().zip(a) { x.sessions += y.sessions; x.purchasing += y.purchasing; x.revenue += y.revenue; x.assisted += y.assisted; } user_rows.extend(rows); }
    if long { user_rows.sort_unstable_by(|a, b| ids.cmp(a.user, b.user).then(a.day.cmp(&b.day))); } else { user_rows.sort_unstable_by_key(|r| (r.user, r.day)); }
    let mut campaign_rows = Vec::new();
    for c in 0..ncamp as u32 { for d in min_day..=max_day { let a = acc[ai(c, d)].clone(); if a.sessions > 0 || a.assisted > 0 { campaign_rows.push((c, d, a)); } } }
    for d in min_day..=max_day { let a = acc[ai(organic, d)].clone(); if a.sessions > 0 { campaign_rows.push((organic, d, a)); } }
    let cname = |c: u32| if c == organic { b"organic".as_slice() } else { campaign_ids[c as usize].as_slice() };
    campaign_rows.sort_unstable_by(|a, b| cname(a.0).cmp(cname(b.0)).then(a.1.cmp(&b.1)));
    mark("merge_sort", &mut last);
    write_user(ids, &output.join("user_daily"), &user_rows, &categories)?;
    write_campaign(&output.join("campaign_daily"), &campaign_rows, &campaign_ids, &channels)?;
    mark("parquet_write", &mut last);
    Ok(())
}

#[derive(Default)]
struct Scratch { cat: Vec<(u32, i64)>, collapsed: Vec<(i64, u32)>, by: Vec<(u32, i64)> }

/// events sorted by (person, ts) -> sessions (flat, in person order) and per person ranges
/// (person, first session, end); appends user_daily rows (rules.md events 5-7, user_daily)
fn sessionize(events: &[Event], categories: &[Vec<u8>], user_rows: &mut Vec<UserRow>, sessions: &mut Vec<Session>, people: &mut Vec<(Id, usize, usize)>, sc: &mut Scratch) {
    let mut i = 0;
    while i < events.len() {
        let person = events[i].person; let mut end = i + 1; while end < events.len() && events[end].person == person { end += 1; }
        let first = sessions.len(); let mut day = None; let (mut ds, mut de, mut dr) = (0i64, 0i64, 0i64); sc.cat.clear();
        let mut j = i;
        while j < end {
            let start = events[j].ts; let mut k = j + 1; while k < end && events[k].ts - events[k - 1].ts <= GAP { k += 1; }
            let sd = start.div_euclid(DAY) as i32;
            if day != Some(sd) { if let Some(d) = day { flush_user(person, d, ds, de, dr, &mut sc.cat, categories, user_rows); } day = Some(sd); ds = 0; de = 0; dr = 0; }
            let (mut rev, mut purchasing) = (0i64, false);
            for e in &events[j..k] { rev += e.price(); if e.kind() == 3 { purchasing = true; sc.cat.push((e.cat(), e.price())); } }
            sessions.push(Session { start, day: sd, revenue: rev, purchasing });
            ds += 1; de += (k - j) as i64; dr += rev; j = k;
        }
        if let Some(d) = day { flush_user(person, d, ds, de, dr, &mut sc.cat, categories, user_rows); }
        people.push((person, first, sessions.len())); i = end;
    }
}

/// top_category: highest summed price; a tie goes to the byte-wise smallest name; none without purchases
fn flush_user(person: Id, day: i32, sessions: i64, events: i64, revenue: i64, cat: &mut Vec<(u32, i64)>, categories: &[Vec<u8>], rows: &mut Vec<UserRow>) {
    let mut top: Option<(u32, i64)> = None;
    if !cat.is_empty() {
        cat.sort_unstable_by_key(|x| x.0);
        let mut i = 0;
        while i < cat.len() {
            let c = cat[i].0; let mut s = 0i64; while i < cat.len() && cat[i].0 == c { s += cat[i].1; i += 1; }
            top = match top { None => Some((c, s)), Some((bc, bs)) => if s > bs || (s == bs && categories[c as usize] < categories[bc as usize]) { Some((c, s)) } else { Some((bc, bs)) } };
        }
    }
    rows.push(UserRow { user: person, day, sessions, events, revenue, top: top.filter(|t| t.1 > 0).map(|t| t.0) });
    cat.clear();
}

fn lower_bound(s: &[Session], v: i64) -> usize { s.partition_point(|x| x.start < v) }
fn upper_bound(s: &[Session], v: i64) -> usize { s.partition_point(|x| x.start <= v) }

/// people and clicks in the same person order, clicks by (person, ts, click_id) and deduped.
/// Credit: the latest click at or before the session start and within 7 days; a tie in ts goes to
/// the smallest click_id (the first of its ts after the sort). Assists: each campaign once per
/// session with any of its clicks in [start - 7 d, start], via merged [t, t + 7 d] islands.
#[allow(clippy::too_many_arguments)]
fn attribute<F: Fn(u32, i32) -> usize>(ids: &Ids, people: &[(Id, usize, usize)], sessions: &[Session], clicks: &[Click], acc: &mut [CampaignAcc], ai: &F, organic: u32, sc: &mut Scratch) {
    let mut ci = 0;
    for &(person, s0, s1) in people {
        let ps = &sessions[s0..s1];
        while ci < clicks.len() && clicks[ci].person != person && ids.cmp(clicks[ci].person, person) == Ordering::Less { ci += 1; }
        let cs = ci; while ci < clicks.len() && clicks[ci].person == person { ci += 1; }
        let pc = &clicks[cs..ci];
        sc.collapsed.clear();
        let mut x = 0; while x < pc.len() { sc.collapsed.push((pc[x].ts, pc[x].campaign)); let t = pc[x].ts; x += 1; while x < pc.len() && pc[x].ts == t { x += 1; } }
        let mut cp = 0;
        for s in ps {
            while cp < sc.collapsed.len() && sc.collapsed[cp].0 <= s.start { cp += 1; }
            let c = if cp > 0 && s.start - sc.collapsed[cp - 1].0 <= WEEK { sc.collapsed[cp - 1].1 } else { organic };
            let a = &mut acc[ai(c, s.day)]; a.sessions += 1; a.purchasing += s.purchasing as i64; a.revenue += s.revenue;
        }
        if pc.is_empty() { continue; }
        sc.by.clear(); sc.by.extend(pc.iter().map(|c| (c.campaign, c.ts))); sc.by.sort_unstable();
        let mut g = 0;
        while g < sc.by.len() {
            let c = sc.by[g].0; let (mut a, mut b) = (sc.by[g].1, sc.by[g].1 + WEEK); g += 1;
            while g < sc.by.len() && sc.by[g].0 == c {
                let t = sc.by[g].1;
                if t > b { assist_range(ps, a, b, c, acc, ai); a = t; b = t + WEEK; } else if t + WEEK > b { b = t + WEEK; }
                g += 1;
            }
            assist_range(ps, a, b, c, acc, ai);
        }
    }
}
fn assist_range<F: Fn(u32, i32) -> usize>(sessions: &[Session], a: i64, b: i64, c: u32, acc: &mut [CampaignAcc], idx: &F) {
    for s in &sessions[lower_bound(sessions, a)..upper_bound(sessions, b)] { acc[idx(c, s.day)].assisted += 1; }
}

// ------------------------------------------------------------------ output

fn id_column(ids: &Ids, rows: &[UserRow]) -> Result<ArrayRef> {
    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 12); let mut buf = Vec::with_capacity(64);
    for r in rows { ids.bytes_into(r.user, &mut buf); b.append_value(std::str::from_utf8(&buf)?); }
    Ok(Arc::new(b.finish()))
}
fn str_column<'a>(n: usize, vals: impl Iterator<Item = Option<&'a [u8]>>) -> Result<ArrayRef> {
    let mut b = StringBuilder::with_capacity(n, n * 8);
    for v in vals { match v { Some(s) => b.append_value(std::str::from_utf8(s)?), None => b.append_null() } }
    Ok(Arc::new(b.finish()))
}
fn write_user(ids: &Ids, path: &Path, rows: &[UserRow], categories: &[Vec<u8>]) -> Result<()> {
    fs::create_dir_all(path)?;
    let schema = Arc::new(Schema::new(vec![Field::new("user_id", DataType::Utf8, false), Field::new("day", DataType::Date32, false), Field::new("sessions", DataType::Int64, false), Field::new("events", DataType::Int64, false), Field::new("revenue_cents", DataType::Int64, false), Field::new("top_category", DataType::Utf8, true)]));
    let arrays: Vec<ArrayRef> = vec![id_column(ids, rows)?, Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.day))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.sessions))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.events))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.revenue))), str_column(rows.len(), rows.iter().map(|r| r.top.map(|i| categories[i as usize].as_slice())))?];
    write_batch(path.join("part-0.parquet"), schema.clone(), RecordBatch::try_new(schema, arrays)?)
}
fn write_campaign(path: &Path, rows: &[(u32, i32, CampaignAcc)], ids: &[Vec<u8>], channels: &[Vec<u8>]) -> Result<()> {
    fs::create_dir_all(path)?; let organic = u32::MAX;
    let schema = Arc::new(Schema::new(vec![Field::new("campaign_id", DataType::Utf8, false), Field::new("channel", DataType::Utf8, false), Field::new("day", DataType::Date32, false), Field::new("sessions", DataType::Int64, false), Field::new("purchasing_sessions", DataType::Int64, false), Field::new("revenue_cents", DataType::Int64, false), Field::new("assisted_sessions", DataType::Int64, false)]));
    let arrays: Vec<ArrayRef> = vec![
        str_column(rows.len(), rows.iter().map(|(c, _, _)| Some(if *c == organic { b"organic".as_slice() } else { ids[*c as usize].as_slice() })))?,
        str_column(rows.len(), rows.iter().map(|(c, _, _)| Some(if *c == organic { b"organic".as_slice() } else { channels[*c as usize].as_slice() })))?,
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.1))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2.sessions))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2.purchasing))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2.revenue))), Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2.assisted)))];
    write_batch(path.join("part-0.parquet"), schema.clone(), RecordBatch::try_new(schema, arrays)?)
}
