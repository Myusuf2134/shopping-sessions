use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 604_800_000;
const PRICE_MAX: u64 = 10_000_000;

/// Deterministic word-at-a-time hash (Fx-style) with a murmur3 finalizer, so every output bit is
/// mixed: partitions take bits 40..46 while hashbrown takes the low bits and the top 7.
#[derive(Default)]
struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline] fn finish(&self) -> u64 {
        let mut x = self.0;
        x ^= x >> 33; x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33; x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^ (x >> 33)
    }
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
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[inline]
fn fast_hash<T: Hash>(value: &T) -> u64 {
    let mut h = FastHasher::default();
    value.hash(&mut h);
    h.finish()
}
const PARTS: usize = 64;
#[inline] fn part_of<T: Hash>(value: &T) -> usize { ((fast_hash(value) >> 40) as usize) & (PARTS - 1) }

#[derive(Clone, Eq, PartialEq, Hash)]
enum Key { Short(u128), Long(Arc<[u8]>) }

impl Key {
    fn new(bytes: &[u8]) -> Self {
        if bytes.len() <= 15 {
            let mut packed = bytes.len() as u128;
            for (i, &b) in bytes.iter().enumerate() { packed |= (b as u128) << (120 - i * 8); }
            Self::Short(packed)
        } else { Self::Long(Arc::from(bytes)) }
    }
    fn to_vec(&self) -> Vec<u8> {
        match self {
            Self::Short(v) => {
                let len = (*v & 0xff) as usize;
                (0..len).map(|i| (v >> (120 - i * 8)) as u8).collect()
            }
            Self::Long(v) => v.to_vec(),
        }
    }
    fn cmp_bytes(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Short(a), Self::Short(b)) => a.cmp(b),
            _ => self.to_vec().cmp(&other.to_vec()),
        }
    }
}
impl Ord for Key { fn cmp(&self, other: &Self) -> Ordering { self.cmp_bytes(other) } }
impl PartialOrd for Key { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }

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

#[derive(Default)]
struct UnionFind { parent: DMap<Key, Key> }
impl UnionFind {
    fn add(&mut self, key: Key) { self.parent.entry(key.clone()).or_insert(key); }
    fn find(&mut self, key: &Key) -> Key {
        if !self.parent.contains_key(key) { return key.clone(); }
        let mut root = key.clone();
        loop { let p = self.parent.get(&root).unwrap().clone(); if p == root { break; } root = p; }
        let mut cur = key.clone();
        while cur != root { let next = self.parent.get(&cur).unwrap().clone(); self.parent.insert(cur, root.clone()); cur = next; }
        root
    }
    fn union(&mut self, a: Key, b: Key) {
        self.add(a.clone()); self.add(b.clone()); let ra = self.find(&a); let rb = self.find(&b);
        if ra != rb { let (small, large) = if ra < rb {(ra,rb)} else {(rb,ra)}; self.parent.insert(large, small); }
    }
    /// point every key straight at its root, so root() can be read-only (and shared by threads)
    fn flatten(&mut self) { let keys: Vec<Key> = self.parent.keys().cloned().collect(); for k in keys { self.find(&k); } }
    fn root(&self, key: &Key) -> Key { self.parent.get(key).cloned().unwrap_or_else(|| key.clone()) }
}

#[derive(Clone)]
struct EventRec { user: Key, ingest: i64, ts: i64, price: u32, category: u32, flags: u8, kind: u8 }
#[derive(Clone)]
struct Event { person: Key, ts: i64, kind: u8, category: u32, price: i64 }
#[derive(Clone)]
struct Click { person: Key, ts: i64, click_id: Key, campaign: u32 }
#[derive(Clone)]
struct Session { start: i64, day: i32, events: i64, revenue: i64, purchasing: bool }
struct PersonSessions { person: Key, sessions: Vec<Session> }
struct UserRow { user: Key, day: i32, sessions: i64, events: i64, revenue: i64, top: Option<u32> }
#[derive(Clone, Default)]
struct CampaignAcc { sessions: i64, purchasing: i64, revenue: i64, assisted: i64 }

fn key_string(key: &Key) -> Result<String> { Ok(String::from_utf8(key.to_vec())?) }
fn bytes_string(bytes: &[u8]) -> Result<String> { Ok(String::from_utf8(bytes.to_vec())?) }
fn write_batch(path: PathBuf, schema: Arc<Schema>, batch: RecordBatch) -> Result<()> {
    let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    writer.write(&batch)?; writer.close()?; Ok(())
}
fn write_user(path: &Path, rows: &[UserRow], categories: &[Vec<u8>]) -> Result<()> {
    fs::create_dir_all(path)?;
    let user: Vec<String> = rows.iter().map(|r| key_string(&r.user)).collect::<Result<_>>()?;
    let top: Vec<Option<String>> = rows.iter().map(|r| r.top.map(|i| bytes_string(&categories[i as usize])).transpose()).collect::<Result<_>>()?;
    let schema = Arc::new(Schema::new(vec![Field::new("user_id",DataType::Utf8,false),Field::new("day",DataType::Date32,false),Field::new("sessions",DataType::Int64,false),Field::new("events",DataType::Int64,false),Field::new("revenue_cents",DataType::Int64,false),Field::new("top_category",DataType::Utf8,true)]));
    let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(user)),Arc::new(Date32Array::from(rows.iter().map(|r|r.day).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.sessions).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.events).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.revenue).collect::<Vec<_>>())),Arc::new(StringArray::from(top))];
    write_batch(path.join("part-0.parquet"),schema.clone(),RecordBatch::try_new(schema,arrays)?)
}
fn write_campaign(path: &Path, rows: &[(u32,i32,CampaignAcc)], ids: &[Vec<u8>], channels: &[Vec<u8>]) -> Result<()> {
    fs::create_dir_all(path)?; let organic=u32::MAX;
    let cid:Vec<String>=rows.iter().map(|(c,_,_)|if *c==organic{Ok("organic".into())}else{bytes_string(&ids[*c as usize])}).collect::<Result<_>>()?;
    let chan:Vec<String>=rows.iter().map(|(c,_,_)|if *c==organic{Ok("organic".into())}else{bytes_string(&channels[*c as usize])}).collect::<Result<_>>()?;
    let schema=Arc::new(Schema::new(vec![Field::new("campaign_id",DataType::Utf8,false),Field::new("channel",DataType::Utf8,false),Field::new("day",DataType::Date32,false),Field::new("sessions",DataType::Int64,false),Field::new("purchasing_sessions",DataType::Int64,false),Field::new("revenue_cents",DataType::Int64,false),Field::new("assisted_sessions",DataType::Int64,false)]));
    let arrays:Vec<ArrayRef>=vec![Arc::new(StringArray::from(cid)),Arc::new(StringArray::from(chan)),Arc::new(Date32Array::from(rows.iter().map(|r|r.1).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.2.sessions).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.2.purchasing).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.2.revenue).collect::<Vec<_>>())),Arc::new(Int64Array::from(rows.iter().map(|r|r.2.assisted).collect::<Vec<_>>()))];
    write_batch(path.join("part-0.parquet"),schema.clone(),RecordBatch::try_new(schema,arrays)?)
}
fn lower_bound(s:&[Session],v:i64)->usize{s.partition_point(|x|x.start<v)}
fn upper_bound(s:&[Session],v:i64)->usize{s.partition_point(|x|x.start<=v)}

// ------------------------------------------------------------------ parallel line reading
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

struct EvPart { index: DMap<Key, u32>, recs: Vec<EventRec> }
const BATCH: usize = 1024;

fn main()->Result<()> {
    let total=Instant::now(); let mut last=total;
    let mark=|name:&str,last:&mut Instant|{let now=Instant::now();eprintln!("stage {name}: {:.6}s (total {:.6}s)",now.duration_since(*last).as_secs_f64(),now.duration_since(total).as_secs_f64());*last=now;};
    let args:Vec<String>=env::args().collect();
    let data=PathBuf::from(env::var("DATA_DIR").ok().or_else(||args.get(1).cloned()).unwrap_or_else(||"/data".into()));
    let output=PathBuf::from(env::var("OUTPUT_DIR").ok().or_else(||args.get(2).cloned()).unwrap_or_else(||"/output".into()));
    let threads = env::var("SHOPPING_THREADS").ok().and_then(|v| v.parse::<usize>().ok())
        .or_else(|| thread::available_parallelism().ok().map(|n| n.get())).unwrap_or(1).clamp(1, 64);
    eprintln!("threads {threads}");
    let manifest=fs::read(data.join("manifest.json"))?;
    let ws=parse_ts(&json_string(&manifest,b"window_start").ok_or("missing window_start")?).ok_or("bad window_start")?;
    let we=parse_ts(&json_string(&manifest,b"window_end").ok_or("missing window_end")?).ok_or("bad window_end")?;
    let mut categories=Vec::new();let mut category_index:DMap<Key,u32>=DMap::default();let mut products:DMap<Key,u32>=DMap::default();
    for_rows(&data.join("products.csv"),|line|if let Some(f)=parse_fields::<2>(line){if let(Some(p),Some(c))=(f[0],f[1]){let ck=Key::new(c);let idx=*category_index.entry(ck).or_insert_with(||{categories.push(c.to_vec());u32::try_from(categories.len()-1).expect("too many categories")});products.insert(Key::new(p),idx);}})?;
    let unknown=u32::try_from(categories.len()).map_err(|_|"too many categories")?;categories.push(b"unknown".to_vec());
    let mut campaigns:DMap<Key,u32>=DMap::default();let mut campaign_ids=Vec::new();let mut channels=Vec::new();
    for_rows(&data.join("campaigns.csv"),|line|if let Some(f)=parse_fields::<2>(line){if let(Some(id),Some(ch))=(f[0],f[1]){let idx=u32::try_from(campaign_ids.len()).expect("too many campaigns");campaigns.insert(Key::new(id),idx);campaign_ids.push(id.to_vec());channels.push(ch.to_vec());}})?;
    if campaign_ids.len() >= u32::MAX as usize { return Err("too many campaigns".into()); }
    mark("lookups",&mut last);
    let mut uf=UnionFind::default();for path in csv_files(&data.join("links"))?{for_rows(&path,|line|if let Some(f)=parse_fields::<2>(line){if let(Some(a),Some(b))=(f[0],f[1]){uf.union(Key::new(a),Key::new(b));}})?;}
    uf.flatten(); let uf=&uf;
    mark("identity",&mut last);

    // ---- events: parse every copy (malformed lines only are dropped), dedup on event_id keeping the
    // min ingest_ts copy even when it is invalid, then validate the kept copy (rules.md events 1-4)
    let event_paths=csv_files(&data.join("events"))?;
    let event_bytes:u64=event_paths.iter().map(|p|fs::metadata(p).map(|m|m.len()).unwrap_or(0)).sum();
    let estimate=(event_bytes/72).max(1024)as usize;
    let per=estimate/PARTS+1024;
    let ev_parts:Vec<Mutex<EvPart>>=(0..PARTS).map(|_|Mutex::new(EvPart{index:DMap::with_capacity_and_hasher(per,BuildHasherDefault::default()),recs:Vec::with_capacity(per)})).collect();
    let merge=|p:usize,batch:&mut Vec<(Key,EventRec)>|{
        let mut g=ev_parts[p].lock().unwrap(); let EvPart{index,recs}=&mut *g;
        for (key,rec) in batch.drain(..) {
            if let Some(&idx)=index.get(&key){let old=&mut recs[idx as usize];if rec.ingest<old.ingest{*old=rec;}}
            else{let idx=u32::try_from(recs.len()).expect("too many events in partition");index.insert(key,idx);recs.push(rec);}
        }
    };
    let (products_r, merge_r) = (&products, &merge);
    parallel_rows(&event_paths, threads,
        || (0..PARTS).map(|_| Vec::with_capacity(BATCH)).collect::<Vec<Vec<(Key,EventRec)>>>(),
        |local, line| {
            let Some(f)=parse_fields::<7>(line)else{return;};let Some(eid)=f[0]else{return;};
            let ingest=f[3].and_then(parse_ts).unwrap_or(i64::MAX);
            let kind=match f[4]{Some(b"view")=>1,Some(b"cart")=>2,Some(b"purchase")=>3,_=>0};
            let mut flags=0u8;
            let user=if let Some(u)=f[1]{flags|=1;Key::new(u)}else{Key::Short(0)};
            let ts=if let Some(t)=f[2].and_then(parse_ts){flags|=2;t}else{i64::MAX};
            if kind!=0{flags|=4;}
            let category=if let Some(p)=f[5]{flags|=8;products_r.get(&Key::new(p)).copied().unwrap_or(unknown)}else{unknown};
            let price=if kind==3{if let Some(p)=f[6].and_then(parse_price){flags|=16;p}else{0}}else{flags|=16;0};
            let key=Key::new(eid); let p=part_of(&key);
            local[p].push((key,EventRec{user,ingest,ts,price,category,flags,kind}));
            if local[p].len()>=BATCH { merge_r(p,&mut local[p]); }
        },
        |mut local| for (p,b) in local.iter_mut().enumerate() { if !b.is_empty() { merge_r(p,b); } })?;
    mark("events_parse_dedup",&mut last);

    // validate the kept copies and route each surviving event to its person's partition
    let ev_by_person:Vec<Mutex<Vec<Vec<Event>>>>=(0..PARTS).map(|_|Mutex::new(Vec::new())).collect();
    parallel_for(PARTS, threads, || (), |p, _| {
        let part = std::mem::replace(&mut *ev_parts[p].lock().unwrap(), EvPart{index:DMap::default(),recs:Vec::new()});
        let EvPart{index,recs}=part; drop(index);
        let mut out:Vec<Vec<Event>>=(0..PARTS).map(|_|Vec::new()).collect();
        for rec in recs {
            if rec.flags!=31||rec.ts<ws||rec.ts>=we{continue;}
            if rec.ingest.saturating_sub(rec.ts)>DAY{continue;}
            let person=uf.root(&rec.user); let q=part_of(&person);
            out[q].push(Event{person,ts:rec.ts,kind:rec.kind,category:rec.category,price:rec.price as i64});
        }
        for (q,v) in out.into_iter().enumerate() { if !v.is_empty() { ev_by_person[q].lock().unwrap().push(v); } }
    });
    drop(ev_parts);
    mark("events_validate",&mut last);

    // ---- clicks: copies are identical (rules.md clicks 2), so validating each copy and removing
    // equal click_ids afterwards is the same as dedup-then-validate. A click's copies share user,
    // person and ts, so they land in one partition and sort next to each other.
    let click_paths=csv_files(&data.join("clicks"))?;
    let cl_by_person:Vec<Mutex<Vec<Vec<Click>>>>=(0..PARTS).map(|_|Mutex::new(Vec::new())).collect();
    let campaigns_r=&campaigns;
    let flush_clicks=|q:usize,v:&mut Vec<Click>|{ let full=std::mem::replace(v,Vec::with_capacity(BATCH)); cl_by_person[q].lock().unwrap().push(full); };
    parallel_rows(&click_paths, threads,
        || (0..PARTS).map(|_| Vec::with_capacity(BATCH)).collect::<Vec<Vec<Click>>>(),
        |local, line| {
            let Some(f)=parse_fields::<4>(line)else{return;};
            let(Some(id),Some(user),Some(camp),Some(tsb))=(f[0],f[1],f[2],f[3])else{return;};
            let Some(ts)=parse_ts(tsb)else{return;};
            if ts<ws-WEEK||ts>=we{return;}
            let Some(&campaign)=campaigns_r.get(&Key::new(camp))else{return;};
            let person=uf.root(&Key::new(user)); let q=part_of(&person);
            local[q].push(Click{person,ts,click_id:Key::new(id),campaign});
            if local[q].len()>=BATCH*8 { flush_clicks(q,&mut local[q]); }
        },
        |mut local| for (q,v) in local.iter_mut().enumerate() { if !v.is_empty() { flush_clicks(q,v); } })?;
    mark("clicks_parse_validate",&mut last);

    // ---- per person partition: sessionize, user_daily rows, credit and assists
    let min_day=ws.div_euclid(DAY)as i32;let max_day=(we-1).div_euclid(DAY)as i32;let nday=(max_day-min_day+1)as usize;
    let organic=u32::MAX;
    let ncamp=campaign_ids.len();
    let ai=|c:u32,d:i32|(if c==organic{ncamp}else{c as usize})*nday+(d-min_day)as usize;
    let categories_r=&categories;
    let states=parallel_for(PARTS, threads,
        || (vec![CampaignAcc::default();(ncamp+1)*nday], Vec::<UserRow>::new()),
        |q, (acc, user_rows)| {
            let mut events:Vec<Event>={let vs=std::mem::take(&mut *ev_by_person[q].lock().unwrap());let n=vs.iter().map(Vec::len).sum();let mut e=Vec::with_capacity(n);for v in vs{e.extend(v);}e};
            events.sort_unstable_by(|a,b|a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            let people=sessionize(&events,categories_r,user_rows);
            drop(events);
            let mut clicks:Vec<Click>={let vs=std::mem::take(&mut *cl_by_person[q].lock().unwrap());let n=vs.iter().map(Vec::len).sum();let mut e=Vec::with_capacity(n);for v in vs{e.extend(v);}e};
            clicks.sort_unstable_by(|a,b|a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then(a.click_id.cmp(&b.click_id)));
            clicks.dedup_by(|a,b|a.click_id==b.click_id);
            attribute(&people,&clicks,acc,&ai,organic);
        });
    mark("sessionize_attribute",&mut last);
    let mut acc=vec![CampaignAcc::default();(ncamp+1)*nday];
    let mut user_rows=Vec::new();
    for (a,rows) in states { for (x,y) in acc.iter_mut().zip(a) { x.sessions+=y.sessions;x.purchasing+=y.purchasing;x.revenue+=y.revenue;x.assisted+=y.assisted; } user_rows.extend(rows); }
    user_rows.sort_unstable_by(|a,b|a.user.cmp(&b.user).then(a.day.cmp(&b.day)));
    let mut campaign_rows=Vec::new();for c in 0..ncamp as u32{for d in min_day..=max_day{let a=acc[ai(c,d)].clone();if a.sessions>0||a.assisted>0{campaign_rows.push((c,d,a));}}}for d in min_day..=max_day{let a=acc[ai(organic,d)].clone();if a.sessions>0{campaign_rows.push((organic,d,a));}}
    campaign_rows.sort_unstable_by(|a,b|{let x=if a.0==organic{b"organic".as_slice()}else{campaign_ids[a.0 as usize].as_slice()};let y=if b.0==organic{b"organic".as_slice()}else{campaign_ids[b.0 as usize].as_slice()};x.cmp(y).then(a.1.cmp(&b.1))});
    mark("merge_sort",&mut last);
    write_user(&output.join("user_daily"),&user_rows,&categories)?;
    write_campaign(&output.join("campaign_daily"),&campaign_rows,&campaign_ids,&channels)?;
    mark("parquet_write",&mut last);
    Ok(())
}

/// events sorted by (person, ts) -> sessions per person; appends user_daily rows (rules.md events 5-7)
fn sessionize(events:&[Event],categories:&[Vec<u8>],user_rows:&mut Vec<UserRow>)->Vec<PersonSessions>{
    let mut people=Vec::new();let mut i=0;
    while i<events.len(){let person=events[i].person.clone();let mut end=i+1;while end<events.len()&&events[end].person==person{end+=1;}let mut sessions=Vec::new();let mut day=None;let(mut ds,mut de,mut dr)=(0,0,0);let mut cat:DMap<u32,i64>=DMap::default();let mut j=i;
        while j<end{let start=events[j].ts;let mut k=j+1;while k<end&&events[k].ts-events[k-1].ts<=GAP{k+=1;}let sd=start.div_euclid(DAY)as i32;if day!=Some(sd){if let Some(d)=day{flush_user(&person,d,ds,de,dr,&mut cat,categories,user_rows);}day=Some(sd);ds=0;de=0;dr=0;}let(mut rev,mut purchasing)=(0,false);for e in&events[j..k]{rev+=e.price;if e.kind==3{purchasing=true;*cat.entry(e.category).or_default()+=e.price;}}let count=(k-j)as i64;sessions.push(Session{start,day:sd,events:count,revenue:rev,purchasing});ds+=1;de+=count;dr+=rev;j=k;}
        if let Some(d)=day{flush_user(&person,d,ds,de,dr,&mut cat,categories,user_rows);}people.push(PersonSessions{person,sessions});i=end;}
    people
}

/// people sorted by person, clicks sorted by (person, ts, click_id) and deduped: credit and assists
/// (rules.md clicks 4-5, assists)
fn attribute<F:Fn(u32,i32)->usize>(people:&[PersonSessions],clicks:&[Click],acc:&mut[CampaignAcc],ai:&F,organic:u32){
    let mut ci=0;
    for p in people{while ci<clicks.len()&&clicks[ci].person<p.person{ci+=1;}let cs=ci;while ci<clicks.len()&&clicks[ci].person==p.person{ci+=1;}let pc=&clicks[cs..ci];let mut collapsed=Vec::new();let mut x=0;while x<pc.len(){collapsed.push((pc[x].ts,pc[x].campaign));let t=pc[x].ts;x+=1;while x<pc.len()&&pc[x].ts==t{x+=1;}}let mut cp=0;for s in&p.sessions{while cp<collapsed.len()&&collapsed[cp].0<=s.start{cp+=1;}let c=if cp>0&&s.start-collapsed[cp-1].0<=WEEK{collapsed[cp-1].1}else{organic};let a=&mut acc[ai(c,s.day)];a.sessions+=1;a.purchasing+=s.purchasing as i64;a.revenue+=s.revenue;}let mut by:DMap<u32,Vec<i64>>=DMap::default();for c in pc{by.entry(c.campaign).or_default().push(c.ts);}for(c,times)in by{let(mut a,mut b)=(times[0],times[0]+WEEK);for&t in&times[1..]{if t>b{assist_range(&p.sessions,a,b,c,acc,ai);a=t;b=t+WEEK;}else if t+WEEK>b{b=t+WEEK;}}assist_range(&p.sessions,a,b,c,acc,ai);}}
}

fn flush_user(person:&Key,day:i32,sessions:i64,events:i64,revenue:i64,cat:&mut DMap<u32,i64>,categories:&[Vec<u8>],rows:&mut Vec<UserRow>){let best=cat.values().copied().max().unwrap_or(0);let top=if best==0{None}else{cat.iter().filter(|(_,v)|**v==best).map(|(k,_)|*k).min_by(|&a,&b|categories[a as usize].cmp(&categories[b as usize]))};rows.push(UserRow{user:person.clone(),day,sessions,events,revenue,top});cat.clear();}
fn assist_range<F:Fn(u32,i32)->usize>(sessions:&[Session],a:i64,b:i64,c:u32,acc:&mut[CampaignAcc],idx:&F){for s in&sessions[lower_bound(sessions,a)..upper_bound(sessions,b)]{acc[idx(c,s.day)].assisted+=1;}}

