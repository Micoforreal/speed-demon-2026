// Track A – Ingest (High Performance Parallel Aggregator)
// Reads /data/requests.log, writes /out/summary.txt.
//
// Each line: ts method endpoint status latency_us (space-delimited, LF)
// ts is RFC 3339: "YYYY-MM-DDTHH:MM:SS.ffffffZ"
//
// Date-general hour grouping: computes linear hour bucket from civil date
// (days_from_civil * 24 + hour) as i64 without hardcoded bounds or tables.
// Dynamically sizes dense bucket arrays for compact ranges (<=10,000 hours),
// and falls back to sparse grouping for wide spans/outliers to avoid OOM.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Mutex;

const CACHE_SIZE: usize = 8192;
const CACHE_MASK: usize = CACHE_SIZE - 1;

#[inline(always)]
fn days_from_civil(mut y: i64, m: u32, d: u32) -> i64 {
    y -= (m <= 2) as i64;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

#[inline(always)]
fn parse_timestamp_hour(ts: &[u8]) -> Option<i64> {
    if ts.len() < 27
        || ts[4] != b'-'
        || ts[7] != b'-'
        || ts[10] != b'T'
        || ts[13] != b':'
        || ts[16] != b':'
        || ts[19] != b'.'
        || ts[26] != b'Z'
    {
        return None;
    }
    for &idx in &[0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 22, 23, 24, 25] {
        if ts[idx] < b'0' || ts[idx] > b'9' {
            return None;
        }
    }
    let year = (ts[0] - b'0') as i64 * 1000
        + (ts[1] - b'0') as i64 * 100
        + (ts[2] - b'0') as i64 * 10
        + (ts[3] - b'0') as i64;
    let month = ((ts[5] - b'0') * 10 + (ts[6] - b'0')) as u32;
    let day = ((ts[8] - b'0') * 10 + (ts[9] - b'0')) as u32;
    let hour = ((ts[11] - b'0') * 10 + (ts[12] - b'0')) as i64;
    let minute = (ts[14] - b'0') * 10 + (ts[15] - b'0');
    let second = (ts[17] - b'0') * 10 + (ts[18] - b'0');

    if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    Some(days_from_civil(year, month, day) * 24 + hour)
}

#[inline(always)]
fn hash_endpoint(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h = (h ^ (b as u64)).wrapping_mul(0x100000001b3);
    }
    if h == 0 { 1 } else { h }
}

#[derive(Clone, Copy)]
struct EndpointEntry {
    hash: u64,
    ep: &'static [u8],
    id: u16,
}

impl EndpointEntry {
    const EMPTY: Self = Self { hash: 0, ep: &[], id: 0 };
}

struct GlobalRegistry {
    endpoints: Mutex<Vec<&'static [u8]>>,
    map: Mutex<[EndpointEntry; CACHE_SIZE]>,
}

impl GlobalRegistry {
    fn new() -> Self {
        Self {
            endpoints: Mutex::new(Vec::with_capacity(2048)),
            map: Mutex::new([EndpointEntry::EMPTY; CACHE_SIZE]),
        }
    }

    fn get_or_insert(&self, ep: &'static [u8], h: u64) -> u16 {
        let mut map = self.map.lock().unwrap();
        let mut idx = (h as usize) & CACHE_MASK;
        loop {
            let entry = &map[idx];
            if entry.hash == h && entry.ep == ep {
                return entry.id;
            }
            if entry.hash == 0 {
                let mut eps = self.endpoints.lock().unwrap();
                let id = eps.len() as u16;
                eps.push(ep);
                map[idx] = EndpointEntry { hash: h, ep, id };
                return id;
            }
            idx = (idx + 1) & CACHE_MASK;
        }
    }
}

#[derive(Clone, Copy)]
struct LineRecord {
    abs_hour: i64,
    raw_ep: u16,
    is_5xx: u8,
    latency: u32,
}

#[inline(always)]
fn write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let mut tmp = [0u8; 20];
    let mut i = 0;
    while n > 0 {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    while i > 0 {
        i -= 1;
        buf.push(tmp[i]);
    }
}

#[cfg(target_family = "unix")]
unsafe fn mmap_file(file: &File, len: usize) -> Option<&'static [u8]> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut u8;
    }
    const PROT_READ: i32 = 1;
    const MAP_PRIVATE: i32 = 2;
    let ptr = mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0);
    if ptr as isize == -1 {
        None
    } else {
        Some(std::slice::from_raw_parts(ptr, len))
    }
}

fn load_input(path: &str) -> &'static [u8] {
    let file = File::open(path).unwrap_or_else(|_| panic!("cannot open input file: {}", path));
    let metadata = file.metadata().expect("cannot read file metadata");
    let len = metadata.len() as usize;

    #[cfg(target_family = "unix")]
    if len > 0 {
        if let Some(slice) = unsafe { mmap_file(&file, len) } {
            return slice;
        }
    }

    let mut buf = Vec::with_capacity(len);
    let mut reader = std::io::BufReader::with_capacity(1 << 20, file);
    reader.read_to_end(&mut buf).expect("cannot read input file");
    Box::leak(buf.into_boxed_slice())
}

fn main() {
    let in_path = std::env::args().nth(1).unwrap_or_else(|| "/data/requests.log".to_string());
    let out_path = std::env::args().nth(2).unwrap_or_else(|| "/out/summary.txt".to_string());

    if let Some(parent) = std::path::Path::new(&out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let data = load_input(&in_path);
    if data.is_empty() {
        File::create(&out_path).expect("cannot create output file");
        return;
    }

    let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(12).min(12);
    let registry = GlobalRegistry::new();

    // Split input into chunks ending at newlines
    let mut chunk_boundaries = Vec::with_capacity(num_threads + 1);
    chunk_boundaries.push(0);
    for i in 1..num_threads {
        let mut pos = (data.len() * i) / num_threads;
        while pos < data.len() && data[pos] != b'\n' {
            pos += 1;
        }
        if pos < data.len() {
            pos += 1; // start of next line
        }
        chunk_boundaries.push(pos.min(data.len()));
    }
    chunk_boundaries.push(data.len());

    let mut thread_records: Vec<Vec<LineRecord>> = Vec::with_capacity(num_threads);
    let mut global_min_hour = i64::MAX;
    let mut global_max_hour = i64::MIN;
    let mut hour_labels_map: HashMap<i64, [u8; 20]> = HashMap::new();

    // Pass 1: Parse chunks in parallel
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let start = chunk_boundaries[i];
            let end = chunk_boundaries[i + 1];
            let reg = &registry;
            handles.push(s.spawn(move || {
                let mut records = Vec::with_capacity((end - start) / 60);
                let mut local_cache = [EndpointEntry::EMPTY; CACHE_SIZE];
                let mut min_hour = i64::MAX;
                let mut max_hour = i64::MIN;
                let mut local_hours: Vec<(i64, [u8; 20])> = Vec::with_capacity(128);
                let mut last_seen_hour = i64::MAX;

                let mut pos = start;
                while pos < end {
                    let line_start = pos;
                    while pos < end && data[pos] != b'\n' {
                        pos += 1;
                    }
                    let mut line_end = pos;
                    if line_end > line_start && data[line_end - 1] == b'\r' {
                        line_end -= 1;
                    }
                    if pos < end {
                        pos += 1; // skip \n
                    }

                    let line = &data[line_start..line_end];
                    if line.len() < 35 {
                        continue;
                    }

                    // ts is always 27 bytes: "YYYY-MM-DDTHH:MM:SS.ffffffZ"
                    let abs_hour = match parse_timestamp_hour(&line[..27]) {
                        Some(h) => h,
                        None => continue,
                    };

                    if line[27] != b' ' {
                        continue;
                    }

                    // Skip method (starts at 28)
                    let mut p = 28;
                    while p < line.len() && line[p] != b' ' {
                        p += 1;
                    }
                    if p >= line.len() {
                        continue;
                    }
                    p += 1; // skip space

                    // Endpoint
                    let ep_start = p;
                    while p < line.len() && line[p] != b' ' {
                        p += 1;
                    }
                    if p >= line.len() || p == ep_start {
                        continue;
                    }
                    let ep = &line[ep_start..p];
                    p += 1; // skip space

                    // Status
                    let status_start = p;
                    while p < line.len() && line[p] >= b'0' && line[p] <= b'9' {
                        p += 1;
                    }
                    if p == status_start || p >= line.len() || line[p] != b' ' {
                        continue;
                    }
                    let status_bytes = &line[status_start..p];
                    let is_5xx = if status_bytes.len() == 3 && status_bytes[0] == b'5' { 1u8 } else { 0u8 };
                    p += 1; // skip space

                    // Latency (u32)
                    if p >= line.len() {
                        continue;
                    }
                    let mut lat: u64 = 0;
                    let mut has_digit = false;
                    while p < line.len() && line[p] >= b'0' && line[p] <= b'9' {
                        lat = lat * 10 + (line[p] - b'0') as u64;
                        has_digit = true;
                        p += 1;
                    }
                    if !has_digit || lat > 4_294_967_295 || (p < line.len() && line[p] != b' ' && line[p] != b'\r') {
                        continue;
                    }

                    // Track hour metadata
                    if abs_hour != last_seen_hour {
                        let mut label = [0u8; 20];
                        label[..13].copy_from_slice(&line[..13]);
                        label[13..].copy_from_slice(b":00:00Z");
                        if !local_hours.iter().any(|&(h, _)| h == abs_hour) {
                            local_hours.push((abs_hour, label));
                        }
                        last_seen_hour = abs_hour;
                    }
                    if abs_hour < min_hour {
                        min_hour = abs_hour;
                    }
                    if abs_hour > max_hour {
                        max_hour = abs_hour;
                    }

                    // Intern endpoint via local cache with full byte comparison on hit
                    let h = hash_endpoint(ep);
                    let mut cache_idx = (h as usize) & CACHE_MASK;
                    let ep_id;
                    loop {
                        let entry = &local_cache[cache_idx];
                        if entry.hash == h && entry.ep == ep {
                            ep_id = entry.id;
                            break;
                        }
                        if entry.hash == 0 {
                            let id = reg.get_or_insert(ep, h);
                            local_cache[cache_idx] = EndpointEntry { hash: h, ep, id };
                            ep_id = id;
                            break;
                        }
                        cache_idx = (cache_idx + 1) & CACHE_MASK;
                    }

                    records.push(LineRecord {
                        abs_hour,
                        raw_ep: ep_id,
                        is_5xx,
                        latency: lat as u32,
                    });
                }
                (records, min_hour, max_hour, local_hours)
            }));
        }

        for h in handles {
            let (records, t_min, t_max, t_hours) = h.join().unwrap();
            thread_records.push(records);
            if t_min <= t_max {
                if t_min < global_min_hour {
                    global_min_hour = t_min;
                }
                if t_max > global_max_hour {
                    global_max_hour = t_max;
                }
            }
            for (hr, label) in t_hours {
                hour_labels_map.insert(hr, label);
            }
        }
    });

    let raw_endpoints = registry.endpoints.into_inner().unwrap();
    let num_endpoints = raw_endpoints.len();
    if num_endpoints == 0 || global_min_hour > global_max_hour {
        File::create(&out_path).expect("cannot create output file");
        return;
    }

    // Sort endpoints lexicographically by raw bytes
    let mut sorted_indices: Vec<u16> = (0..num_endpoints as u16).collect();
    sorted_indices.sort_unstable_by(|&a, &b| raw_endpoints[a as usize].cmp(raw_endpoints[b as usize]));

    let mut remap = vec![0u16; num_endpoints];
    for (rank, &raw_id) in sorted_indices.iter().enumerate() {
        remap[raw_id as usize] = rank as u16;
    }

    let span_hours = global_max_hour - global_min_hour + 1;
    let is_dense = span_hours > 0
        && span_hours <= 10_000
        && (span_hours as usize).checked_mul(num_endpoints).map_or(false, |b| b <= 5_000_000);

    if is_dense {
        let num_hours = span_hours as usize;
        let num_buckets = num_hours * num_endpoints;

        let mut hour_labels_vec = vec![[0u8; 20]; num_hours];
        for (&h, &label) in &hour_labels_map {
            let rel_h = (h - global_min_hour) as usize;
            if rel_h < num_hours {
                hour_labels_vec[rel_h] = label;
            }
        }

        // Pass 2: Tally counts and errors per bucket per thread
        let mut thread_counts: Vec<Vec<u32>> = Vec::with_capacity(num_threads);
        let mut thread_errors: Vec<Vec<u32>> = Vec::with_capacity(num_threads);

        std::thread::scope(|s2| {
            let mut handles2 = Vec::with_capacity(num_threads);
            for records in &thread_records {
                let remap_ref = &remap;
                let min_hour = global_min_hour;
                handles2.push(s2.spawn(move || {
                    let mut counts = vec![0u32; num_buckets];
                    let mut errors = vec![0u32; num_buckets];
                    for rec in records {
                        let sorted_ep = remap_ref[rec.raw_ep as usize] as usize;
                        let rel_hour = (rec.abs_hour - min_hour) as usize;
                        let b = rel_hour * num_endpoints + sorted_ep;
                        counts[b] += 1;
                        if rec.is_5xx != 0 {
                            errors[b] += 1;
                        }
                    }
                    (counts, errors)
                }));
            }
            for h in handles2 {
                let (c, e) = h.join().unwrap();
                thread_counts.push(c);
                thread_errors.push(e);
            }
        });

        // Global counts, errors and prefix sum offsets
        let mut global_counts = vec![0u32; num_buckets];
        let mut global_errors = vec![0u32; num_buckets];
        for t in 0..num_threads {
            for b in 0..num_buckets {
                global_counts[b] += thread_counts[t][b];
                global_errors[b] += thread_errors[t][b];
            }
        }

        let mut offsets = vec![0usize; num_buckets + 1];
        let mut total_records = 0usize;
        for b in 0..num_buckets {
            offsets[b] = total_records;
            total_records += global_counts[b] as usize;
        }
        offsets[num_buckets] = total_records;

        let mut all_latencies = vec![0u32; total_records];
        let lat_ptr = all_latencies.as_mut_ptr() as usize;

        // Pass 3: Scatter latencies into contiguous bucket slices in parallel
        std::thread::scope(|s3| {
            for t in 0..num_threads {
                let records = &thread_records[t];
                let remap_ref = &remap;
                let offsets_ref = &offsets;
                let counts_ref = &thread_counts;
                let min_hour = global_min_hour;
                s3.spawn(move || {
                    let mut write_pos = vec![0usize; num_buckets];
                    for b in 0..num_buckets {
                        let mut prior = 0usize;
                        for prev_t in 0..t {
                            prior += counts_ref[prev_t][b] as usize;
                        }
                        write_pos[b] = offsets_ref[b] + prior;
                    }

                    let ptr = lat_ptr as *mut u32;
                    for rec in records {
                        let sorted_ep = remap_ref[rec.raw_ep as usize] as usize;
                        let rel_hour = (rec.abs_hour - min_hour) as usize;
                        let b = rel_hour * num_endpoints + sorted_ep;
                        let pos = write_pos[b];
                        unsafe {
                            *ptr.add(pos) = rec.latency;
                        }
                        write_pos[b] = pos + 1;
                    }
                });
            }
        });

        // Pass 4: Sort each bucket's latencies, compute percentiles, format output in parallel
        let mut thread_outputs: Vec<Vec<u8>> = Vec::with_capacity(num_threads);
        let active_buckets_per_thread = (num_buckets + num_threads - 1) / num_threads;

        std::thread::scope(|s4| {
            let mut handles4 = Vec::with_capacity(num_threads);
            for t in 0..num_threads {
                let b_start = (t * active_buckets_per_thread).min(num_buckets);
                let b_end = (b_start + active_buckets_per_thread).min(num_buckets);
                let offsets_ref = &offsets;
                let counts_ref = &global_counts;
                let errors_ref = &global_errors;
                let raw_eps = &raw_endpoints;
                let sorted_idx = &sorted_indices;
                let hour_labels_ref = &hour_labels_vec;

                handles4.push(s4.spawn(move || {
                    let mut out = Vec::with_capacity((b_end - b_start).min(32768) * 64);
                    let ptr = lat_ptr as *mut u32;

                    for b in b_start..b_end {
                        let count = counts_ref[b] as usize;
                        if count == 0 {
                            continue;
                        }

                        let off = offsets_ref[b];
                        let slice = unsafe { std::slice::from_raw_parts_mut(ptr.add(off), count) };
                        slice.sort_unstable();

                        let n = count as u64;
                        let p50 = slice[((50 * n + 99) / 100 - 1) as usize];
                        let p95 = slice[((95 * n + 99) / 100 - 1) as usize];
                        let p99 = slice[((99 * n + 99) / 100 - 1) as usize];
                        let errors = errors_ref[b] as u64;
                        let error_ppm = errors * 1_000_000 / n;

                        let rel_hour = b / num_endpoints;
                        let sorted_ep = b % num_endpoints;
                        let raw_ep = sorted_idx[sorted_ep] as usize;
                        let ep_bytes = raw_eps[raw_ep];
                        let hour_bytes = &hour_labels_ref[rel_hour];

                        // hour endpoint count p50 p95 p99 errors error_ppm\n
                        out.extend_from_slice(hour_bytes);
                        out.push(b' ');
                        out.extend_from_slice(ep_bytes);
                        out.push(b' ');
                        write_u64(&mut out, n);
                        out.push(b' ');
                        write_u64(&mut out, p50 as u64);
                        out.push(b' ');
                        write_u64(&mut out, p95 as u64);
                        out.push(b' ');
                        write_u64(&mut out, p99 as u64);
                        out.push(b' ');
                        write_u64(&mut out, errors);
                        out.push(b' ');
                        write_u64(&mut out, error_ppm);
                        out.push(b'\n');
                    }
                    out
                }));
            }

            for h in handles4 {
                thread_outputs.push(h.join().unwrap());
            }
        });

        let mut out_file = std::io::BufWriter::with_capacity(
            1 << 20,
            File::create(&out_path).unwrap_or_else(|_| panic!("cannot create output file: {}", out_path)),
        );
        for out in &thread_outputs {
            out_file.write_all(out).expect("cannot write output");
        }
    } else {
        // Sparse grouping: used when hour span is large (far outliers) to prevent memory blowup
        let mut merged_groups: HashMap<(i64, u16), (Vec<u32>, u64)> = HashMap::new();
        for records in thread_records {
            for rec in records {
                let sorted_ep = remap[rec.raw_ep as usize];
                let entry = merged_groups.entry((rec.abs_hour, sorted_ep)).or_insert_with(|| (Vec::new(), 0));
                entry.0.push(rec.latency);
                if rec.is_5xx != 0 {
                    entry.1 += 1;
                }
            }
        }

        let mut sorted_keys: Vec<(i64, u16)> = merged_groups.keys().copied().collect();
        sorted_keys.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut out = Vec::with_capacity(sorted_keys.len() * 64);
        for key in sorted_keys {
            let (mut latencies, errors) = merged_groups.remove(&key).unwrap();
            latencies.sort_unstable();

            let n = latencies.len() as u64;
            let p50 = latencies[((50 * n + 99) / 100 - 1) as usize];
            let p95 = latencies[((95 * n + 99) / 100 - 1) as usize];
            let p99 = latencies[((99 * n + 99) / 100 - 1) as usize];
            let error_ppm = errors * 1_000_000 / n;

            let (abs_hour, sorted_ep) = key;
            let raw_ep = sorted_indices[sorted_ep as usize] as usize;
            let ep_bytes = raw_endpoints[raw_ep];
            let hour_bytes = hour_labels_map.get(&abs_hour).unwrap();

            out.extend_from_slice(hour_bytes);
            out.push(b' ');
            out.extend_from_slice(ep_bytes);
            out.push(b' ');
            write_u64(&mut out, n);
            out.push(b' ');
            write_u64(&mut out, p50 as u64);
            out.push(b' ');
            write_u64(&mut out, p95 as u64);
            out.push(b' ');
            write_u64(&mut out, p99 as u64);
            out.push(b' ');
            write_u64(&mut out, errors);
            out.push(b' ');
            write_u64(&mut out, error_ppm);
            out.push(b'\n');
        }

        let mut out_file = std::io::BufWriter::with_capacity(
            1 << 20,
            File::create(&out_path).unwrap_or_else(|_| panic!("cannot create output file: {}", out_path)),
        );
        out_file.write_all(&out).expect("cannot write output");
    }
}
