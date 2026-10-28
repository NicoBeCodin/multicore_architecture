use spsc::Spsc;
use std::hint::{black_box, spin_loop};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
enum Kind {
    StdBlocking,
    StdSpinning,
    Custom,
}

const KINDS: [Kind; 3] = [Kind::StdBlocking, Kind::StdSpinning, Kind::Custom];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::StdBlocking => "std blocking",
            Kind::StdSpinning => "std spinning",
            Kind::Custom => "custom SPSC",
        }
    }
}

// Dedicated endpoint types make the hot loops statically dispatched.
// This avoids a runtime enum branch on every message.
trait SendEndpoint<T>: Send {
    fn send(&mut self, value: T, full_retries: &mut u64);
}
trait RecvEndpoint<T>: Send {
    fn recv(&mut self, empty_retries: &mut u64) -> T;
}

struct BlockingTx<T>(mpsc::SyncSender<T>);
struct BlockingRx<T>(mpsc::Receiver<T>);
struct SpinningTx<T>(mpsc::SyncSender<T>);
struct SpinningRx<T>(mpsc::Receiver<T>);
struct CustomTx<T>(spsc::Producer<T>);
struct CustomRx<T>(spsc::Consumer<T>);

impl<T: Send> SendEndpoint<T> for BlockingTx<T> {
    #[inline]
    fn send(&mut self, value: T, _: &mut u64) {
        self.0.send(value).unwrap_or_else(|_| panic!("receiver disconnected"));
    }
}
impl<T: Send> RecvEndpoint<T> for BlockingRx<T> {
    #[inline]
    fn recv(&mut self, _: &mut u64) -> T {
        self.0.recv().unwrap_or_else(|_| panic!("sender disconnected"))
    }
}
impl<T: Send> SendEndpoint<T> for SpinningTx<T> {
    #[inline]
    fn send(&mut self, mut value: T, full_retries: &mut u64) {
        loop {
            match self.0.try_send(value) {
                Ok(()) => return,
                Err(mpsc::TrySendError::Full(v)) => {
                    value = v;
                    *full_retries += 1;
                    spin_loop();
                }
                Err(mpsc::TrySendError::Disconnected(_)) => panic!("receiver disconnected"),
            }
        }
    }
}
impl<T: Send> RecvEndpoint<T> for SpinningRx<T> {
    #[inline]
    fn recv(&mut self, empty_retries: &mut u64) -> T {
        loop {
            match self.0.try_recv() {
                Ok(value) => return value,
                Err(mpsc::TryRecvError::Empty) => {
                    *empty_retries += 1;
                    spin_loop();
                }
                Err(mpsc::TryRecvError::Disconnected) => panic!("sender disconnected"),
            }
        }
    }
}
impl<T: Send> SendEndpoint<T> for CustomTx<T> {
    #[inline]
    fn send(&mut self, mut value: T, full_retries: &mut u64) {
        loop {
            match self.0.push(value) {
                Ok(()) => return,
                Err(v) => {
                    assert!(self.0.is_consumer_alive(), "receiver disconnected");
                    value = v;
                    *full_retries += 1;
                    spin_loop();
                }
            }
        }
    }
}
impl<T: Send> RecvEndpoint<T> for CustomRx<T> {
    #[inline]
    fn recv(&mut self, empty_retries: &mut u64) -> T {
        loop {
            if let Some(value) = self.0.pop() {
                return value;
            }
            assert!(self.0.is_producer_alive(), "sender disconnected");
            *empty_retries += 1;
            spin_loop();
        }
    }
}

fn start_barrier() -> Arc<Barrier> {
    Arc::new(Barrier::new(3))
}

fn busy_work(steps: usize) {
    for _ in 0..steps {
        spin_loop();
    }
}

#[derive(Debug)]
struct StreamResult {
    elapsed: Duration,
    full_retries: u64,
    empty_retries: u64,
}

fn stream(kind: Kind, capacity: usize, n: u64, producer_work: usize, consumer_work: usize) -> StreamResult {
    match kind {
        Kind::StdBlocking => {
            let (tx, rx) = mpsc::sync_channel(capacity);
            stream_inner(BlockingTx(tx), BlockingRx(rx), n, producer_work, consumer_work)
        }
        Kind::StdSpinning => {
            let (tx, rx) = mpsc::sync_channel(capacity);
            stream_inner(SpinningTx(tx), SpinningRx(rx), n, producer_work, consumer_work)
        }
        Kind::Custom => {
            let (tx, rx) = Spsc::<u64>::new(capacity);
            stream_inner(CustomTx(tx), CustomRx(rx), n, producer_work, consumer_work)
        }
    }
}

fn stream_inner<TX: SendEndpoint<u64> + 'static, RX: RecvEndpoint<u64> + 'static>(
    mut tx: TX,
    mut rx: RX,
    n: u64,
    producer_work: usize,
    consumer_work: usize,
) -> StreamResult {
    let gate = start_barrier();

    let p_gate = Arc::clone(&gate);
    let producer = thread::spawn(move || {
        let mut full = 0;
        p_gate.wait();
        for i in 0..n {
            busy_work(producer_work);
            tx.send(black_box(i), &mut full);
        }
        full
    });

    let c_gate = Arc::clone(&gate);
    let consumer = thread::spawn(move || {
        let mut empty = 0;
        let mut sum = 0u64;
        c_gate.wait();
        for _ in 0..n {
            let value = rx.recv(&mut empty);
            sum = sum.wrapping_add(black_box(value));
            busy_work(consumer_work);
        }
        (sum, empty)
    });

    let start = Instant::now();
    gate.wait();
    let full_retries = producer.join().unwrap();
    let (sum, empty_retries) = consumer.join().unwrap();
    let elapsed = start.elapsed();
    let expected = (n as u128 * (n as u128 - 1) / 2) as u64;
    assert_eq!(sum, expected, "checksum mismatch");

    StreamResult { elapsed, full_retries, empty_retries }
}

fn median_stream(kind: Kind, cap: usize, n: u64, p_work: usize, c_work: usize, runs: usize) -> StreamResult {
    let mut samples: Vec<_> = (0..runs).map(|_| stream(kind, cap, n, p_work, c_work)).collect();
    samples.sort_by_key(|s| s.elapsed);
    samples.remove(runs / 2)
}

fn print_stream(kind: Kind, cap: usize, n: u64, p_work: usize, c_work: usize, runs: usize) {
    let sample = median_stream(kind, cap, n, p_work, c_work, runs);
    let mps = (n as f64 / sample.elapsed.as_secs_f64()) / 1e6;
    if matches!(kind, Kind::StdBlocking) {
        println!("  {:<14} {:>9.2} M/s  retries: n/a (blocking)", kind.name(), mps);
    } else {
        println!(
            "  {:<14} {:>9.2} M/s  full retries/msg: {:>8.2}  empty retries/msg: {:>8.2}",
            kind.name(), mps, sample.full_retries as f64 / n as f64,
            sample.empty_retries as f64 / n as f64
        );
    }
}

fn throughput() {
    println!("\n=== 1. Capacity sweep: continuous throughput ===");
    println!("Capacity 1 and 2 use fewer messages to keep runtime manageable.");
    for cap in [1, 2, 8, 64, 1024, 16_384] {
        let n = if cap <= 2 { 25_000 } else { 2_000_000 };
        println!("\ncapacity={cap}, messages={n}, median of 3:");
        for kind in KINDS {
            print_stream(kind, cap, n, 0, 0, 3);
        }
    }
}

fn asymmetry() {
    println!("\n=== 2. Producer/consumer imbalance ===");
    println!("Work means processor spin hints per message, not real market-data parsing.");
    for (label, p_work, c_work) in [
        ("balanced", 0, 0),
        ("slow producer", 100, 0),
        ("slow consumer", 0, 100),
    ] {
        let n = 300_000;
        println!("\n{label} (producer work={p_work}, consumer work={c_work}):");
        for kind in KINDS {
            print_stream(kind, 1024, n, p_work, c_work, 3);
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Stamped {
    sequence: u64,
    submitted_ns: u64,
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    assert!(!sorted.is_empty());
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn print_latency(kind: Kind, samples_ns: &mut [u64], full: u64, empty: u64) {
    samples_ns.sort_unstable();
    let ns = |p| percentile(samples_ns, p) as f64;
    if matches!(kind, Kind::StdBlocking) {
        println!(
            "  {:<14} p50={:>8.2} us p99={:>8.2} us p99.9={:>8.2} us  retries: n/a (blocking)",
            kind.name(), ns(0.5) / 1_000.0, ns(0.99) / 1_000.0,
            ns(0.999) / 1_000.0
        );
    } else {
        println!(
            "  {:<14} p50={:>8.2} us p99={:>8.2} us p99.9={:>8.2} us  retries: full={} empty={}",
            kind.name(), ns(0.5) / 1_000.0, ns(0.99) / 1_000.0,
            ns(0.999) / 1_000.0, full, empty
        );
    }
}

// Submitted-to-received latency: includes time waiting to enqueue if full,
// time spent queued, and timestamp-measurement overhead.
fn paced_latency(kind: Kind, capacity: usize, n: u64, burst_size: u64, pause: Duration) {
    match kind {
        Kind::StdBlocking => {
            let (tx, rx) = mpsc::sync_channel(capacity);
            paced_inner(kind, BlockingTx(tx), BlockingRx(rx), n, burst_size, pause);
        }
        Kind::StdSpinning => {
            let (tx, rx) = mpsc::sync_channel(capacity);
            paced_inner(kind, SpinningTx(tx), SpinningRx(rx), n, burst_size, pause);
        }
        Kind::Custom => {
            let (tx, rx) = Spsc::<Stamped>::new(capacity);
            paced_inner(kind, CustomTx(tx), CustomRx(rx), n, burst_size, pause);
        }
    }
}

fn paced_inner<TX: SendEndpoint<Stamped> + 'static, RX: RecvEndpoint<Stamped> + 'static>(
    kind: Kind,
    mut tx: TX,
    mut rx: RX,
    n: u64,
    burst_size: u64,
    pause: Duration,
) {
    let gate = start_barrier();
    let epoch = Instant::now();

    let p_gate = Arc::clone(&gate);
    let producer = thread::spawn(move || {
        let mut full = 0;
        p_gate.wait();
        for i in 0..n {
            if i != 0 && i % burst_size == 0 {
                thread::sleep(pause);
            }
            let message = Stamped {
                sequence: i,
                submitted_ns: epoch.elapsed().as_nanos() as u64,
            };
            tx.send(message, &mut full);
        }
        full
    });

    let c_gate = Arc::clone(&gate);
    let consumer = thread::spawn(move || {
        let mut empty = 0;
        let mut latencies = Vec::with_capacity(n as usize);
        c_gate.wait();
        for expected in 0..n {
            let message = rx.recv(&mut empty);
            let received_ns = epoch.elapsed().as_nanos() as u64;
            assert_eq!(message.sequence, expected, "FIFO failure for {}", kind.name());
            latencies.push(received_ns.saturating_sub(message.submitted_ns));
        }
        (latencies, empty)
    });

    gate.wait();
    let full = producer.join().unwrap();
    let (mut latencies, empty) = consumer.join().unwrap();
    print_latency(kind, &mut latencies, full, empty);
}

fn pacing() {
    println!("\n=== 3. Sparse arrivals: 1 item about every 200 us ===");
    println!("Submitted-to-received latency; OS sleep is approximate.");
    for kind in KINDS {
        paced_latency(kind, 1024, 500, 1, Duration::from_micros(200));
    }

    println!("\n=== 4. Bursts: 32 messages, then about 200 us idle ===");
    println!("Includes burst queuing and scheduling effects; not single-operation cost.");
    for kind in KINDS {
        paced_latency(kind, 1024, 16_384, 32, Duration::from_micros(200));
    }
}

fn ping_pong(kind: Kind, n: u64) {
    match kind {
        Kind::StdBlocking => {
            let (tx1, rx1) = mpsc::sync_channel(2);
            let (tx2, rx2) = mpsc::sync_channel(2);
            ping_pong_inner(kind, BlockingTx(tx1), BlockingRx(rx1), BlockingTx(tx2), BlockingRx(rx2), n);
        }
        Kind::StdSpinning => {
            let (tx1, rx1) = mpsc::sync_channel(2);
            let (tx2, rx2) = mpsc::sync_channel(2);
            ping_pong_inner(kind, SpinningTx(tx1), SpinningRx(rx1), SpinningTx(tx2), SpinningRx(rx2), n);
        }
        Kind::Custom => {
            let (tx1, rx1) = Spsc::<u64>::new(2);
            let (tx2, rx2) = Spsc::<u64>::new(2);
            ping_pong_inner(kind, CustomTx(tx1), CustomRx(rx1), CustomTx(tx2), CustomRx(rx2), n);
        }
    }
}

fn ping_pong_inner<TX: SendEndpoint<u64> + 'static, RX: RecvEndpoint<u64> + 'static>(
    kind: Kind,
    mut a_to_b: TX,
    mut b_from_a: RX,
    mut b_to_a: TX,
    mut a_from_b: RX,
    n: u64,
) {
    let gate = start_barrier();

    let a_gate = Arc::clone(&gate);
    let a = thread::spawn(move || {
        let mut samples = Vec::with_capacity(n as usize);
        let (mut full, mut empty) = (0u64, 0u64);
        a_gate.wait();
        for i in 0..n {
            let start = Instant::now();
            a_to_b.send(black_box(i), &mut full);
            let reply = a_from_b.recv(&mut empty);
            let round_trip = start.elapsed().as_nanos() as u64;
            assert_eq!(reply, i);
            samples.push(round_trip);
        }
        (samples, full, empty)
    });

    let b_gate = Arc::clone(&gate);
    let b = thread::spawn(move || {
        let (mut full, mut empty) = (0u64, 0u64);
        b_gate.wait();
        for _ in 0..n {
            let value = b_from_a.recv(&mut empty);
            b_to_a.send(value, &mut full);
        }
    });

    gate.wait();
    let (mut samples, full, empty) = a.join().unwrap();
    b.join().unwrap();
    println!("  Two channel hops (request -> response), not one-way latency:");
    print_latency(kind, &mut samples, full, empty);
}

fn pingpong() {
    println!("\n=== 5. Ping-pong round-trip latency (only 1 request outstanding) ===");
    println!("Each sample includes two queue transfers, worker scheduling and Instant overhead.");
    for kind in KINDS {
        ping_pong(kind, 10_000);
    }
}

// --- Batch-size benchmark: separate from the single-message scenarios ---
#[derive(Clone, Copy)]
enum BatchMode {
    StdSpinning,
    CustomScalar,
    CustomBatch,
    CustomReservation,
}

// Identical start and end timing for all three modes.
fn timed_pair<P, C>(produce: P, consume: C, expected_sum: u64) -> Duration
where
    P: FnOnce() + Send + 'static,
    C: FnOnce() -> u64 + Send + 'static,
{
    let gate = start_barrier();
    let pg = Arc::clone(&gate);
    let producer = thread::spawn(move || {
        pg.wait();
        produce();
    });
    let cg = Arc::clone(&gate);
    let consumer = thread::spawn(move || {
        cg.wait();
        consume()
    });

    let start = Instant::now();
    gate.wait();
    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(sum, expected_sum, "lost or corrupted values");
    elapsed
}

fn batch_once(mode: BatchMode, capacity: usize, batch: usize, n: u64) -> Duration {
    assert!(batch > 0);
    match mode {
        BatchMode::StdSpinning => {
            let (tx, rx) = mpsc::sync_channel::<u64>(capacity);
            timed_pair(
                move || {
                    for _ in 0..n {
                        loop {
                            match tx.try_send(1) {
                                Ok(()) => break,
                                Err(mpsc::TrySendError::Full(_)) => spin_loop(),
                                Err(mpsc::TrySendError::Disconnected(_)) => panic!("receiver gone"),
                            }
                        }
                    }
                },
                move || {
                    let mut sum = 0u64;
                    for _ in 0..n {
                        let value = loop {
                            match rx.try_recv() {
                                Ok(v) => break v,
                                Err(mpsc::TryRecvError::Empty) => spin_loop(),
                                Err(mpsc::TryRecvError::Disconnected) => panic!("producer gone"),
                            }
                        };
                        sum = sum.wrapping_add(black_box(value));
                    }
                    sum
                },
                n,
            )
        }
        BatchMode::CustomScalar => {
            let (mut tx, mut rx) = Spsc::<u64>::new(capacity);
            timed_pair(
                move || {
                    for _ in 0..n {
                        while tx.push(1).is_err() {
                            spin_loop();
                        }
                    }
                },
                move || {
                    let mut sum = 0u64;
                    for _ in 0..n {
                        let value = loop {
                            if let Some(v) = rx.pop() {
                                break v;
                            }
                            spin_loop();
                        };
                        sum = sum.wrapping_add(black_box(value));
                    }
                    sum
                },
                n,
            )
        }
        BatchMode::CustomBatch => {
            let (mut tx, mut rx) = Spsc::<u64>::new(capacity);
            // Allocate both buffers before the timed interval.
            let input = vec![1u64; batch];
            let mut output = vec![0u64; batch];
            timed_pair(
                move || {
                    let mut sent = 0u64;
                    while sent < n {
                        let want = (n - sent).min(batch as u64) as usize;
                        let mut offset = 0;
                        while offset < want {
                            let accepted = tx.push_slice(&input[offset..want]);
                            if accepted == 0 {
                                spin_loop();
                            } else {
                                debug_assert!(accepted <= want - offset);
                                offset += accepted;
                            }
                        }
                        sent += want as u64;
                    }
                },
                move || {
                    let (mut received, mut sum) = (0u64, 0u64);
                    while received < n {
                        let want = (n - received).min(batch as u64) as usize;
                        let count = rx.pop_slice(&mut output[..want]);
                        if count == 0 {
                            spin_loop();
                            continue;
                        }
                        debug_assert!(count <= want);
                        for &value in &output[..count] {
                            sum = sum.wrapping_add(black_box(value));
                        }
                        received += count as u64;
                    }
                    sum
                },
                n,
            )
        }
        BatchMode::CustomReservation => {
            let (mut tx, mut rx) = Spsc::<u64>::new(capacity);
            timed_pair(
                move || {
                    let mut sent = 0u64;
                    while sent < n {
                        let want = (n - sent).min(batch as u64) as usize;
                        if let Some(mut reservation) = tx.try_reserve(want) {
                            let count = reservation.capacity();
                            for _ in 0..count {
                                reservation.push(1).unwrap();
                            }
                            sent += reservation.commit() as u64;
                        } else {
                            spin_loop();
                        }
                    }
                },
                move || {
                    let (mut received, mut sum) = (0u64, 0u64);
                    while received < n {
                        let want = (n - received).min(batch as u64) as usize;
                        if let Some(reservation) = rx.try_read(want) {
                            let count = reservation.len();
                            let (first, second) = reservation.as_slices();
                            for &value in first.iter().chain(second) {
                                sum = sum.wrapping_add(black_box(value));
                            }
                            reservation.advance(count);
                            received += count as u64;
                        } else {
                            spin_loop();
                        }
                    }
                    sum
                },
                n,
            )
        }
    }
}

fn batch_median(mode: BatchMode, capacity: usize, batch: usize, n: u64, runs: usize) -> Duration {
    batch_once(mode, capacity, batch, 100_000); // warm up
    let mut times: Vec<_> = (0..runs)
        .map(|_| batch_once(mode, capacity, batch, n))
        .collect();
    times.sort_unstable();
    times[runs / 2]
}

fn batches() {
    const CAPACITY: usize = 1024;
    const N: u64 = 5_000_000;
    const RUNS: usize = 7;

    println!("\n=== 6. Batch sizes (capacity={CAPACITY}, messages={N}, median of {RUNS}) ===");
    println!("Same 1u64 payloads, spinning policy and checksum in each mode.");
    let std = batch_median(BatchMode::StdSpinning, CAPACITY, 1, N, RUNS);
    let scalar = batch_median(BatchMode::CustomScalar, CAPACITY, 1, N, RUNS);
    let throughput = |elapsed: Duration| N as f64 / elapsed.as_secs_f64() / 1e6;
    println!("{:>16}: {:>9.2} M/s", "std spinning", throughput(std));
    println!("{:>16}: {:>9.2} M/s", "custom scalar", throughput(scalar));
    println!("{:>16}  {:>12}  {:>12}  {:>10}", "batch size", "copy M/s", "reserve M/s", "copy/scalar");
    for batch in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048] {
        let elapsed = batch_median(BatchMode::CustomBatch, CAPACITY, batch, N, RUNS);
        let reserved = batch_median(BatchMode::CustomReservation, CAPACITY, batch, N, RUNS);
        let speedup = scalar.as_secs_f64() / elapsed.as_secs_f64();
        println!("{:>16}  {:>12.2}  {:>12.2}  {:>9.2}x", batch, throughput(elapsed), throughput(reserved), speedup);
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "all".to_owned());
    match mode.as_str() {
        "throughput" => throughput(),
        "asymmetry" => asymmetry(),
        "pacing" => pacing(),
        "pingpong" => pingpong(),
        "batch" => batches(),
        "all" => {
            throughput();
            asymmetry();
            pacing();
            pingpong();
            batches();
        }
        _ => {
            eprintln!("Usage: cargo run --release -- [all|throughput|asymmetry|pacing|pingpong|batch]");
            std::process::exit(2);
        }
    }
}
