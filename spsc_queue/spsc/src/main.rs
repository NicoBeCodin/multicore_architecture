
mod spsc;

use spsc::Spsc;

use std::hint::{black_box, spin_loop};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CAPACITY: usize = 1024;
const MESSAGES: u64 = 1_000_000;
const RUNS: usize = 7;

fn expected_sum(n: u64) -> u64 {
    (n as u128 * (n as u128 - 1) / 2) as u64
}

// A: Standard bounded channel, blocking.
fn bench_std_blocking(n: u64) -> Duration {
    let (tx, rx) = mpsc::sync_channel::<u64>(CAPACITY);
    let barrier = Arc::new(Barrier::new(3));

    let p_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        p_barrier.wait();

        for i in 0..n {
            tx.send(black_box(i)).unwrap();
        }
    });

    let c_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        c_barrier.wait();

        let mut sum = 0u64;
        for _ in 0..n {
            let value = rx.recv().unwrap();
            sum = sum.wrapping_add(black_box(value));
        }
        sum
    });

    let start = Instant::now();
    barrier.wait();

    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(sum, expected_sum(n));
    elapsed
}

// B: Standard bounded channel, nonblocking + spinning.
fn bench_std_spinning(n: u64) -> Duration {
    let (tx, rx) = mpsc::sync_channel::<u64>(CAPACITY);
    let barrier = Arc::new(Barrier::new(3));

    let p_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        p_barrier.wait();

        for i in 0..n {
            let mut value = black_box(i);

            loop {
                match tx.try_send(value) {
                    Ok(()) => break,
                    Err(mpsc::TrySendError::Full(v)) => {
                        value = v;
                        spin_loop();
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        panic!("Receiver disconnected");
                    }
                }
            }
        }
    });

    let c_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        c_barrier.wait();

        let mut sum = 0u64;

        for _ in 0..n {
            let value = loop {
                match rx.try_recv() {
                    Ok(v) => break v,
                    Err(mpsc::TryRecvError::Empty) => spin_loop(),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("Producer disconnected");
                    }
                }
            };

            sum = sum.wrapping_add(black_box(value));
        }

        sum
    });

    let start = Instant::now();
    barrier.wait();

    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(sum, expected_sum(n));
    elapsed
}

// C: Your custom SPSC, nonblocking + spinning.
fn bench_custom_spsc(n: u64) -> Duration {
    let (mut tx, mut rx) = Spsc::<u64>::new(CAPACITY);
    let barrier = Arc::new(Barrier::new(3));

    let p_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        p_barrier.wait();

        for i in 0..n {
            let mut value = black_box(i);

            loop {
                match tx.push(value) {
                    Ok(()) => break,
                    Err(v) => {
                        value = v;
                        spin_loop();
                    }
                }
            }
        }
    });

    let c_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        c_barrier.wait();

        let mut sum = 0u64;

        for _ in 0..n {
            let value = loop {
                match rx.pop() {
                    Some(v) => break v,
                    None => spin_loop(),
                }
            };

            sum = sum.wrapping_add(black_box(value));
        }

        sum
    });

    let start = Instant::now();
    barrier.wait();

    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(sum, expected_sum(n));
    elapsed
}

fn measure(
    name: &str,
    bench: fn(u64) -> Duration,
) {
    // Warmup
    bench(100_000);

    let mut samples = Vec::with_capacity(RUNS);

    for _ in 0..RUNS {
        samples.push(bench(MESSAGES));
    }

    samples.sort();

    let median = samples[RUNS / 2];
    let seconds = median.as_secs_f64();

    println!(
        "{:<20} {:>10.2} M msg/s  {:>10.2} ns/msg",
        name,
        MESSAGES as f64 / seconds / 1e6,
        seconds * 1e9 / MESSAGES as f64
    );
}

fn main() {
    println!("Capacity: {CAPACITY}");
    println!("Messages per run: {MESSAGES}");
    println!("Runs: {RUNS}\n");

    measure("std blocking", bench_std_blocking);
    measure("std spinning", bench_std_spinning);
    measure("custom SPSC", bench_custom_spsc);
}
