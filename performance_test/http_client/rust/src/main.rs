// To run this test, you'll need the following dependencies in your Cargo.toml:
// reqwest = { version = "0.12", features = ["json"] }
// tokio = { version = "1", features = ["full"] }
// futures = "0.3"

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const URL: &str = "http://nginx:21000";

struct Counters {
    success: AtomicU64,
    failed: AtomicU64,
}

/// Performs a single HTTP GET request.
async fn fetch(client: &reqwest::Client) -> Result<(), reqwest::Error> {
    let response = client.get(URL).send().await?;
    // We don't need the body, but reading it ensures the connection
    // is released back to the pool properly.
    response.bytes().await?;
    Ok(())
}

/// A worker that runs until cancelled, performing requests and counting results.
async fn worker(
    client: reqwest::Client,
    counters: Arc<Counters>,
    start_barrier: Arc<tokio::sync::Barrier>,
) {
    // Wait for the signal to start.
    start_barrier.wait().await;
    loop {
        // The task will be cancelled on the next await point when abort() is called.
        match fetch(&client).await {
            Ok(_) => {
                counters.success.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                counters.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Runs the workload for a specified duration and concurrency, and returns the results.
async fn run_test(duration_secs: u64, concurrency: u32) -> (u64, u64, f64) {
    let counters = Arc::new(Counters {
        success: AtomicU64::new(0),
        failed: AtomicU64::new(0),
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let start_barrier = Arc::new(tokio::sync::Barrier::new(concurrency as usize + 1));
    let mut tasks = Vec::new();

    for _ in 0..concurrency {
        let client_clone = client.clone();
        let counters_clone = counters.clone();
        let barrier_clone = start_barrier.clone();
        tasks.push(tokio::spawn(worker(
            client_clone,
            counters_clone,
            barrier_clone,
        )));
    }

    // All tasks are created and waiting. Now, signal them to start and begin timing.
    start_barrier.wait().await;
    let start_time = Instant::now();

    // Let the workers run for the specified duration.
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;

    // Cancel all worker tasks to stop them gracefully.
    for task in &tasks {
        task.abort();
    }
    // Wait for all tasks to finish their cancellation.
    let _ = futures::future::join_all(tasks).await;

    let actual_duration = start_time.elapsed().as_secs_f64();
    let success = counters.success.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);

    (success, failed, actual_duration)
}

async fn single_core_test(duration: u64, concurrency: u32) -> f64 {
    println!(
        "Starting single-core test for {}s with a concurrency of {}...",
        duration, concurrency
    );

    let (success, failed, actual_duration) = run_test(duration, concurrency).await;

    println!("\n--- Single-Core Test Results ---");
    println!("Test ran for: {:.2} seconds", actual_duration);
    println!("Successful requests: {}", success);
    println!("Failed requests: {}", failed);

    let rps = if actual_duration > 0.0 {
        success as f64 / actual_duration
    } else {
        0.0
    };
    println!("Successful requests per second (RPS): {:.0}", rps);
    rps
}

/// The target function for each process in the multi-core test.
fn process_worker(duration: u64, concurrency: u32) -> (u64, u64, f64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(run_test(duration, concurrency))
}

/// Runs the test across multiple processes to utilize multiple CPU cores.
fn multi_core_test(num_processes: usize, duration: u64, concurrency: u32) -> f64 {
    println!(
        "Starting multi-core test for {}s with a concurrency of {} per process across {} processes...",
        duration, concurrency, num_processes
    );

    let mut handles = vec![];
    let start_time = Instant::now();

    for _ in 0..num_processes {
        let handle = std::thread::spawn(move || process_worker(duration, concurrency));
        handles.push(handle);
    }

    let mut total_success = 0;
    let mut total_failed = 0;
    let mut total_rps = 0.0;

    for handle in handles {
        let (success, failed, duration_thread) = handle.join().unwrap();
        total_success += success;
        total_failed += failed;
        if duration_thread > 0.0 {
            total_rps += success as f64 / duration_thread;
        }
    }

    let total_duration = start_time.elapsed().as_secs_f64();

    println!("\n--- Multi-Core Test Results ---");
    println!("Test ran for: {:.2} seconds", total_duration);
    println!("Total successful requests: {}", total_success);
    println!("Total failed requests: {}", total_failed);
    println!("Aggregated RPS (sum of RPS from each process): {:.0}", total_rps);

    total_rps
}

#[tokio::main]
async fn main() {
    single_core_test(30, 50).await;
    multi_core_test(4, 30, 50);
}
