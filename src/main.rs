use std::fs;
use std::fs::File;
use std::io::{Error, Write};
use std::thread;
use std::time::Instant;

const A1: [u8; 1] = [b'a'; 1];
const A10: [u8; 10] = [b'a'; 10];
const A100: [u8; 100] = [b'a'; 100];
const A1000: [u8; 1000] = [b'a'; 1000];

fn main() {
    // TESTING THREADS
    println!("---------------");
    println!("testing different thread counts");
    let threads = vec![1, 2, 4, 16, 64];
    let to_write: Vec<&'static [u8]> = vec![b"a", b"a", b"a", b"a", b"a"];
    let iterations = vec![5_000, 5_000, 5_000, 5_000, 5_000];

    runner(threads, to_write, iterations);

    println!("---------------");
    //

    // TESTING KEY LEN
    println!("---------------");
    println!("testing different key sizes");
    let threads = vec![4, 4, 4, 4];
    let to_write: Vec<&'static [u8]> = vec![&A1, &A10, &A100, &A1000];
    let iterations = vec![5_000, 5_000, 5_000, 5_000];

    runner(threads, to_write, iterations);
    println!("---------------");
    //
}

fn runner(threads: Vec<u64>, to_write: Vec<&'static [u8]>, iterations: Vec<u64>) {
    for i in 0..threads.len() {
        let mut handles = vec![];
        for t in 0..threads[i] {
            let writing = to_write[i];
            let iteration = iterations[i];
            handles.push(thread::spawn(move || {
                run_experiment(t, writing, iteration, thread_work)
            }));
        }

        println!("{} threads", threads[i]);
        println!("{} iterations", iterations[i]);
        let mut times = vec![];
        let mut throughputs = vec![];
        for handle in handles {
            let time = handle.join().unwrap().expect("thread returned err");
            times.push(time);
            throughputs.push(iterations[i] as f64 / time);
        }

        print_aggregates(times, throughputs, threads[i]);
    }
}

fn run_experiment(
    i: u64,
    to_write: &[u8],
    iterations: u64,
    experiment: fn(&[u8], u64, File) -> Result<f64, Error>,
) -> Result<f64, Error> {
    let file = File::create(format!("wal_{}.log", i))?; // make each wal unique
    let time = experiment(to_write, iterations, file)?;
    fs::remove_file(format!("wal_{}.log", i))?;

    Ok(time)
}

fn thread_work(to_write: &[u8], iterations: u64, mut file: File) -> Result<f64, Error> {
    let start = Instant::now();
    for _ in 0..iterations {
        file.write_all(to_write)?;
        file.sync_all()?;
    }

    let end = start.elapsed();
    Ok(end.as_secs_f64())
}

fn print_aggregates(times: Vec<f64>, throughputs: Vec<f64>, threads: u64) {
    let average_time = times.clone().into_iter().sum::<f64>() / times.len() as f64;
    let average_throughput =
        throughputs.clone().into_iter().sum::<f64>() / throughputs.len() as f64;
    let total_writes = average_throughput * threads as f64;
    println!("Average time to complete writes: {:.3}", average_time);
    println!(
        "Average throughput of thread: {:.3} writes per second",
        average_throughput
    );
    println!(
        "Estimated total writes to WAL per second: {:.1}",
        total_writes
    );
    println!();
}
