#![allow(dead_code)]
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::io::{Error, Write, BufWriter};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

const BATCH_SIZE: u64 = 10;
const BLOCK_SIZE: usize = 512;
const A1: [u8; 1] = [b'a'; 1];
const A10: [u8; 10] = [b'a'; 10];
const A100: [u8; 100] = [b'a'; 100];
const A1000: [u8; 1000] = [b'a'; 1000];
const ABLOCK: [u8; BLOCK_SIZE] = [b'a'; BLOCK_SIZE];

struct ExperimentConfig {
    threads: Vec<u64>, 
    to_write: Vec<&'static [u8]>, 
    iterations: Vec<u64>,
    experiment: fn(&[u8], u64, u64) -> Result<f64, Error>,
}

// NOTE: Run program with `cargo build --release && sudo target/release/wal-bench`
fn main() {
    let cores = &core_affinity::get_core_ids().unwrap();

    // TESTING THREADS
    println!("---------------");
    println!("testing different thread counts - UNOPTIMZED");
    let config = ExperimentConfig {
        threads: vec![1, 4, 16, 64],
        to_write: vec![&A100, &A100, &A100, &A100],
        iterations: vec![10_000, 10_000, 10_000, 10_000],
        experiment: thread_work_unoptimized
    };

    runner(config, cores);

    println!("---------------");
    //
    
    //
    println!("---------------");
    println!("testing different thread counts - BATCHING, size = {}", BATCH_SIZE);
    let config = ExperimentConfig {
        threads: vec![1, 4, 16, 64],
        to_write: vec![&A100, &A100, &A100, &A100],
        iterations: vec![10_000, 10_000, 10_000, 10_000],
        experiment: thread_work_batching
    };

    runner(config, cores);

    println!("---------------");
    //

    println!("---------------");
    println!("testing different thread counts, writing by block size.");

    let config = ExperimentConfig {
        threads: vec![1, 4, 16, 64],
        to_write: vec![&ABLOCK, &ABLOCK, &ABLOCK, &ABLOCK],
        iterations: vec![10_000, 10_000, 10_000, 10_000],
        experiment: thread_work_unoptimized
    };

    runner(config, cores);

    // 
    println!("---------------");
    println!("testing different thread counts, writing by block size - BATCHING");

    let config = ExperimentConfig {
        threads: vec![1, 4, 16, 64],
        to_write: vec![&ABLOCK, &ABLOCK, &ABLOCK, &ABLOCK],
        iterations: vec![10_000, 10_000, 10_000, 10_000],
        experiment: thread_work_batching
    };
    
    runner(config, cores);
    
    /*
    // TESTING KEY LEN
    println!("---------------");
    println!("testing different key sizes");
    let threads = vec![4, 4, 4, 4];
    let to_write: Vec<&'static [u8]> = vec![&A1, &A10, &A100, &A1000];
    let iterations = vec![5_000, 5_000, 5_000, 5_000];

    runner(threads, to_write, iterations, thread_work_unoptimized);
    println!("---------------");
    //
    */
}


fn runner(config: ExperimentConfig, cores: &Vec<core_affinity::CoreId>) {
    for ((&n_threads, &writing), &iteration) in config.threads.iter().zip(&config.to_write).zip(&config.iterations) {
        let start = Instant::now();
        let barrier = Barrier::new(n_threads as usize);

        let times: Vec<f64> = thread::scope(|s| {
            let mut handles = Vec::with_capacity(n_threads as usize);

            for t in 0..n_threads {
                let barrier = &barrier;
                handles.push(s.spawn(move || {
                    // pin to core
                    let core = cores[(t as usize) % cores.len()];
                    let res = core_affinity::set_for_current(core);
                    if !res {
                        panic!("cannot pin thread");
                    }

                    // drop kernel level page cache entries
                    let mut file = OpenOptions::new()
                        .write(true)
                        .open("/proc/sys/vm/drop_caches")?;
                    file.write_all(b"3\n")?;

                    barrier.wait();
                    (config.experiment)(writing, iteration, t)
                }));
            }

            handles
                .into_iter()
                .map(|h| h.join().expect("thread panicked").expect("experiment failed"))
                .collect()
        });

        let elapsed = start.elapsed().as_secs_f64();
        print_aggregates(times, n_threads, iteration, elapsed);
    } 
}

fn thread_work_unoptimized(to_write: &[u8], iterations: u64, i: u64) -> Result<f64, Error> {
    let mut file = OpenOptions::new().create(true).append(true).open(format!("wal_{}.log", i))?;
    let start = Instant::now();
    for _ in 0..iterations {
        file.write_all(to_write)?;
        file.sync_data()?;
        //file.sync_all()?; sync_all includes all metadata. do we need that?
    }

    let end = start.elapsed();
    fs::remove_file(format!("wal_{}.log", i))?;
    Ok(end.as_secs_f64())
}

fn thread_work_batching(to_write: &[u8], iterations: u64, i: u64) -> Result<f64, Error> {
    let mut file = OpenOptions::new().create(true).append(true).open(format!("wal_{}.log", i))?;
    let start = Instant::now();
    for iter_num in 0..iterations {
        file.write_all(to_write)?;
        if (iter_num + 1) % BATCH_SIZE == 0 {
            file.sync_data()?;
        }
    }
    file.sync_data()?; // leftovers

    let end = start.elapsed();
    fs::remove_file(format!("wal_{}.log", i))?;
    Ok(end.as_secs_f64())
}

// not working - getting invalid argument - probl need to align writes or something
fn thread_work_flags(to_write: &[u8], iterations: u64, i: u64) -> Result<f64, Error> {
    let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .custom_flags(libc::O_DIRECT | libc::O_SYNC)
                    .open(format!("wal_{}.log", i))?;

    let start = Instant::now();
    for _ in 0..iterations {
        file.write_all(to_write)?;
    }

    let end = start.elapsed();
    Ok(end.as_secs_f64())
}

// no benefit - probably not using bufwriter correctly. not sure if using this will yield anything major
fn thread_work_bufwriter(to_write: &[u8], iterations: u64, i: u64) -> Result<f64, Error> {
    let file = OpenOptions::new().create(true).append(true).open(format!("wal_{}.log", i))?;
    let mut writer = BufWriter::new(file);
    let start = Instant::now();
    for iter_num in 0..iterations {
        writer.write_all(to_write)?;
        if (iter_num + 1) % BATCH_SIZE == 0 {
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
    }

    writer.flush()?;
    writer.get_ref().sync_data()?;

    let end = start.elapsed();
    fs::remove_file(format!("wal_{}.log", i))?;
    Ok(end.as_secs_f64())
}

fn print_aggregates(times: Vec<f64>, threads: u64, iterations: u64, elapsed: f64) {
    let avg_thread_time = times.iter().sum::<f64>() / times.len() as f64;
    let total_writes = (threads as f64) * (iterations as f64);
    let writes_per_sec = total_writes / elapsed;

    println!(
        "threads: {:>} | writes to complete: {:>} | average time for thread to complete: {:>6.3} | writes per second {:>6.3}",
        threads,
        iterations,
        avg_thread_time,
        writes_per_sec,
    );
}
