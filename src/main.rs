use futures::executor::block_on;
use rand::prelude::*;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use std::{env, sync::Arc, thread, time::Instant};

#[derive(Debug)]
enum RequestType {
    Put,
    Get,
}

#[derive(Debug)]
struct Stats {
    start_time: Instant,
    end_time: Instant,
    request_type: RequestType,
    file_size: usize,
}

fn main() {
    let num_args = env::args().len();
    if num_args != 5 {
        println!(
            "Usage: {} endpoint bucket root_prefix put_c get_c",
            env::args().nth(0).unwrap()
        );
        std::process::exit(1);
    }

    let endpoint = env::args().nth(1).unwrap();
    let bucket = env::args().nth(2).unwrap();
    let root_prefix = env::args().nth(3).unwrap();
    let put_c = env::args().nth(4).unwrap().parse::<usize>().unwrap();
    let get_c = env::args().nth(5).unwrap().parse::<usize>().unwrap();

    let s3 = S3Client::new(Region::Custom {
        name: "Custom".to_owned(),
        endpoint: endpoint.clone(),
    });

    let stats_vec = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut thread_vec = Vec::new();

    // spawn put threads
    for _ in 0..put_c {
        let s3 = s3.clone();
        let stats_vec = Arc::clone(&stats_vec);
        let endpoint = endpoint.clone();
        let bucket = bucket.clone();
        let root_prefix = root_prefix.clone();
        let t = thread::spawn(move || loop {
            let file_size = thread_rng().gen_range(1024..1024 * 1024 * 100);
            let file_name = format!("rust_{}_put_{}", endpoint, file_size);
            let key = format!("{}/{}/{}", root_prefix, "put", file_name);
            let start_time = Instant::now();
            let body: Vec<u8> = (0..file_size).map(|_| thread_rng().gen()).collect();
            let put_req = PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                body: Some(body.into()),
                ..Default::default()
            };
            match block_on(s3.put_object(put_req)) {
                Ok(_) => {
                    let end_time = Instant::now();
                    let stats = Stats {
                        start_time,
                        end_time,
                        request_type: RequestType::Put,
                        file_size,
                    };
                    stats_vec.lock().unwrap().push(stats);
                }
                Err(RusotoError::HttpDispatch(_)) => {}
                Err(e) => {
                    eprintln!("Error putting object: {:?}", e);
                }
            }
        });
        thread_vec.push(t);
    }

    // spawn get threads
    for _ in 0..get_c {
        let s3 = s3.clone();
        let stats_vec = Arc::clone(&stats_vec);
        let endpoint = endpoint.clone();
        let bucket = bucket.clone();
        let root_prefix = root_prefix.clone();
        let t = thread::spawn(move || loop {
            let file_name = format!(
                "rust_{}_get_{}",
                endpoint,
                thread_rng().gen_range(1024..1024 * 1024 * 100)
            );
            let key = format!("{}/{}/{}", root_prefix, "get", file_name);
            let start_time = Instant::now();
            let get_req = GetObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                ..Default::default()
            };
            match block_on(s3.get_object(get_req)) {
                Ok(_) => {
                    let end_time = Instant::now();
                    let stats = Stats {
                        start_time,
                        end_time,
                        request_type: RequestType::Get,
                        file_size: 0,
                    };
                    stats_vec.lock().unwrap().push(stats);
                }
                Err(RusotoError::HttpDispatch(_)) => {}
                Err(e) => {
                    eprintln!("Error getting object: {:?}", e);
                }
            }
        });
        thread_vec.push(t);
    }

    for t in thread_vec {
        t.join().expect("Thread failed");
    }

    let mut put_count = 0;
    let mut get_count = 0;
    let mut put_time = 0;
    let mut get_time = 0;
    let mut put_file_size = 0;
    let stat_vec = stats_vec.lock().unwrap();
    for i in stat_vec.iter() {
        match i.request_type {
            RequestType::Put => {
                put_count += 1;
                put_time += i.end_time.duration_since(i.start_time).as_millis() as u128;
                put_file_size += i.file_size;
            }
            RequestType::Get => {
                get_count += 1;
                get_time += i.end_time.duration_since(i.start_time).as_millis() as u128;
            }
        }
    }
    let put_avg_time = put_time / put_count as u128;
    let get_avg_time = get_time / get_count as u128;

    println!(
        "PUT stats: count={}, total_time={}ms, avg_time={}ms, total_size={} bytes",
        put_count, put_time, put_avg_time, put_file_size
    );
    println!(
        "GET stats: count={}, total_time={}ms, avg_time={}ms",
        get_count, get_time, get_avg_time
    );
}
