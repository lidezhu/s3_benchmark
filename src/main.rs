use futures::executor::block_on;
use rand::prelude::*;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use std::{env, sync::Arc, time::Instant};
use tokio::io::AsyncReadExt;
use tokio::time::sleep;

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

#[tokio::main]
async fn main() {
    let num_args = env::args().len();
    if num_args != 8 {
        println!(
            "Usage: {} endpoint bucket root_prefix put_thread_num put_per_thread get_thread_num get_per_thread",
            env::args().nth(0).unwrap()
        );
        std::process::exit(1);
    }

    let endpoint = env::args().nth(1).unwrap();
    let bucket = env::args().nth(2).unwrap();
    let root_prefix = env::args().nth(3).unwrap();
    let put_thread_num = env::args().nth(4).unwrap().parse::<usize>().unwrap();
    let put_per_thread = env::args().nth(5).unwrap().parse::<usize>().unwrap();
    let get_thread_num = env::args().nth(6).unwrap().parse::<usize>().unwrap();
    let get_per_thread = env::args().nth(7).unwrap().parse::<usize>().unwrap();

    let s3 = S3Client::new(Region::Custom {
        name: "us-east-2".to_owned(),
        endpoint: endpoint,
    });

    let stats_vec = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut tasks_future = Vec::new();

    // spawn put threads
    for _ in 0..put_thread_num {
        let s3 = s3.clone();
        let stats_vec = Arc::clone(&stats_vec);
        let bucket = bucket.clone();
        let root_prefix = root_prefix.clone();
        let put_task_future = tokio::task::spawn(async move {
            for _ in 0..put_per_thread {
                let file_size = thread_rng().gen_range(1024..1024 * 1024 * 100);
                let file_name = format!("put_{}", file_size);
                let key = format!("{}/{}", root_prefix, file_name);
                let start_time = Instant::now();
                let body: Vec<u8> = (0..file_size).map(|_| thread_rng().gen()).collect();
                let put_req = PutObjectRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    body: Some(body.into()),
                    ..Default::default()
                };
                println!("before put");
                match s3.put_object(put_req).await {
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
                    Err(RusotoError::HttpDispatch(_)) => {
                        println!("HttpDispatch");
                    }
                    Err(e) => {
                        eprintln!("Error putting object: {:?}", e);
                    }
                }
                println!("after put");
            }
        });
        tasks_future.push(put_task_future);
    }

    // spawn get threads
    for _ in 0..get_thread_num {
        let s3 = s3.clone();
        let stats_vec = Arc::clone(&stats_vec);
        let bucket = bucket.clone();
        let root_prefix = root_prefix.clone();

        let get_task_future = tokio::task::spawn(async move {
            for _ in 0..get_per_thread {
                let mut request = ListObjectsV2Request {
                    bucket: bucket.clone(),
                    prefix: Some(root_prefix.clone()),
                    ..Default::default()
                };
                let mut objects = Vec::new();
                loop {
                    let result = s3.list_objects_v2(request.clone()).await.unwrap();
                    objects.extend(result.contents.unwrap());
                    if result.next_continuation_token.is_none() {
                        break;
                    }
                    request.continuation_token = result.next_continuation_token;
                }
                println!("list objects size: {}", objects.len());
                if objects.len() == 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
                let key = objects[thread_rng().gen_range(0..objects.len())]
                    .key
                    .clone()
                    .unwrap();
                let start_time = Instant::now();
                let get_req = GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    ..Default::default()
                };
                println!("before get {}", key);
                match s3.get_object(get_req).await {
                    Ok(resp) => match resp.body {
                        Some(body) => {
                            let mut body = body.into_async_read();
                            let mut buf = Vec::new();
                            body.read_to_end(&mut buf).await.unwrap();
                            let end_time = Instant::now();
                            let stats = Stats {
                                start_time,
                                end_time,
                                request_type: RequestType::Get,
                                file_size: buf.len(),
                            };
                            stats_vec.lock().unwrap().push(stats);
                        }
                        None => {
                            eprintln!("No body in response");
                        }
                    },
                    Err(RusotoError::HttpDispatch(_)) => {}
                    Err(e) => {
                        eprintln!("Error getting object: {:?}", e);
                    }
                }
                println!("after get");
            }
        });
        tasks_future.push(get_task_future);
    }

    println!("waiting for futures");
    let _results = block_on(futures::future::join_all(tasks_future));
    println!("Done!");

    let mut put_count = 0;
    let mut get_count = 0;
    let mut put_time = 0;
    let mut get_time = 0;
    let mut put_file_size = 0;
    let mut get_file_size = 0;
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
                get_file_size += i.file_size;
            }
        }
    }
    let put_avg_time = put_time / put_count as u128;
    let get_avg_time = get_time / get_count as u128;

    println!(
        "PUT stats: count={}, total_time={}ms, avg_time={}ms, total_size={} MB",
        put_count,
        put_time,
        put_avg_time,
        put_file_size / 1024 / 1024
    );
    println!(
        "GET stats: count={}, total_time={}ms, avg_time={}ms, total_size={} MB",
        get_count,
        get_time,
        get_avg_time,
        get_file_size / 1024 / 1024
    );
}
