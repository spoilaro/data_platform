use serde_derive::{Deserialize, Serialize};
use serde_json::Result;
use std::thread::sleep;
use std::time::Duration;
use std::{fs::OpenOptions, io::Write};
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

use std::str;
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
struct InputSource {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    input: InputSource,
    output: String,
    interval: u64,
}

async fn get_config() -> Result<Config> {
    let config = {
        let config_raw = fs::read_to_string("config/collector.json").await.unwrap();
        serde_json::from_str::<Config>(&config_raw).unwrap()
    };
    Ok(config)
}

async fn dump_data(data: &str) {
    let stream = TcpStream::connect("localhost:8001").await.unwrap();
    let mut writer = BufWriter::new(stream);

    if data == "" {
        return;
    }

    println!("LINE: {:?}", data);
    writer.write_all(data.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
}

/// Cleans the output file everytime the service is restarted
async fn clean_file(path: &String) {
    let mut file = File::create(path).await.unwrap();

    file.write("".as_bytes()).await.unwrap();
}

#[tokio::main]
async fn main() {
    // Reads the config from config/collector.json
    let config = get_config().await.unwrap();
    let file = File::open(config.input.path).await.unwrap();
    let mut reader = BufReader::new(file);

    clean_file(&config.output).await;

    loop {
        println!("Checking for more logs");
        let buffer = reader.fill_buf().await.unwrap();

        if buffer.len() > 0 {
            println!("Found more longs, dumping...");
            let lines = std::str::from_utf8(buffer).unwrap();
            // let lines = lines.split("\n");

            // for line in lines {
            dump_data(lines).await;
            // }
        } else {
            println!("No new logs found...")
        }

        // Tells the reader not to return any more read bytes
        let length = buffer.len();
        reader.consume(length);
        sleep(Duration::from_secs(config.interval));
    }
}
