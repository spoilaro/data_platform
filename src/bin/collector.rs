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

#[derive(Debug, Serialize, Deserialize)]
struct InputSource {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    input: InputSource,
    output: String,
}

async fn get_config() -> Result<Config> {
    let config = {
        let config_raw = fs::read_to_string("config/collector.json").await.unwrap();
        serde_json::from_str::<Config>(&config_raw).unwrap()
    };
    Ok(config)
}

async fn dump_data(data: &[u8], path: &String) {
    // TODO: Check the files size

    let mut file = OpenOptions::new()
        .append(true)
        .open("data/logs.log")
        .expect("Could not open file");

    // let mut writer = BufWriter::new(out_file);
    //
    // writer.write(data).await.unwrap();
    // writer.flush().await.unwrap();
    //
    file.write(data).expect("Could not write");
}

#[tokio::main]
async fn main() {
    // Reads the config from config/collector.json
    let config = get_config().await.unwrap();

    let file = File::open(config.input.path).await.unwrap();

    let mut reader = BufReader::new(file);

    loop {
        println!("New Round");
        let buffer = reader.fill_buf().await.unwrap();
        // let data = core::str::from_utf8(buffer).unwrap();

        // Process the data
        // print!("{}", data);

        dump_data(buffer, &config.output).await;

        // Tells the reader not to return any more read bytes
        let length = buffer.len();
        reader.consume(length);
        sleep(Duration::from_secs(4));
    }
}
