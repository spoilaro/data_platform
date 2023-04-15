use rusqlite::Connection;

use serde_derive::{Deserialize, Serialize};
use serde_json::Result;
use std::str;
use std::thread::sleep;
use std::time::Duration;
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    data: String,
    interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogRow {
    timestamp: String,
    module: String,
    level: String,
    message: String,
}
async fn get_config() -> Result<Config> {
    let config = {
        let config_raw = fs::read_to_string("config/warehouse.json").await.unwrap();
        serde_json::from_str::<Config>(&config_raw).unwrap()
    };
    Ok(config)
}

fn initialize_db() {
    let conn = Connection::open("warehouse/logs.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS logs (
            timestamp datetime,
            module varchar(30),
            level varchar(10),
            message text,
            PRIMARY KEY(timestamp, module)
        )
        ",
        (),
    )
    .unwrap();

    conn.close().unwrap();
}

/// Parse the plain text data into vector of logrow structs
fn parse_data(buffer: &[u8]) -> Vec<LogRow> {
    let mut rows: Vec<LogRow> = vec![];
    let data = std::str::from_utf8(buffer).unwrap();

    let lines = data.split("\n");

    for line in lines {
        if line == "" {
            continue;
        }
        let tokens = line.split(" ").collect::<Vec<&str>>();

        rows.push(LogRow {
            timestamp: String::from(tokens[0]),
            module: String::from(tokens[1]),
            level: String::from(tokens[2]),
            message: String::from(tokens[3]),
        });
    }

    rows
}

fn save_data(conn: Connection, rows: Vec<LogRow>) {
    for row in rows {
        conn.execute(
            "INSERT OR IGNORE INTO LOGS
            (timestamp, module, level, message)
            values (?1, ?2, ?3, ?4)
            ",
            (&row.timestamp, &row.module, &row.level, &row.message),
        )
        .unwrap();
    }

    conn.close().unwrap();
}

#[tokio::main]
async fn main() {
    // Reads the config from config/warehouse.json
    let config = get_config().await.unwrap();

    // Opens the "datalake" file which is just unstructured data as a text
    let file = File::open(config.data).await.unwrap();
    let mut reader = BufReader::new(file);

    // Database connection
    initialize_db();

    loop {
        println!("Checking for more data");
        let buffer = reader.fill_buf().await.unwrap();

        if buffer.len() > 0 {
            println!("Found new data, parsing and saving");
            let rows = parse_data(buffer);

            let conn = Connection::open("warehouse/logs.db").unwrap();
            save_data(conn, rows);
        } else {
            println!("No new data found...")
        }

        // Tells the reader not to return any more read bytes
        let length = buffer.len();
        reader.consume(length);
        sleep(Duration::from_secs(config.interval));
    }
}
