use rusqlite::Connection;

use serde_derive::{Deserialize, Serialize};
use serde_json::Result;
use std::error::Error;
use std::str;
use std::thread::sleep;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

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
fn parse_data(buffer: String) -> LogRow {
    let tokens = buffer.split(" ").collect::<Vec<&str>>();

    let row = LogRow {
        timestamp: String::from(tokens[0]),
        module: String::from(tokens[1]),
        level: String::from(tokens[2]),
        message: String::from(tokens[3]),
    };

    row
}

fn save_data(row: LogRow) {
    let conn = Connection::open("warehouse/logs.db").unwrap();

    conn.execute(
        "INSERT OR IGNORE INTO LOGS
            (timestamp, module, level, message)
            values (?1, ?2, ?3, ?4)
            ",
        (&row.timestamp, &row.module, &row.level, &row.message),
    )
    .unwrap();

    conn.close().unwrap();
}

#[tokio::main]
async fn main() {
    // Reads the config from config/warehouse.json
    let config = get_config().await.unwrap();

    // Database connection
    initialize_db();

    let address = "localhost:8001";

    // Binds the address set above
    let listener = TcpListener::bind(address).await.unwrap();

    println!("\nStarting the server, address: {}", address);

    loop {
        // Gets the socket and the address of the connected client
        let (socket, addr) = listener.accept().await.unwrap();

        // New thread for each of the clients
        tokio::spawn(async move {
            // let (reader, _writer) = socket.split();
            let mut reader = BufReader::new(socket);

            // reader.read_line(&mut lines).await.unwrap();
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await.unwrap() {
                println!("LINE: {}", line);
                let row = parse_data(line);
                save_data(row);
            }
        });
    }
}
