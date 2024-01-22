use anyhow::anyhow;
use arrow_flight as flight;
use arrow_flight::decode::FlightRecordBatchStream;
use base64::Engine;
use tonic::transport::Channel;
use futures_util::StreamExt;
use base64::engine::general_purpose::STANDARD as b64_encoder;
use configparser::ini::Ini;

#[tokio::main]
async fn main() -> anyhow::Result<()>{

    let config = Ini::new().load("../config.ini").or_else(|e| Result::Err(anyhow!("{}", e)))?;
    let es_config = &config["elasticsearch"];

    let login = es_config["login"].as_ref().unwrap();
    let password = es_config["password"].as_ref().unwrap();

    let channel = Channel::from_static("http://localhost:33333")
        .connect()
        .await?;

    let mut client = flight::FlightClient::new(channel);

    let basic_auth = format!("Basic {}", b64_encoder.encode(format!("{login}:{password}")));
    client.add_header("authorization", &basic_auth)?;

    let request = flight::Ticket::new("from employees | stats count = count(id) by language");

    let mut stream: FlightRecordBatchStream = client
        .do_get(request)
        .await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("{:?}", batch);
    }

    Vec::<i32>::new().sort();

    Ok(())
}

