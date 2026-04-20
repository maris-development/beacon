use std::{net::SocketAddr, sync::Arc, time::Duration};

use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
use arrow_flight::{
    flight_service_server::FlightServiceServer, sql::client::FlightSqlServiceClient,
};
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint, Server};

use crate::flight_sql::service::BeaconFlightSqlService;

async fn spawn_server(allow_anonymous: bool) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let tmp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = Arc::new(beacon_core::runtime::Runtime::new().await.unwrap());
    let service = BeaconFlightSqlService::new_with_options(runtime, allow_anonymous).unwrap();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::net::TcpStream::connect(addr).is_ok() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Flight SQL test server did not become ready within 5 seconds");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    (addr, handle)
}

async fn client(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Endpoint::new(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn handshake_execute_and_metadata_work() {
    let (addr, handle) = spawn_server(false).await;
    let mut client = client(addr).await;

    client
        .handshake(
            &beacon_config::CONFIG.admin.username,
            &beacon_config::CONFIG.admin.password,
        )
        .await
        .unwrap();
    assert!(client.token().is_some());

    let flight_info = client
        .execute("SELECT 1 AS value".to_string(), None)
        .await
        .unwrap();
    let ticket = flight_info.endpoint[0].ticket.clone().unwrap();
    let batches = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let catalogs = client.get_catalogs().await.unwrap();
    let catalog_batches = client
        .do_get(catalogs.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!catalog_batches.is_empty());

    let schemas = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        })
        .await
        .unwrap();
    let schema_batches = client
        .do_get(schemas.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!schema_batches.is_empty());

    let tables = client
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        })
        .await
        .unwrap();
    let table_batches = client
        .do_get(tables.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!table_batches.is_empty());

    handle.abort();
}

#[tokio::test]
async fn prepared_statement_flow_works() {
    let (addr, handle) = spawn_server(false).await;
    let mut client = client(addr).await;

    client
        .handshake(
            &beacon_config::CONFIG.admin.username,
            &beacon_config::CONFIG.admin.password,
        )
        .await
        .unwrap();

    let mut prepared = client
        .prepare("SELECT 42 AS value".to_string(), None)
        .await
        .unwrap();
    assert_eq!(prepared.dataset_schema().unwrap().fields().len(), 1);

    let flight_info = prepared.execute().await.unwrap();
    let ticket = flight_info.endpoint[0].ticket.clone().unwrap();
    let batches = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    prepared.close().await.unwrap();
    handle.abort();
}

#[tokio::test]
async fn anonymous_metadata_and_select_work() {
    let (addr, handle) = spawn_server(true).await;
    let mut client = client(addr).await;

    let catalogs = client.get_catalogs().await.unwrap();
    let catalog_batches = client
        .do_get(catalogs.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!catalog_batches.is_empty());

    let flight_info = client
        .execute("SELECT 7 AS value".to_string(), None)
        .await
        .unwrap();
    let ticket = flight_info.endpoint[0].ticket.clone().unwrap();
    let batches = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let mut prepared = client
        .prepare("SELECT 9 AS value".to_string(), None)
        .await
        .unwrap();
    let prepared_info = prepared.execute().await.unwrap();
    let prepared_ticket = prepared_info.endpoint[0].ticket.clone().unwrap();
    let prepared_batches = client
        .do_get(prepared_ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(prepared_batches.len(), 1);
    assert_eq!(prepared_batches[0].num_rows(), 1);

    prepared.close().await.unwrap();
    handle.abort();
}

#[tokio::test]
async fn anonymous_write_statement_is_rejected() {
    let (addr, handle) = spawn_server(true).await;
    let mut client = client(addr).await;

    let error = client
        .execute(
            "CREATE VIEW anonymous_view AS SELECT 1 AS value".to_string(),
            None,
        )
        .await
        .unwrap_err();
    let error_message = error.to_string();
    assert!(
        error_message.contains("DDL not supported")
            || error_message.contains("anonymous Flight SQL access only supports")
    );

    handle.abort();
}
