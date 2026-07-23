mod common;

use std::{net::SocketAddr, time::Duration};

use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint, Server};

/// A running loopback Flight SQL server: its address, the lake it serves (held
/// so its temp root outlives the server), and the serve task's handle.
struct TestServer {
    addr: SocketAddr,
    lake: common::TestLake,
    handle: tokio::task::JoinHandle<()>,
}

/// Spawns a Flight SQL server on an ephemeral loopback port, backed by a fresh
/// ephemeral lake. `allow_anonymous` sets the service's anonymous-access policy.
async fn spawn_server(allow_anonymous: bool) -> TestServer {
    let tmp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let lake = common::test_lake().await;
    let service =
        beacon_datalake::flight_sql::flight_service(lake.lake.clone(), allow_anonymous).unwrap();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
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

    TestServer { addr, lake, handle }
}

async fn run_sql_rows(
    runtime: &beacon_core::runtime::Runtime,
    sql: &str,
) -> Vec<arrow::array::RecordBatch> {
    runtime
        .run_query(beacon_core::query::Query::sql(sql.to_string()), beacon_core::AuthIdentity::system())
        .await
        .expect("query should run")
        .into_record_stream()
        .expect("streamed result")
        .try_collect::<Vec<_>>()
        .await
        .expect("stream should drain")
}

async fn client(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Endpoint::new(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

/// End-to-end federation over a loopback: one Runtime serves `obs` over Flight
/// SQL, and a `remote_obs` table in the same Runtime points back at that port.
/// Querying `remote_obs` pushes a filtered aggregate to the (loopback) remote and
/// streams the reduced result back.
#[tokio::test(flavor = "multi_thread")]
async fn federated_remote_table_pushes_down_and_streams() {
    // Anonymous access on the remote: remote tables connect without credentials.
    let server = spawn_server(true).await;
    let port = server.addr.port();
    let runtime = server.lake.lake.runtime();

    // Unique names so the shared process-wide test state can't collide.
    let suffix = uuid::Uuid::new_v4().simple();
    let obs = format!("obs_{suffix}");
    let remote_obs = format!("remote_obs_{suffix}");

    // Seed the "remote" table.
    run_sql_rows(runtime, &format!("CREATE TABLE {obs} (id BIGINT, val DOUBLE)")).await;
    run_sql_rows(
        runtime,
        &format!("INSERT INTO {obs} VALUES (1, 10.0), (2, 20.0), (3, 30.0)"),
    )
    .await;

    // Register a federated remote table pointing at the loopback Flight SQL port.
    // No credentials — the remote allows anonymous access.
    run_sql_rows(
        runtime,
        &format!(
            "CREATE EXTERNAL TABLE {remote_obs} STORED AS REMOTE \
             LOCATION 'beacon://127.0.0.1:{port}/{obs}'"
        ),
    )
    .await;

    // Schema was fetched from the remote at registration: two columns are
    // visible through the catalog (`information_schema.columns`).
    let column_count = run_sql_rows(
        runtime,
        &format!(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = '{remote_obs}'"
        ),
    )
    .await;
    let columns = column_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count is Int64")
        .value(0);
    assert_eq!(columns, 2, "the remote table should expose its two columns");

    // A filtered aggregate over the remote table returns the correct result.
    let batches = run_sql_rows(
        runtime,
        &format!("SELECT count(*) AS c, sum(val) AS s FROM {remote_obs} WHERE id > 1"),
    )
    .await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column is Int64")
        .value(0);
    assert_eq!(count, 2, "id > 1 should match two rows");

    // The plan federates the scan (pushed to the remote), rather than scanning
    // locally — confirm a federated/virtual node appears in the physical plan.
    let explain = run_sql_rows(
        runtime,
        &format!("EXPLAIN SELECT count(*) FROM {remote_obs} WHERE id > 1"),
    )
    .await;
    let explain_text = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("explain should format")
        .to_string();
    assert!(
        explain_text.contains("Virtual")
            || explain_text.to_lowercase().contains("federat")
            || explain_text.contains("SchemaCast"),
        "expected a federated scan node in the plan, got:\n{explain_text}"
    );

    run_sql_rows(runtime, &format!("DROP TABLE {remote_obs}")).await;
    run_sql_rows(runtime, &format!("DROP TABLE {obs}")).await;

    server.handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_execute_and_metadata_work() {
    let server = spawn_server(false).await;
    let mut client = client(server.addr).await;

    client
        .handshake(common::ADMIN_USERNAME, common::ADMIN_PASSWORD)
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

    server.handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_statement_flow_works() {
    let server = spawn_server(false).await;
    let mut client = client(server.addr).await;

    client
        .handshake(common::ADMIN_USERNAME, common::ADMIN_PASSWORD)
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
    server.handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_metadata_and_select_work() {
    let server = spawn_server(true).await;
    let mut client = client(server.addr).await;

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
    server.handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_write_statement_is_rejected() {
    let server = spawn_server(true).await;
    let mut client = client(server.addr).await;

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

    server.handle.abort();
}
