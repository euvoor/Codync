use super::*;
use crate::{hub::Hub, market::save_connectors, remote::identity::Identity, store::Store};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

struct Fixture {
    hub: Arc<Hub>,
    dir: PathBuf,
    gate: TcpListener,
    original: Connector,
}

impl Fixture {
    async fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("codync-edit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let original: Connector = serde_json::from_value(json!({
            "id": "local", "name": "Local", "description": "original",
            "command": if cfg!(windows) { "python" } else { "python3" },
            "args": [fixture_script(), "original"],
            "env": {"API_KEY": "test-private-key", "EDIT_GATE_PORT": gate.local_addr().unwrap().port().to_string()}
        }))
        .unwrap();
        save_connectors(&store, std::slice::from_ref(&original)).unwrap();
        let hub = Hub::new(store, "test".into(), Identity::load_or_create(&dir).unwrap(), "token".into(), 0);
        Self { hub, dir, gate, original }
    }

    async fn waiting(&self, variant: &str) -> TcpStream {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (stream, _) = self.gate.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line.trim(), variant);
            reader.into_inner()
        })
        .await
        .expect("fixture reached tools/list")
    }

    fn edit(&self, patch: Value) -> tokio::task::JoinHandle<Result<Value>> {
        let hub = self.hub.clone();
        tokio::spawn(async move { update_custom_connector(&hub.store, &patch, 0).await })
    }

    fn stored(&self) -> Vec<Connector> {
        connectors(&self.hub.store).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Best-effort cleanup must not mask an assertion failure during unwinding.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn fixture_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/connector_edit_mcp.py")
}

async fn release(mut gate: TcpStream) {
    gate.write_all(b"x").await.unwrap();
}

async fn finished(task: tokio::task::JoinHandle<Result<Value>>) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(15), task).await.expect("edit finished").unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn overlapping_command_and_args_edits_cannot_save_an_unchecked_combination() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    let command = f.dir.join("new-command");
    std::fs::write(&command, "#!/bin/sh\nNEW_COMMAND=1 exec python3 \"$@\"\n").unwrap();
    std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o700)).unwrap();
    let first = f.edit(json!({"id":"local", "command": command}));
    let first_gate = f.waiting("original").await;
    let second = f.edit(json!({"id":"local", "args":[fixture_script(), "changed"]}));
    let second_gate = f.waiting("changed").await;
    release(first_gate).await;
    assert_eq!(finished(first).await.unwrap()["verify"]["status"], "ready");
    let winner = f.stored();
    let ciphertext = f.hub.store.kv_get("connectors");
    release(second_gate).await;
    let error = finished(second).await.expect_err("the second edit checked an outdated command");
    assert!(error.to_string().contains("changed during verification"));
    assert_eq!(f.stored(), winner);
    assert_eq!(f.hub.store.kv_get("connectors"), ciphertext, "rejection must not write");
    assert_eq!(winner[0].command.as_deref(), command.to_str());
    assert_eq!(winner[0].args, f.original.args);
    assert_eq!(winner[0].env, f.original.env);
    let mut incompatible = winner[0].clone();
    incompatible.args = vec![fixture_script().to_str().unwrap().into(), "changed".into()];
    assert!(
        verify::check_connector(&f.hub.store, &incompatible, 0).await.is_err(),
        "the combined settings cannot start"
    );
}

#[tokio::test]
async fn a_metadata_change_during_verification_is_preserved() {
    let f = Fixture::new().await;
    let edit = f.edit(json!({"id":"local", "args":[fixture_script(), "changed"]}));
    let gate = f.waiting("changed").await;
    update_connectors(&f.hub.store, |all| {
        all[0].name = "Renamed elsewhere".into();
        Ok(())
    })
    .unwrap();
    let winner = f.stored();
    let ciphertext = f.hub.store.kv_get("connectors");
    release(gate).await;
    assert!(finished(edit).await.unwrap_err().to_string().contains("changed during verification"));
    assert_eq!(f.stored(), winner);
    assert_eq!(f.hub.store.kv_get("connectors"), ciphertext);
}

#[tokio::test]
async fn deletion_during_verification_does_not_recreate_the_connector() {
    let f = Fixture::new().await;
    let edit = f.edit(json!({"id":"local", "description":"pending"}));
    let gate = f.waiting("original").await;
    update_connectors(&f.hub.store, |all| {
        all.clear();
        Ok(())
    })
    .unwrap();
    let ciphertext = f.hub.store.kv_get("connectors");
    release(gate).await;
    assert!(finished(edit).await.unwrap_err().to_string().contains("Unknown connector"));
    assert!(f.stored().is_empty());
    assert_eq!(f.hub.store.kv_get("connectors"), ciphertext);
}

#[tokio::test]
async fn another_connector_can_change_and_a_fresh_edit_can_follow() {
    let f = Fixture::new().await;
    let mut other = f.original.clone();
    other.id = "other".into();
    save_connectors(&f.hub.store, &[f.original.clone(), other.clone()]).unwrap();
    let edit = f.edit(json!({"id":"local", "description":"first"}));
    let gate = f.waiting("original").await;
    other.name = "Concurrent rename".into();
    update_connectors(&f.hub.store, |all| {
        all[1] = other.clone();
        Ok(())
    })
    .unwrap();
    release(gate).await;
    let answer = finished(edit).await.unwrap();
    assert_eq!(answer["verify"]["status"], "ready");
    assert!(!answer.to_string().contains("test-private-key"));
    let mut expected = f.original.clone();
    expected.description = "first".into();
    assert_eq!(f.stored(), [expected.clone(), other.clone()]);
    let next = f.edit(json!({"id":"local", "description":"second"}));
    release(f.waiting("original").await).await;
    finished(next).await.unwrap();
    expected.description = "second".into();
    assert_eq!(f.stored(), [expected, other]);
}

#[tokio::test]
async fn http_conflict_keeps_bot_assignments_and_does_not_refresh_tools() {
    let f = Fixture::new().await;
    // No actor is running: reaching refresh_tools_for_bots_using would fail with "unknown bot".
    for (id, selected) in [("uses-local", vec!["local"]), ("unrelated", vec![])] {
        f.hub
            .store
            .save_bot(
                &serde_json::from_value(json!({
                    "id":id, "name":id, "backend":"claude", "cwd":f.dir, "connectors":selected
                }))
                .unwrap(),
            )
            .unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = crate::api::router(f.hub.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
    });
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://{address}/api/updateConnector"))
            .bearer_auth("token")
            .json(&json!({"id":"local", "description":"pending"}))
            .send()
            .await
            .unwrap()
    });
    let gate = f.waiting("original").await;
    update_connectors(&f.hub.store, |all| {
        all[0].name = "Winner".into();
        Ok(())
    })
    .unwrap();
    let ciphertext = f.hub.store.kv_get("connectors");
    release(gate).await;
    let response = tokio::time::timeout(Duration::from_secs(15), request).await.unwrap().unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = response.json::<Value>().await.unwrap();
    assert_eq!(error, json!({"error":"The connector changed during verification. Read it again and retry."}));
    assert_eq!(f.hub.store.kv_get("connectors"), ciphertext);
    assert_eq!(f.hub.store.bot("uses-local").unwrap().unwrap().config.connectors, ["local"]);
    assert!(f.hub.store.bot("unrelated").unwrap().unwrap().config.connectors.is_empty());
    server.abort();
    let _ = server.await;
}
