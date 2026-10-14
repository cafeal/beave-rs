#![cfg(feature = "lua")]

use beavers::{Emit, Error, Handler, HandlerError, handlers::Lua};
use serde_json::{Value, json};

async fn run(code: &str, input: Value) -> beavers::Result<Emit<Value>> {
    Lua::new(code).unwrap().handle(input).await
}

fn values(emit: Emit<Value>) -> Vec<Value> {
    match emit {
        Emit::None => vec![],
        Emit::One(value) => vec![value],
        Emit::Many(values) => values,
    }
}

#[tokio::test]
async fn returns_one_output_per_returned_value() {
    let code = r#"
        if input.status ~= "paid" then return nil end
        return { order_id = input.id, tags = { "a", "b" } }
    "#;

    let paid = run(code, json!({"id": 1, "status": "paid"})).await.unwrap();
    assert_eq!(values(paid), [json!({"order_id": 1, "tags": ["a", "b"]})]);
    let open = run(code, json!({"id": 2, "status": "open"})).await.unwrap();
    assert!(matches!(open, Emit::None));
    let many = run("return input, input.id", json!({"id": 3}))
        .await
        .unwrap();
    assert_eq!(values(many), [json!({"id": 3}), json!(3)]);
    let nothing = run("local _ = input", json!(1)).await.unwrap();
    assert!(matches!(nothing, Emit::None));
}

#[tokio::test]
async fn json_null_is_beavers_null() {
    let output = run(
        "return { missing = input.value == beavers.null, value = beavers.null }",
        json!({"value": null}),
    )
    .await
    .unwrap();
    assert_eq!(values(output), [json!({"missing": true, "value": null})]);
}

#[tokio::test]
async fn errors_reject_and_beavers_retry_retries() {
    let rejected = run(r#"error("bad order")"#, json!({})).await.unwrap_err();
    assert!(
        matches!(&rejected, HandlerError::Reject(error) if error.to_string().contains("bad order"))
    );

    let retried = run(r#"beavers.retry("rate limited")"#, json!({}))
        .await
        .unwrap_err();
    assert!(matches!(&retried, HandlerError::Retry(error) if error.to_string() == "rate limited"));

    let unsupported = run("return function() end", json!({})).await.unwrap_err();
    assert!(matches!(unsupported, HandlerError::Reject(_)));
}

#[test]
fn syntax_errors_are_reported_when_compiling() {
    let error = Lua::new("return {").err().unwrap();
    assert!(matches!(error, Error::Config(message) if message.contains("handler:1")));
}

#[tokio::test]
async fn runs_without_io_and_os_libraries_and_keeps_globals() {
    let output = run(
        "return io == nil and os == nil and require == nil",
        json!(null),
    )
    .await
    .unwrap();
    assert_eq!(values(output), [json!(true)]);

    let counter = Lua::new("count = (count or 0) + 1; return count").unwrap();
    counter.handle(json!(null)).await.unwrap();
    let second = counter.handle(json!(null)).await.unwrap();
    assert_eq!(values(second), [json!(2)]);
}

#[tokio::test]
async fn empty_tables_become_objects() {
    let output = run("return {}", json!(null)).await.unwrap();
    assert_eq!(values(output), [json!({})]);
}
