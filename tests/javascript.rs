#![cfg(feature = "javascript")]

use beavers::{Emit, Error, Handler, HandlerError, handlers::JavaScript};
use serde_json::{Value, json};

async fn run(code: &str, input: Value) -> beavers::Result<Emit<Value>> {
    JavaScript::new(code).unwrap().handle(input).await
}

fn values(emit: Emit<Value>) -> Vec<Value> {
    match emit {
        Emit::None => vec![],
        Emit::One(value) => vec![value],
        Emit::Many(values) => values,
    }
}

#[tokio::test]
async fn returns_one_output_or_nothing() {
    let code = r#"
        function handle(input) {
          if (input.status !== "paid") return;
          return { order_id: input.id, tags: ["a", "b"] };
        }
    "#;

    let paid = run(code, json!({"id": 1, "status": "paid"})).await.unwrap();
    assert_eq!(values(paid), [json!({"order_id": 1, "tags": ["a", "b"]})]);
    let open = run(code, json!({"id": 2, "status": "open"})).await.unwrap();
    assert!(matches!(open, Emit::None));
    let null = run("function handle(input) { return null; }", json!(1))
        .await
        .unwrap();
    assert!(matches!(null, Emit::None));
    let array = run("const handle = (input) => [input, input];", json!(3))
        .await
        .unwrap();
    assert_eq!(values(array), [json!([3, 3])]);
}

#[tokio::test]
async fn generators_emit_one_output_per_yield() {
    let code = r#"
        function* handle(input) {
          for (const item of input.items) yield { order_id: input.id, item };
        }
    "#;

    let output = run(code, json!({"id": 1, "items": ["a", null]}))
        .await
        .unwrap();
    assert_eq!(
        values(output),
        [
            json!({"order_id": 1, "item": "a"}),
            json!({"order_id": 1, "item": null})
        ]
    );
    let empty = run(code, json!({"id": 2, "items": []})).await.unwrap();
    assert!(matches!(empty, Emit::None));
}

#[tokio::test]
async fn errors_reject_and_beavers_retry_retries() {
    let rejected = run(
        r#"function handle() { throw new Error("bad order"); }"#,
        json!({}),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&rejected, HandlerError::Reject(error) if error.to_string().contains("bad order"))
    );

    let retried = run(
        r#"function handle() { throw new beavers.Retry("rate limited"); }"#,
        json!({}),
    )
    .await
    .unwrap_err();
    assert!(matches!(&retried, HandlerError::Retry(error) if error.to_string() == "rate limited"));

    for code in [
        "function handle() { return () => 1; }",
        "async function handle() { return 1; }",
    ] {
        let unsupported = run(code, json!({})).await.unwrap_err();
        assert!(matches!(unsupported, HandlerError::Reject(_)), "{code}");
    }
}

#[test]
fn invalid_code_is_reported_when_created() {
    let syntax = JavaScript::new("function handle( {").err().unwrap();
    assert!(matches!(syntax, Error::Config(message) if message.contains("handler:1")));
    let thrown = JavaScript::new(r#"throw new Error("boom")"#).err().unwrap();
    assert!(matches!(thrown, Error::Config(message) if message.contains("boom")));
    let missing = JavaScript::new("const x = 1;").err().unwrap();
    assert!(matches!(missing, Error::Config(message) if message.contains("handle")));
}

#[tokio::test]
async fn runs_without_host_access_and_keeps_globals() {
    let output = run(
        r#"function handle() {
          return [typeof require, typeof process, typeof fetch, typeof std, typeof os];
        }"#,
        json!(null),
    )
    .await
    .unwrap();
    assert_eq!(
        values(output),
        [json!([
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined"
        ])]
    );

    let counter = JavaScript::new("let count = 0; function handle() { return ++count; }").unwrap();
    counter.handle(json!(null)).await.unwrap();
    let second = counter.handle(json!(null)).await.unwrap();
    assert_eq!(values(second), [json!(2)]);
}
