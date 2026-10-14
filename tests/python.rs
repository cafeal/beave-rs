#![cfg(feature = "python")]

use beavers::{Emit, Error, Handler, HandlerError, handlers::Python};
use serde_json::{Value, json};

async fn run(code: &str, input: Value) -> beavers::Result<Emit<Value>> {
    Python::new(code).unwrap().handle(input).await
}

fn values(emit: Emit<Value>) -> Vec<Value> {
    match emit {
        Emit::None => vec![],
        Emit::One(value) => vec![value],
        Emit::Many(values) => values,
    }
}

#[tokio::test]
async fn returns_none_one_value_or_a_generator() {
    let code = r#"
def handle(input):
    if input["status"] != "paid":
        return None
    return {"order_id": input["id"], "tags": ["a", "b"], "note": None}
"#;

    let paid = run(code, json!({"id": 1, "status": "paid"})).await.unwrap();
    assert_eq!(
        values(paid),
        [json!({"order_id": 1, "tags": ["a", "b"], "note": null})]
    );
    let open = run(code, json!({"id": 2, "status": "open"})).await.unwrap();
    assert!(matches!(open, Emit::None));

    let split = r#"
def handle(input):
    for item in input["items"]:
        yield {"item": item}
"#;
    let many = run(split, json!({"items": ["x", "y"]})).await.unwrap();
    assert!(matches!(&many, Emit::Many(_)));
    assert_eq!(values(many), [json!({"item": "x"}), json!({"item": "y"})]);

    let list = run("def handle(input):\n    return [1, 2]\n", json!(null))
        .await
        .unwrap();
    assert_eq!(values(list), [json!([1, 2])]);
}

#[tokio::test]
async fn exceptions_reject_and_beavers_retry_retries() {
    let rejected = run(
        "def handle(input):\n    raise ValueError('bad order')\n",
        json!({}),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&rejected, HandlerError::Reject(error) if error.to_string().contains("ValueError: bad order"))
    );

    let retried = run(
        "def handle(input):\n    raise beavers.Retry('rate limited')\n",
        json!({}),
    )
    .await
    .unwrap_err();
    assert!(matches!(&retried, HandlerError::Retry(error) if error.to_string() == "rate limited"));

    let imported = run(
        "import beavers as b\ndef handle(input):\n    raise b.Retry('again')\n",
        json!({}),
    )
    .await
    .unwrap_err();
    assert!(matches!(imported, HandlerError::Retry(_)));

    let unsupported = run("def handle(input):\n    return object()\n", json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(&unsupported, HandlerError::Reject(error) if error.to_string().contains("not JSON"))
    );
}

#[test]
fn invalid_code_is_reported_when_loading() {
    let syntax = Python::new("def handle(input)\n    return input\n")
        .err()
        .unwrap();
    assert!(matches!(syntax, Error::Config(message) if message.contains("SyntaxError")));

    let missing = Python::new("x = 1\n").err().unwrap();
    assert!(matches!(missing, Error::Config(message) if message.contains("handle(input)")));
}

#[tokio::test]
async fn keeps_module_globals_between_inputs() {
    let counter = Python::new(
        "count = 0\ndef handle(input):\n    global count\n    count += 1\n    return count\n",
    )
    .unwrap();
    counter.handle(json!(null)).await.unwrap();
    let second = counter.handle(json!(null)).await.unwrap();
    assert_eq!(values(second), [json!(2)]);
}
