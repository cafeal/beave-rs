# Channel adapter

The channel adapter connects subscriptions with a bounded, typed, process-local
queue. It is useful for composing pipelines inside one application without
serializing values or introducing an external broker.

Create both ends with `channel`:

```rust
use beavers::{channel, App, InMemorySink, IterSource};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (sink, source) = channel(16);
    let output = InMemorySink::default();

    App::new()
        .subscribe(IterSource::new([1, 2, 3]), sink, |value: i32| async move {
            Ok(value * 2)
        })
        .subscribe(source, output.clone(), |value: i32| async move {
            Ok(value + 1)
        })
        .run()
        .await?;

    assert_eq!(output.values(), vec![3, 5, 7]);
    Ok(())
}
```

`ChannelSource::bounded` and `ChannelSink::bounded` expose the corresponding
Tokio channel endpoint when only one adapter side is needed. `ChannelSource::new`
and `ChannelSink::new` wrap an existing bounded Tokio MPSC channel. A capacity of
zero panics, following `tokio::sync::mpsc::channel`.

## Delivery and completion

The queue applies backpressure when it reaches its configured capacity. A
pending publish can be canceled without enqueueing its value. Likewise, a
pending receive can be canceled without consuming a value.

The source returns `Receive::End` only after every sender has been dropped or
closed and all buffered values have been received. Closing a source rejects new
sends while preserving buffered values for draining.

A successful sink publication confirms that the typed value was enqueued. It
does not mean another subscription processed the value or stored it durably.
Channel deliveries therefore have a no-op ACK and cannot be recovered after a
process failure.

## Closing

Cloned sinks share one close state. Closing any clone rejects later publication,
wakes publications waiting for capacity, and closes the queue after already
buffered values have drained. Closing either a source or sink more than once is
safe.

Dropping a sink clone does not close the queue while another clone remains.
Dropping the final sink closes it naturally. Dropping the source causes current
and future publication attempts to fail.
