use beavers::{
    App, ChannelSink, ChannelSource, InMemorySink, IterSource, Receive, Sink, Source,
    SourceMessage, channel,
};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

async fn assert_pending<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn pending_receive_can_be_cancelled_and_end_waits_for_all_senders() {
    let (sender, mut source) = ChannelSource::bounded(2);
    {
        let receive = source.receive();
        tokio::pin!(receive);
        assert_pending(receive.as_mut()).await;
    }
    sender.send(42).await.unwrap();
    let clone = sender.clone();
    drop(sender);
    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("missing message")
    };
    assert_eq!(message.decode().unwrap(), 42);
    message.ack().await.unwrap();
    {
        let receive = source.receive();
        tokio::pin!(receive);
        assert_pending(receive.as_mut()).await;
    }
    drop(clone);
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn sender_drop_drains_buffer_before_end() {
    let (sender, mut source) = ChannelSource::bounded(2);
    sender.send(1).await.unwrap();
    sender.send(2).await.unwrap();
    drop(sender);

    let Receive::Message(first) = source.receive().await.unwrap() else {
        panic!("missing first message")
    };
    let Receive::Message(second) = source.receive().await.unwrap() else {
        panic!("missing second message")
    };
    assert_eq!(first.decode().unwrap(), 1);
    assert_eq!(second.decode().unwrap(), 2);
    first.ack().await.unwrap();
    second.ack().await.unwrap();
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn bounded_publication_is_cancellation_safe() {
    let (sink, mut receiver) = ChannelSink::bounded(1);
    sink.publish(&1).await.unwrap();
    {
        let publish = sink.publish(&2);
        tokio::pin!(publish);
        assert_pending(publish.as_mut()).await;
    }
    assert_eq!(receiver.recv().await, Some(1));
    assert!(receiver.try_recv().is_err());
    sink.publish(&3).await.unwrap();
    assert_eq!(receiver.recv().await, Some(3));
    drop(receiver);
    assert!(sink.publish(&4).await.is_err());
}

#[tokio::test]
async fn closing_sink_wakes_pending_publish_and_closes_clones() {
    let (sink, mut receiver) = ChannelSink::bounded(1);
    let clone = sink.clone();
    sink.publish(&1).await.unwrap();
    let publish = sink.publish(&2);
    tokio::pin!(publish);
    assert_pending(publish.as_mut()).await;
    clone.close().await.unwrap();
    sink.close().await.unwrap();
    assert!(publish.await.is_err());
    assert!(sink.publish(&3).await.is_err());
    assert_eq!(receiver.recv().await, Some(1));
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn closing_source_rejects_sends_but_allows_buffer_drain() {
    let (sender, mut source) = ChannelSource::bounded(1);
    sender.send(1).await.unwrap();
    source.close().await.unwrap();
    assert!(sender.send(2).await.is_err());
    assert!(matches!(
        source.receive().await.unwrap(),
        Receive::Message(_)
    ));
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn connected_subscriptions_drain_and_finish() {
    let (sink, source) = channel(1);
    let output = InMemorySink::default();
    App::new()
        .subscribe(IterSource::new([1, 2, 3]), sink, |value: i32| async move {
            Ok(value * 2)
        })
        .subscribe(
            source,
            output.clone(),
            |value: i32| async move { Ok(value + 1) },
        )
        .run_until(beavers::CancellationToken::new())
        .await
        .unwrap();
    let mut values = output.values();
    values.sort();
    assert_eq!(values, [3, 5, 7]);
}
