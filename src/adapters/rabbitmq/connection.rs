//! Broker connections shared by the source and sink.
use crate::error::{Context, Error};
use lapin::{Connection, ConnectionProperties};

/// AMQP reply code of a normal close.
const REPLY_SUCCESS: u16 = 200;

/// Connects with `name` as the connection name the broker's management tools show.
pub(super) async fn connect(uri: &str, name: String) -> Result<Connection, Error> {
    let properties = ConnectionProperties::default().with_connection_name(name.into());
    Connection::connect(uri, properties)
        .await
        .context("connecting to RabbitMQ")
}

/// Closes a connection, which closes its channels. The broker requeues the
/// messages a closed channel had not acknowledged.
pub(super) async fn close(connection: &Connection) -> Result<(), Error> {
    if !connection.status().connected() {
        return Ok(());
    }
    connection
        .close(REPLY_SUCCESS, "closing".into())
        .await
        .context("closing the RabbitMQ connection")
}
