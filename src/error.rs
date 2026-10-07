//! Errors the crate reports, and the boxed error accepted from application code.
//!
//! Operations the crate performs, such as [`App::run`](crate::App::run), configuration
//! validation, and adapter constructors, return [`Error`]. Traits an application
//! implements, such as [`Decoder`](crate::Decoder), [`Sink`](crate::Sink), and
//! [`SourceMessage`](crate::SourceMessage), return [`BoxError`], so an implementation can
//! propagate any error type with `?`, including an [`Error`].
use std::{error::Error as StdError, fmt, iter};

/// Any error an application component returns: a boxed [`std::error::Error`] that can be
/// sent between threads.
///
/// `?` converts every `std::error::Error + Send + Sync` type, `String`, and `&str` into it.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// An error reported by the crate.
///
/// `Display` describes this error only and [`source`](StdError::source) returns its cause,
/// as the standard library recommends. The alternate form, `{:#}`, prints the whole chain
/// separated by `": "`, and `Debug` lists the causes on separate lines, so returning the
/// error from `main` prints every cause.
#[non_exhaustive]
pub enum Error {
    /// Configuration rejected by local validation, before any network access.
    Config(String),
    /// A record that cannot be interpreted or published as given, such as a dead-letter
    /// record with a missing header or an output a sink cannot accept.
    InvalidRecord(String),
    /// An operation on a sink, source handle, or worker that has closed or stopped.
    Closed(String),
    /// A subscription did not drain or close within its deadline.
    Timeout(String),
    /// An operation failed. `source` is the underlying cause, when there is one.
    Failed {
        /// What failed.
        message: String,
        /// The underlying cause.
        source: Option<BoxError>,
    },
    /// A subscription stopped because of `source`. [`App::run`](crate::App::run) returns
    /// the first such failure.
    Subscription {
        /// The subscription's name.
        name: String,
        /// Why the subscription stopped.
        source: BoxError,
    },
}

impl Error {
    pub(crate) fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub(crate) fn invalid_record(message: impl Into<String>) -> Self {
        Self::InvalidRecord(message.into())
    }

    pub(crate) fn closed(message: impl Into<String>) -> Self {
        Self::Closed(message.into())
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
    }

    pub(crate) fn msg(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
            source: None,
        }
    }

    /// `message` describes what failed because of `source`.
    pub(crate) fn wrap(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Failed {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Recovers an [`Error`] that was boxed, or describes another error with `message`.
    pub(crate) fn unbox(error: BoxError, message: impl Into<String>) -> Self {
        match error.downcast::<Self>() {
            Ok(error) => *error,
            Err(error) => Self::wrap(message, error),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "invalid configuration: {message}")?,
            Self::InvalidRecord(message)
            | Self::Closed(message)
            | Self::Timeout(message)
            | Self::Failed { message, .. } => f.write_str(message)?,
            Self::Subscription { name, .. } => write!(f, "subscription {name:?} failed")?,
        }
        if f.alternate() {
            let mut source = self.source();
            while let Some(cause) = source {
                write!(f, ": {cause}")?;
                source = cause.source();
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")?;
        let mut source = self.source();
        if source.is_some() {
            f.write_str("\n\nCaused by:")?;
        }
        while let Some(cause) = source {
            write!(f, "\n    {cause}")?;
            source = cause.source();
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Failed {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            Self::Subscription { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// Adds a description of the failed operation to an error.
pub(crate) trait Context<T> {
    fn context(self, message: impl Into<String>) -> Result<T, Error>;
    #[cfg_attr(
        not(any(feature = "http", feature = "health", feature = "rabbitmq")),
        allow(dead_code)
    )]
    fn with_context<M: Into<String>>(self, message: impl FnOnce() -> M) -> Result<T, Error>;
}

impl<T, E: Into<BoxError>> Context<T> for Result<T, E> {
    fn context(self, message: impl Into<String>) -> Result<T, Error> {
        self.map_err(|error| Error::wrap(message, error))
    }

    fn with_context<M: Into<String>>(self, message: impl FnOnce() -> M) -> Result<T, Error> {
        self.map_err(|error| Error::wrap(message(), error))
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, message: impl Into<String>) -> Result<T, Error> {
        self.ok_or_else(|| Error::msg(message))
    }

    fn with_context<M: Into<String>>(self, message: impl FnOnce() -> M) -> Result<T, Error> {
        self.ok_or_else(|| Error::msg(message()))
    }
}

/// Adds a description of the failed operation to a boxed error.
pub(crate) trait BoxContext {
    fn context(self, message: impl Into<String>) -> BoxError;
}

impl BoxContext for BoxError {
    fn context(self, message: impl Into<String>) -> BoxError {
        Box::new(Error::wrap(message, self))
    }
}

/// `error` followed by each of its causes.
pub(crate) fn causes<'a>(
    error: &'a (dyn StdError + 'static),
) -> impl Iterator<Item = &'a (dyn StdError + 'static)> {
    iter::successors(Some(error), |&error| error.source())
}

/// Formats `error` and each of its causes, separated by `": "`.
pub(crate) fn chain(error: &(dyn StdError + 'static)) -> String {
    causes(error)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

/// Returns early with the error built by `$make` from the formatted message unless `$cond`
/// holds. `$make` is an [`Error`] constructor such as `Error::config`.
macro_rules! ensure {
    ($cond:expr, $make:path, $($arg:tt)+) => {
        if !$cond {
            return Err($make(format!($($arg)+)).into());
        }
    };
}
pub(crate) use ensure;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn alternate_display_and_debug_include_every_cause() {
        let error = Error::Subscription {
            name: "orders".into(),
            source: Box::new(Error::wrap(
                "publish retry exhausted",
                io::Error::other("connection reset"),
            )),
        };
        assert_eq!(error.to_string(), r#"subscription "orders" failed"#);
        assert_eq!(
            format!("{error:#}"),
            r#"subscription "orders" failed: publish retry exhausted: connection reset"#
        );
        assert_eq!(
            format!("{error:?}"),
            "subscription \"orders\" failed\n\nCaused by:\n    publish retry exhausted\n    connection reset"
        );
        assert_eq!(chain(&error), format!("{error:#}"));
    }
}
