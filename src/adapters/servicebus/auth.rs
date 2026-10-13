//! Shared access signature tokens for claims-based authorization.
use super::config::Credential;
use crate::error::{Context as _, Error};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Token type of a shared access signature in a CBS `put-token` request.
pub(super) const SAS_TOKEN_TYPE: &str = "servicebus.windows.net:sastoken";

/// How long a token signed with a shared access key stays valid.
const TOKEN_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// A token for one audience and when it expires.
pub(super) struct Token {
    pub(super) value: String,
    pub(super) expires: SystemTime,
}

impl Credential {
    /// The token to authorize `audience` with at `now`.
    pub(super) fn token(&self, audience: &str, now: SystemTime) -> Result<Token, Error> {
        match self {
            Self::SharedAccessKey { name, key } => {
                let expires = now + TOKEN_LIFETIME;
                let expiry = expires
                    .duration_since(UNIX_EPOCH)
                    .context("the clock is before the Unix epoch")?
                    .as_secs();
                Ok(Token {
                    value: sign(audience, name, key, expiry),
                    expires,
                })
            }
            Self::SharedAccessSignature(signature) => Ok(Token {
                value: signature.clone(),
                expires: signature_expiry(signature)?,
            }),
        }
    }

    /// Whether tokens of this credential can be renewed before they expire.
    pub(super) fn renewable(&self) -> bool {
        matches!(self, Self::SharedAccessKey { .. })
    }
}

/// Signs `audience` until `expiry`, in seconds since the Unix epoch.
fn sign(audience: &str, key_name: &str, key: &str, expiry: u64) -> String {
    let resource = form_encode(audience);
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    format!(
        "SharedAccessSignature sig={}&se={expiry}&skn={}&sr={resource}",
        form_encode(&signature),
        form_encode(key_name),
    )
}

/// The `se` field of a signed token.
fn signature_expiry(signature: &str) -> Result<SystemTime, Error> {
    let fields = signature
        .strip_prefix("SharedAccessSignature ")
        .ok_or_else(|| Error::config("Service Bus shared access signature has no prefix"))?;
    let expiry = fields
        .split('&')
        .find_map(|field| field.strip_prefix("se="))
        .ok_or_else(|| Error::config("Service Bus shared access signature has no expiry (se)"))?;
    let seconds = expiry.parse().map_err(|_| {
        Error::config("Service Bus shared access signature has an invalid expiry (se)")
    })?;
    Ok(UNIX_EPOCH + Duration::from_secs(seconds))
}

/// `application/x-www-form-urlencoded` encoding, which shared access
/// signatures use for their fields.
fn form_encode(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures_match_the_service_bus_algorithm() {
        // Expected value computed by the Azure SDK algorithm for these inputs.
        assert_eq!(
            sign(
                "amqps://fe2o3-amqp-example.servicebus.windows.net",
                "RootManageSharedAccessKey",
                "r9rdIglXNiIPN2Lgj/HyhgOuq+aGht0qH3n+/lYQhfo=",
                1667344375,
            ),
            "SharedAccessSignature sig=WOVqJi%2B2fowHpCC2g3ztxEQrYAU173BGWrkVaPlvPj4%3D\
             &se=1667344375&skn=RootManageSharedAccessKey\
             &sr=amqps%3A%2F%2Ffe2o3-amqp-example.servicebus.windows.net"
        );
    }

    #[test]
    fn key_tokens_expire_after_an_hour() {
        let credential = Credential::SharedAccessKey {
            name: "send".into(),
            key: "key".into(),
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let token = credential.token("sb://ns/orders", now).unwrap();
        assert_eq!(token.expires, now + TOKEN_LIFETIME);
        assert!(token.value.contains("&se=4600&"));
        assert!(credential.renewable());
    }

    #[test]
    fn signed_tokens_are_used_as_given() {
        let credential = Credential::SharedAccessSignature(
            "SharedAccessSignature sr=x&sig=y&se=1700000000&skn=z".into(),
        );
        let token = credential
            .token("sb://ns/orders", SystemTime::now())
            .unwrap();
        assert_eq!(
            token.expires,
            UNIX_EPOCH + Duration::from_secs(1_700_000_000)
        );
        assert!(token.value.ends_with("skn=z"));
        assert!(!credential.renewable());
        let invalid = Credential::SharedAccessSignature("SharedAccessSignature sr=x".into());
        assert!(invalid.token("sb://ns/orders", SystemTime::now()).is_err());
    }
}
