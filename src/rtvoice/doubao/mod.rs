pub mod config;
pub mod net_client;
pub mod protocol;
pub mod rt_service;
pub mod request_payloads;

use cpal;
use serde_json;

use crate::rtvoice::{doubao::protocol::ProtocolError, microphone};

#[derive(Debug)]
pub enum DoubaoError {
    NetClient(net_client::NetClientError),
    StartSessionTimeout,
    NotStartConnection,
    NotStartSession,
    Microphone(microphone::MicrophoneError),
    Io(std::io::Error),
    Json(serde_json::Error),
    Ws(String),
    Tls(native_tls::Error),

    Url(url::ParseError),
    Audio(String),
    Protocol(ProtocolError),
    InvalidConfig(String),
    Unsupported(String),
}

impl std::fmt::Display for DoubaoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NetClient(e) => write!(f, "net client error: {e}"),
            Self::StartSessionTimeout => write!(f, "start session timeout"),
            Self::NotStartConnection => write!(f, "not start connection"),
            Self::NotStartSession => write!(f, "not start session"),
            Self::Microphone(e) => write!(f, "microphone error: {e}"),
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Json(e) => write!(f, "json error: {e}"),
            Self::Ws(e) => write!(f, "websocket error: {e}"),
            Self::Tls(e) => write!(f, "tls error: {e}"),
            Self::Url(e) => write!(f, "url parse error: {e}"),
            Self::Audio(e) => write!(f, "audio error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::InvalidConfig(e) => write!(f, "invalid config: {e}"),
            Self::Unsupported(e) => write!(f, "unsupported: {e}"),
        }
    }
}

impl std::error::Error for DoubaoError {}

impl From<std::io::Error> for DoubaoError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for DoubaoError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<native_tls::Error> for DoubaoError {
    fn from(value: native_tls::Error) -> Self {
        Self::Tls(value)
    }
}

impl From<url::ParseError> for DoubaoError {
    fn from(value: url::ParseError) -> Self {
        Self::Url(value)
    }
}

impl From<cpal::BuildStreamError> for DoubaoError {
    fn from(value: cpal::BuildStreamError) -> Self {
        Self::Audio(value.to_string())
    }
}

impl From<cpal::PlayStreamError> for DoubaoError {
    fn from(value: cpal::PlayStreamError) -> Self {
        Self::Audio(value.to_string())
    }
}

impl From<cpal::SupportedStreamConfigsError> for DoubaoError {
    fn from(value: cpal::SupportedStreamConfigsError) -> Self {
        Self::Audio(value.to_string())
    }
}

impl From<cpal::DefaultStreamConfigError> for DoubaoError {
    fn from(value: cpal::DefaultStreamConfigError) -> Self {
        Self::Audio(value.to_string())
    }
}

impl From<protocol::ProtocolError> for DoubaoError {
    fn from(value: protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
