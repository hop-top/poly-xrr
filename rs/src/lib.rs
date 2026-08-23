pub mod adapters;
mod b64;
pub mod cassette;
pub mod error;
pub mod redact;
pub mod session;
pub mod stream;
mod stream_fingerprint;
pub mod stream_scrub;
mod stream_session;

pub use cassette::FileCassette;
pub use error::XrrError;
pub use redact::{
    RedactConfig, Redactor, ENV_REDACT_ALLOW, ENV_REDACT_DENY, ENV_REDACT_DISABLE,
};
pub use session::{Mode, Session};
pub use stream_scrub::{StreamDirection, StreamScrub, StreamScrubInfo};
pub use stream_session::{StreamRecording, StreamReplay};

use serde::{de::DeserializeOwned, Serialize};

/// Adapter intercepts one channel type.
pub trait Adapter: Send + Sync {
    type Req: Serialize + DeserializeOwned + Send;
    type Resp: Serialize + DeserializeOwned + Send;

    fn id(&self) -> &str;
    fn fingerprint(&self, req: &Self::Req) -> Result<String, XrrError>;
}
