//! Server errors.

use alloy_json_rpc::ErrorPayload;
use serde_json::value::to_raw_value;

/// Caps the error `data` echoed back to the caller.
const MAX_ERROR_DATA_LEN: usize = 1024;

/// The params of a call don't match the method's arguments.
///
/// Converts into `-32602 Invalid params` carrying the decoder's message.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InvalidParams(#[from] serde_json::Error);

impl From<InvalidParams> for ErrorPayload {
    fn from(err: InvalidParams) -> Self {
        let mut message = err.to_string();
        if message.len() > MAX_ERROR_DATA_LEN {
            message.truncate(message.floor_char_boundary(MAX_ERROR_DATA_LEN));
            message.push('…');
        }
        let payload = Self::invalid_params();
        // Never panic while answering a bad request.
        match to_raw_value(&message) {
            Ok(data) => Self { data: Some(data), ..payload },
            Err(_) => payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(json: &str) -> ErrorPayload {
        InvalidParams(serde_json::from_str::<u64>(json).unwrap_err()).into()
    }

    fn data(payload: ErrorPayload) -> String {
        serde_json::from_str(payload.data.unwrap().get()).unwrap()
    }

    #[test]
    fn carries_the_message() {
        let payload = payload(r#""x""#);
        assert_eq!((payload.code, payload.message.as_ref()), (-32602, "Invalid params"));
        assert_eq!(data(payload), "invalid type: string \"x\", expected u64 at line 1 column 3");
    }

    #[test]
    fn long_data_is_cut() {
        let value = "é".repeat(MAX_ERROR_DATA_LEN);
        let message = format!("invalid type: string \"{value}");
        let cut = &message[..message.floor_char_boundary(MAX_ERROR_DATA_LEN)];
        assert_eq!(data(payload(&format!(r#""{value}""#))), format!("{cut}…"));
    }
}
