//! Server error payloads.

use alloy_json_rpc::ErrorPayload;
use serde_json::value::to_raw_value;

/// Caps the error `data` echoed back to the caller.
const MAX_ERROR_DATA_LEN: usize = 1024;

/// `-32602 Invalid params` carrying the decoder's message.
pub(crate) fn invalid_params(err: serde_json::Error) -> ErrorPayload {
    let payload = ErrorPayload::invalid_params();
    let mut message = err.to_string();
    if message.len() > MAX_ERROR_DATA_LEN {
        message.truncate(message.floor_char_boundary(MAX_ERROR_DATA_LEN));
        message.push('…');
    }
    // Never panic while answering a bad request.
    match to_raw_value(&message) {
        Ok(data) => ErrorPayload { data: Some(data), ..payload },
        Err(_) => payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(err: ErrorPayload) -> String {
        serde_json::from_str(err.data.unwrap().get()).unwrap()
    }

    #[test]
    fn invalid_params_carries_the_message() {
        let err = invalid_params(serde_json::from_str::<u64>(r#""x""#).unwrap_err());
        assert_eq!((err.code, err.message.as_ref()), (-32602, "Invalid params"));
        assert_eq!(data(err), "invalid type: string \"x\", expected u64 at line 1 column 3");
    }

    #[test]
    fn long_data_is_cut() {
        let value = "é".repeat(MAX_ERROR_DATA_LEN);
        let err =
            invalid_params(serde_json::from_str::<u64>(&format!(r#""{value}""#)).unwrap_err());
        let message = format!("invalid type: string \"{value}");
        let cut = &message[..message.floor_char_boundary(MAX_ERROR_DATA_LEN)];
        assert_eq!(data(err), format!("{cut}…"));
    }
}
