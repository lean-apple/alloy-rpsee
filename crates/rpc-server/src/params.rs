//! Method parameter decoding.

use alloy_json_rpc::ErrorPayload;
use serde::de::{
    self, DeserializeOwned, IgnoredAny, IntoDeserializer, SeqAccess, Visitor, value::Error,
};
use serde_json::value::{RawValue, to_raw_value};
use std::{fmt, marker::PhantomData};

/// Decodes a method's parameters from the request's raw `params` member.
///
/// Params are positional only, as in geth: an object is invalid params. Implemented for `()`,
/// which ignores any params, and for tuples of up to six [`DeserializeOwned`] types:
///
/// - Absent params count as `[]`.
/// - A missing trailing element decodes like `null`: an [`Option`] becomes `None`, and any other
///   type fails with `-32602 Invalid params`.
/// - Elements past the tuple's length are ignored.
///
/// ```
/// use alloy_rpc_server::FromParams;
/// use serde_json::value::RawValue;
///
/// let params = RawValue::from_string(r#"["0xab"]"#.to_owned())?;
/// let (block, full) = <(String, Option<bool>)>::from_params(Some(&params)).unwrap();
/// assert_eq!((block.as_str(), full), ("0xab", None));
///
/// let err = <(u64,)>::from_params(None).unwrap_err();
/// assert_eq!(err.code, -32602);
/// # Ok::<_, serde_json::Error>(())
/// ```
pub trait FromParams: Sized {
    /// Decodes `Self` from `params`, which is `None` when the member is absent or `null`.
    fn from_params(params: Option<&RawValue>) -> Result<Self, ErrorPayload>;
}

impl FromParams for () {
    fn from_params(_: Option<&RawValue>) -> Result<Self, ErrorPayload> {
        Ok(())
    }
}

struct TupleVisitor<T>(PhantomData<T>);

/// Reads the element at `index`, decoding a missing one like `null`.
fn next_element<'de, T, A>(seq: &mut A, index: usize, len: usize) -> Result<T, A::Error>
where
    T: DeserializeOwned,
    A: SeqAccess<'de>,
{
    if let Some(value) = seq.next_element()? {
        return Ok(value);
    }
    T::deserialize(IntoDeserializer::<Error>::into_deserializer(()))
        .map_err(|_| de::Error::invalid_length(index, &format!("{len} params").as_str()))
}

/// `-32602 Invalid params`, with the decoder's message as a JSON string in `data`.
fn invalid_params(err: serde_json::Error) -> ErrorPayload {
    let data = to_raw_value(&err.to_string()).expect("a string always serializes to JSON");
    ErrorPayload { data: Some(data), ..ErrorPayload::invalid_params() }
}

macro_rules! tuple_params {
    ($($len:literal => ($($t:ident $i:tt),+)),+ $(,)?) => {$(
        impl<'de, $($t: DeserializeOwned),+> Visitor<'de> for TupleVisitor<($($t,)+)> {
            type Value = ($($t,)+);

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!("an array of ", $len, " params"))
            }

            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                let value = ($(next_element::<$t, S>(&mut seq, $i, $len)?,)+);
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(value)
            }
        }

        impl<$($t: DeserializeOwned),+> FromParams for ($($t,)+) {
            fn from_params(params: Option<&RawValue>) -> Result<Self, ErrorPayload> {
                // A `RawValue` holds exactly one JSON value, so nothing can trail it.
                let mut de = serde_json::Deserializer::from_str(params.map_or("[]", RawValue::get));
                de::Deserializer::deserialize_seq(&mut de, TupleVisitor::<Self>(PhantomData))
                    .map_err(invalid_params)
            }
        }
    )+};
}

tuple_params! {
    1 => (A 0),
    2 => (A 0, B 1),
    3 => (A 0, B 1, C 2),
    4 => (A 0, B 1, C 2, D 3),
    5 => (A 0, B 1, C 2, D 3, E 4),
    6 => (A 0, B 1, C 2, D 3, E 4, F 5),
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    fn decode<T: FromParams>(raw: Option<&str>) -> Result<T, ErrorPayload> {
        let raw = raw.map(|raw| RawValue::from_string(raw.to_owned()).unwrap());
        T::from_params(raw.as_deref())
    }

    #[test_case(Some(r#"["0xab", 5]"#), ("0xab".to_owned(), Some(5)) ; "both present")]
    #[test_case(Some(r#"["0xab"]"#), ("0xab".to_owned(), None) ; "trailing optional missing")]
    #[test_case(Some(r#"["0xab", null]"#), ("0xab".to_owned(), None) ; "trailing optional null")]
    #[test_case(Some(r#"["0xab", 5, {"x": [2]}, "z"]"#), ("0xab".to_owned(), Some(5)) ; "extra elements ignored")]
    fn decodes(raw: Option<&str>, expected: (String, Option<u64>)) {
        assert_eq!(decode::<(String, Option<u64>)>(raw).unwrap(), expected);
    }

    #[test_case(None ; "absent")]
    #[test_case(Some("[]") ; "empty array")]
    fn optional_only_list_accepts_no_params(raw: Option<&str>) {
        assert_eq!(decode::<(Option<u64>,)>(raw).unwrap(), (None,));
    }

    #[test_case(None, "invalid length 0, expected 1 params at line 1 column 2" ; "absent")]
    #[test_case(Some("[]"), "invalid length 0, expected 1 params at line 1 column 2" ; "empty array")]
    #[test_case(Some(r#"["x"]"#), "invalid type: string \"x\", expected u64 at line 1 column 4" ; "wrong type")]
    #[test_case(Some(r#"{"a":1}"#), "invalid type: map, expected an array of 1 params at line 1 column 0" ; "object")]
    fn rejects(raw: Option<&str>, data: &str) {
        let err = decode::<(u64,)>(raw).unwrap_err();
        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "Invalid params");
        assert_eq!(err.data.unwrap().get(), serde_json::to_string(data).unwrap());
    }

    #[test]
    fn unit_ignores_params() {
        assert!(decode::<()>(Some(r#"{"anything": true}"#)).is_ok());
    }
}
