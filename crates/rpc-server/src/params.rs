//! Method parameter decoding.
//!
//! Params are positional, as in geth. Trailing optional params may be left out and extra ones are
//! ignored. A method without params takes `()`. For `struct Params(String, #[serde(default)]
//! Option<u64>)`:
//!
//! ```text
//! ["0xab", 5]       -> Params("0xab", Some(5))
//! ["0xab"]          -> Params("0xab", None)
//! ["0xab", 5, true] -> Params("0xab", Some(5))
//! []                -> -32602 Invalid params
//! {"block": "0xab"} -> -32602 Invalid params
//! ```

use crate::InvalidParams;
use serde::{
    Deserializer,
    de::{DeserializeOwned, IgnoredAny, SeqAccess, Visitor},
    forward_to_deserialize_any,
};
use serde_json::value::RawValue;
use std::fmt;

/// Decodes a method's positional params.
///
/// # Errors
///
/// Returns [`InvalidParams`] when the params don't match `T`.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{ErrorPayload, parse_params};
/// use serde::Deserialize;
/// use serde_json::value::RawValue;
///
/// #[derive(Debug, PartialEq, Deserialize)]
/// struct Params(String, #[serde(default)] Option<bool>);
///
/// let raw = RawValue::from_string(r#"["0xab"]"#.to_owned())?;
/// assert_eq!(parse_params::<Params>(Some(&raw)).unwrap(), Params("0xab".to_owned(), None));
/// let err = parse_params::<Params>(None).unwrap_err();
/// assert_eq!(ErrorPayload::from(err).code, -32602);
/// # Ok::<_, serde_json::Error>(())
/// ```
pub fn parse_params<T: DeserializeOwned>(params: Option<&RawValue>) -> Result<T, InvalidParams> {
    // A `RawValue` holds exactly one JSON value, so nothing can trail it.
    T::deserialize(IgnoreExtraParams(&mut serde_json::Deserializer::from_str(
        params.map_or("[]", RawValue::get),
    )))
    .map_err(InvalidParams::from)
}

// Lets a tuple or tuple struct decode an array longer than its fields, which serde rejects.
struct IgnoreExtraParams<D>(D);

impl<'de, D: Deserializer<'de>> Deserializer<'de> for IgnoreExtraParams<D> {
    type Error = D::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_any(visitor)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_seq(NoParams(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_seq(IgnoreExtraParams(visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_seq(IgnoreExtraParams(visitor))
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option
        unit_struct newtype_struct seq map struct enum identifier ignored_any
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for IgnoreExtraParams<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<V::Value, A::Error> {
        let value = self.0.visit_seq(&mut seq)?;
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(value)
    }
}

// Lets `()` decode an array of any length, for a method without params.
struct NoParams<V>(V);

impl<'de, V: Visitor<'de>> Visitor<'de> for NoParams<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<V::Value, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        self.0.visit_unit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_rpc::ErrorPayload;
    use serde::Deserialize;
    use test_case::test_case;

    #[derive(Debug, PartialEq, Deserialize)]
    struct Params(String, #[serde(default)] Option<u64>);

    #[derive(Debug, PartialEq, Deserialize)]
    struct OptionalOnly(#[serde(default)] Option<u64>);

    fn parse<T: DeserializeOwned>(raw: Option<&str>) -> Result<T, InvalidParams> {
        let raw = raw.map(|raw| RawValue::from_string(raw.to_owned()).unwrap());
        parse_params(raw.as_deref())
    }

    #[test_case(r#"["0xab", 5]"#, Some(5) ; "both present")]
    #[test_case(r#"["0xab"]"#, None ; "trailing optional missing")]
    #[test_case(r#"["0xab", null]"#, None ; "trailing optional null")]
    #[test_case(r#"["0xab", 5, {"x": [2]}, "z"]"#, Some(5) ; "extra elements ignored")]
    fn decodes(raw: &str, second: Option<u64>) {
        assert_eq!(parse::<Params>(Some(raw)).unwrap(), Params("0xab".to_owned(), second));
    }

    #[test]
    fn plain_tuple_ignores_extra_elements() {
        assert_eq!(parse::<(u64,)>(Some("[1, 2]")).unwrap(), (1,));
    }

    #[test_case(None ; "absent")]
    #[test_case(Some("[]") ; "empty array")]
    #[test_case(Some("[1, true]") ; "extra elements")]
    fn unit_accepts_any_params(raw: Option<&str>) {
        parse::<()>(raw).unwrap();
    }

    #[test]
    fn unit_rejects_an_object() {
        let err = ErrorPayload::from(parse::<()>(Some(r#"{"a":1}"#)).unwrap_err());
        assert_eq!(
            err.data.unwrap().get(),
            r#""invalid type: map, expected unit at line 1 column 0""#
        );
    }

    #[test_case(None ; "absent")]
    #[test_case(Some("[]") ; "empty array")]
    fn optional_only_accepts_no_params(raw: Option<&str>) {
        assert_eq!(parse::<OptionalOnly>(raw).unwrap(), OptionalOnly(None));
    }

    #[test_case(None, "invalid length 0, expected tuple struct Params with 2 elements at line 1 column 2" ; "absent")]
    #[test_case(Some(r#"[5]"#), "invalid type: integer `5`, expected a string at line 1 column 2" ; "wrong type")]
    #[test_case(Some(r#"{"a":1}"#), "invalid type: map, expected tuple struct Params at line 1 column 0" ; "object")]
    fn rejects(raw: Option<&str>, data: &str) {
        let err = ErrorPayload::from(parse::<Params>(raw).unwrap_err());
        assert_eq!((err.code, err.message.as_ref()), (-32602, "Invalid params"));
        assert_eq!(err.data.unwrap().get(), serde_json::to_string(data).unwrap());
    }
}
