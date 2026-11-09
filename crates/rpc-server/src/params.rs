//! Method parameter decoding.
//!
//! Params are positional, as in geth. Trailing optional params may be left out and extra ones are
//! ignored. A method without params takes `()`. Errors are worded as geth words them. For
//! `struct Params(String, #[serde(default)] Option<u64>)`:
//!
//! ```text
//! ["0xab", 5]       -> Params("0xab", Some(5))
//! ["0xab"]          -> Params("0xab", None)
//! ["0xab", 5, true] -> Params("0xab", Some(5))
//! [5]               -> -32602 invalid argument 0: invalid type: integer `5`, expected a string
//! []                -> -32602 missing value for required argument 0
//! {"block": "0xab"} -> -32602 non-array args
//! ```

use crate::InvalidParams;
use serde::{
    Deserializer,
    de::{DeserializeOwned, DeserializeSeed, Error as _, IgnoredAny, SeqAccess, Visitor},
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
    let params = params.map_or("[]", RawValue::get);
    if !params.starts_with('[') {
        return Err(InvalidParams::NonArray);
    }
    // A `RawValue` holds exactly one JSON value, so nothing can trail it.
    T::deserialize(IgnoreExtraParams(&mut serde_json::Deserializer::from_str(params)))
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

    // A method with one argument decodes into a one-field struct, which serde reads as a newtype.
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_seq(IgnoreExtraParams(visitor))
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
        unit_struct seq map struct enum identifier ignored_any
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for IgnoreExtraParams<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        let mut seq = Arguments { seq, index: 0, ended: false };
        let value = match self.0.visit_seq(&mut seq) {
            Ok(value) => value,
            // The array ran out before the arguments did.
            Err(_) if seq.ended => {
                let index = seq.index;
                return Err(A::Error::custom(format_args!(
                    "missing value for required argument {index}"
                )));
            }
            Err(err) => return Err(err),
        };
        while seq.seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(value)
    }
}

// The params array, counting the arguments read so errors can name the argument.
struct Arguments<A> {
    // The array.
    seq: A,
    // The index of the next argument.
    index: usize,
    // Whether the array ran out.
    ended: bool,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Arguments<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        let index = self.index;
        match self.seq.next_element_seed(seed) {
            Ok(Some(value)) => {
                self.index = index.saturating_add(1);
                Ok(Some(value))
            }
            Ok(None) => {
                self.ended = true;
                Ok(None)
            }
            Err(err) => Err(A::Error::custom(format_args!("invalid argument {index}: {err}"))),
        }
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
        assert_eq!(err.message, "non-array args");
    }

    #[test_case(None ; "absent")]
    #[test_case(Some("[]") ; "empty array")]
    fn optional_only_accepts_no_params(raw: Option<&str>) {
        assert_eq!(parse::<OptionalOnly>(raw).unwrap(), OptionalOnly(None));
    }

    #[test_case(None, "missing value for required argument 0" ; "absent")]
    #[test_case(Some(r#"[5]"#), "invalid argument 0: invalid type: integer `5`, expected a string" ; "wrong type")]
    #[test_case(Some(r#"["0xab", "x"]"#), "invalid argument 1: invalid type: string \"x\", expected u64" ; "second argument")]
    #[test_case(Some(r#"{"a":1}"#), "non-array args" ; "object")]
    #[test_case(Some("true"), "non-array args" ; "boolean")]
    #[test_case(Some("1"), "non-array args" ; "number")]
    #[test_case(Some(r#""0xab""#), "non-array args" ; "string")]
    fn rejects(raw: Option<&str>, message: &str) {
        let err = ErrorPayload::from(parse::<Params>(raw).unwrap_err());
        assert_eq!((err.code, err.message.as_ref()), (-32602, message));
        assert!(err.data.is_none());
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct One(u64);

    #[test_case(Some("[1, 2]"), Ok(One(1)) ; "extra ignored")]
    #[test_case(Some(r#"["x"]"#), Err("invalid argument 0: invalid type: string \"x\", expected u64") ; "wrong type")]
    #[test_case(Some("[]"), Err("missing value for required argument 0") ; "missing")]
    #[test_case(None, Err("missing value for required argument 0") ; "absent")]
    fn one_field_struct_is_indexed(raw: Option<&str>, expected: Result<One, &str>) {
        let result = parse::<One>(raw).map_err(|err| ErrorPayload::from(err).message.into_owned());
        assert_eq!(result, expected.map_err(str::to_owned));
    }

    #[test]
    fn missing_argument_after_others_is_named() {
        let err = ErrorPayload::from(parse::<(String, u64)>(Some(r#"["a"]"#)).unwrap_err());
        assert_eq!(err.message, "missing value for required argument 1");
    }
}
