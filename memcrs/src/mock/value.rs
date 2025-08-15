use crate::cache::cache::ValueType;
use std::str;

pub fn from_string(val: &str) -> ValueType {
    val.as_bytes().to_vec()
}

pub fn from_slice(val: &[u8]) -> ValueType {
    val.to_vec()
}
