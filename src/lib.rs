#![forbid(unsafe_code)]

//! The Apache Avro content contract — a technology of `xmip-core-contract`.
//!
//! Two claims, decided 2026-09-07 (ADR-0042): **well-formedness is a given**
//! and **conformance is a given once a contract is named**.
//!
//! Well-formed here is a *sound object container file*: the magic, a
//! metadata map that names a schema this technology can read, and blocks
//! whose every datum decodes against that schema exactly — no byte short, no
//! byte over — each closed by the header's sync marker. The schema travels
//! with the data, which is Avro's point, so the bare contract needs nothing
//! from outside to hold a Stream.
//!
//! Conformance is the *named type*: a Location that names this contract
//! with `com.example.Order` bound has every container's schema held to a
//! record of that full name. A schema kept elsewhere — a registry, a file —
//! and the single-object encoding that refers to one by fingerprint are the
//! next layer here.

pub mod binary;
pub mod container;
pub mod schema;

use contract::{
    Contract, ContractDescriptor, ContractError, ContractFactory, ContractId, ValidationIssue,
    ValidationResult,
};
use stream::Stream;

/// The Avro contract, bare or bound to a named type.
pub struct Avro {
    descriptor: ContractDescriptor,
    full_name: Option<String>,
}

impl Avro {
    /// A sound container, of any schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: descriptor("avro"),
            full_name: None,
        }
    }

    /// A sound container whose schema is the record `full_name`.
    #[must_use]
    pub fn of(full_name: &str) -> Self {
        Self {
            descriptor: descriptor(&format!("avro:{full_name}")),
            full_name: Some(full_name.to_string()),
        }
    }

    /// Whether a named type is bound.
    #[must_use]
    pub fn is_bound(&self) -> bool {
        self.full_name.is_some()
    }
}

impl Default for Avro {
    fn default() -> Self {
        Self::new()
    }
}

fn descriptor(id: &str) -> ContractDescriptor {
    ContractDescriptor {
        id: ContractId(id.to_string()),
        version: "1".to_string(),
        representation: "application/avro".to_string(),
    }
}

impl Contract for Avro {
    fn descriptor(&self) -> &ContractDescriptor {
        &self.descriptor
    }

    fn identify(&self, stream: &Stream) -> Result<bool, ContractError> {
        if stream.media_type().is_some_and(|m| {
            let base = m.split(';').next().unwrap_or("").trim();
            base.eq_ignore_ascii_case("application/avro")
                || base.eq_ignore_ascii_case("avro/binary")
        }) {
            return Ok(true);
        }
        Ok(stream.bytes().starts_with(&container::MAGIC))
    }

    fn validate(&self, stream: &Stream) -> Result<ValidationResult, ContractError> {
        let (parsed, mut issues) = container::validate(stream.bytes());
        if let (Some(wanted), Some(parsed)) = (&self.full_name, parsed) {
            let actual = parsed.root.name().unwrap_or("an unnamed type");
            if actual != wanted {
                issues.push(ValidationIssue {
                    code: "named-type".to_string(),
                    message: format!("is {actual}, the contract is {wanted}"),
                    path: Some("avro.schema".to_string()),
                });
            }
        }
        Ok(ValidationResult {
            valid: issues.is_empty(),
            issues,
        })
    }
}

/// Loads the contract a Location names: an empty reference is the bare
/// contract, anything else the full name of the record every container
/// must carry, `com.example.Order`.
pub struct AvroFactory;

impl ContractFactory for AvroFactory {
    fn technology(&self) -> &'static str {
        "avro"
    }

    fn load(&self, reference: &str) -> Result<Box<dyn Contract>, ContractError> {
        let reference = reference.trim();
        if reference.is_empty() {
            return Ok(Box::new(Avro::new()));
        }
        let sound = reference
            .split('.')
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_'));
        if !sound {
            return Err(ContractError {
                message: format!("{reference:?} is not a full name, dotted names of a type"),
            });
        }
        Ok(Box::new(Avro::of(reference)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary::{encode_long, encode_string};
    use xcore::StreamId;

    const PROBE: &str = r#"{"type":"record","name":"Probe","namespace":"xmip",
        "fields":[{"name":"n","type":"int"},{"name":"s","type":"string"}]}"#;

    fn probe() -> Vec<u8> {
        let mut datum = encode_long(1);
        datum.extend(encode_string("ping-pong"));
        container::write(PROBE, &[datum])
    }

    fn stream(bytes: Vec<u8>, media_type: Option<&str>) -> Stream {
        Stream::new(StreamId::new(1), bytes, media_type.map(str::to_string))
    }

    #[test]
    fn a_sound_container_holds_bare_and_bound() {
        let bare = Avro::new();
        assert!(bare.identify(&stream(probe(), None)).expect("identify"));
        assert!(
            bare.identify(&stream(vec![], Some("avro/binary")))
                .expect("identify")
        );
        assert!(
            !bare
                .identify(&stream(b"{}".to_vec(), None))
                .expect("identify")
        );
        assert!(
            bare.validate(&stream(probe(), None))
                .expect("validate")
                .valid
        );
        let bound = AvroFactory.load("xmip.Probe").expect("load");
        assert_eq!(bound.descriptor().id.0, "avro:xmip.Probe");
        assert!(
            bound
                .validate(&stream(probe(), None))
                .expect("validate")
                .valid
        );
        assert!(Avro::of("xmip.Probe").is_bound());
        assert!(
            !AvroFactory
                .load("")
                .expect("bare")
                .descriptor()
                .id
                .0
                .contains(':')
        );
    }

    #[test]
    fn a_type_that_is_not_the_contract_is_named() {
        let bound = Avro::of("com.example.Order");
        let result = bound.validate(&stream(probe(), None)).expect("validate");
        assert!(!result.valid);
        assert_eq!(result.issues[0].code, "named-type");
        assert_eq!(
            result.issues[0].message,
            "is xmip.Probe, the contract is com.example.Order"
        );
        assert!(AvroFactory.load("com..Order").is_err());
        assert!(AvroFactory.load("com/Order").is_err());
    }

    #[test]
    fn what_is_unsound_does_not_hold() {
        let mut broken = probe();
        let last = broken.len() - 1;
        broken[last] = 0;
        let result = Avro::new()
            .validate(&stream(broken, None))
            .expect("validate");
        assert!(!result.valid);
        assert_eq!(result.issues[0].code, "malformed");
        let result = Avro::new()
            .validate(&stream(b"not avro".to_vec(), None))
            .expect("validate");
        assert_eq!(result.issues[0].path.as_deref(), Some("header"));
    }
}
