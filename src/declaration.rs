//! This contract's declaration: the contract guide,
//! `module/core/capability/contract/doc/adding-a-contract.md`.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// What a Location gives this contract (ADR-0064, amendment 2026-09-26).
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[Setting {
        name: "reference",
        kind: Kind::Text,
        presence: Presence::Optional,
        meaning: "The full name of the record every container carries, com.example.Order.",
        applies: Applies::Both,
    }],
};

/// The technology's name, as a Validate step's `format` names it.
pub const NAME: &str = "avro";
