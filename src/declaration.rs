//! What a Location naming this contract is configured with, and the name a
//! Validate step's `format` names it by: its declaration, which its
//! `ContractFactory` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

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
