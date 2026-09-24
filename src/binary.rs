//! Avro binary encoding, read by schema without keeping what is read.
//!
//! The decoder walks one datum and says whether the bytes are what the
//! schema says they are: every varint terminates, every length fits, every
//! string is UTF-8, every enum index and union branch is in range, every
//! block-encoded array and map ends with its zero count. Nothing is built,
//! which is what lets a Receive Location hold a megabyte container to its
//! schema without allocating one.
//!
//! The cursor, the base-128 varint and the zig-zag over it are codec's, the
//! estate's one of each; what is Avro's is the walk by schema, added to the
//! cursor as [`Datum`]. The encoders
//! below are the other half, small enough to keep with the decoder they
//! mirror: a test writes with them and a probe does too.

use codec::cursor::Cursor;
use codec::varint;

use crate::schema::{Parsed, Schema};

/// What reading an Avro datum adds to the byte cursor.
pub trait Datum<'a> {
    /// One zig-zag varint, at most ten bytes.
    ///
    /// # Errors
    /// A varint that does not terminate within ten bytes, or the end.
    fn long(&mut self) -> Result<i64, String>;

    /// Length-prefixed bytes.
    ///
    /// # Errors
    /// A negative length, or one past the end.
    fn bytes(&mut self) -> Result<&'a [u8], String>;

    /// A length-prefixed UTF-8 string.
    ///
    /// # Errors
    /// As [`Datum::bytes`], or bytes that are not UTF-8.
    fn string(&mut self) -> Result<&'a str, String>;

    /// Walk one datum of `schema`, saying where it stopped being one.
    ///
    /// # Errors
    /// The first departure, with the path to it.
    fn walk(&mut self, schema: &Schema, parsed: &Parsed, path: &str) -> Result<(), String>;
}

impl<'a> Datum<'a> for Cursor<'a> {
    fn long(&mut self) -> Result<i64, String> {
        self.varint()
            .map(varint::unzigzag)
            .map_err(|error| error.message)
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let length = length(self, "bytes")?;
        taken(self, length, "bytes")
    }

    fn string(&mut self) -> Result<&'a str, String> {
        std::str::from_utf8(self.bytes()?).map_err(|_| "a string that is not UTF-8".to_string())
    }

    fn walk(&mut self, schema: &Schema, parsed: &Parsed, path: &str) -> Result<(), String> {
        let at = |message: String| format!("{message} at {path}");
        match parsed.resolve(schema).map_err(at)? {
            Schema::Null => Ok(()),
            Schema::Boolean => match taken(self, 1, "a boolean").map_err(at)?[0] {
                0 | 1 => Ok(()),
                other => Err(at(format!("a boolean that is {other}"))),
            },
            Schema::Int => {
                let value = self.long().map_err(at)?;
                i32::try_from(value)
                    .map(|_| ())
                    .map_err(|_| at(format!("an int that is {value}")))
            }
            Schema::Long => self.long().map(|_| ()).map_err(at),
            Schema::Float => taken(self, 4, "a float").map(|_| ()).map_err(at),
            Schema::Double => taken(self, 8, "a double").map(|_| ()).map_err(at),
            Schema::Bytes => self.bytes().map(|_| ()).map_err(at),
            Schema::String => self.string().map(|_| ()).map_err(at),
            Schema::Fixed { size, .. } => taken(self, *size, "a fixed").map(|_| ()).map_err(at),
            Schema::Enum { symbols, .. } => {
                let index = self.long().map_err(at)?;
                if index < 0 || usize::try_from(index).unwrap_or(usize::MAX) >= *symbols {
                    return Err(at(format!("an enum index {index} of {symbols} symbols")));
                }
                Ok(())
            }
            Schema::Record { fields, .. } => {
                for (name, field) in fields {
                    self.walk(field, parsed, &format!("{path}.{name}"))?;
                }
                Ok(())
            }
            Schema::Union(branches) => {
                let index = self.long().map_err(at)?;
                let branch = usize::try_from(index)
                    .ok()
                    .and_then(|i| branches.get(i))
                    .ok_or_else(|| at(format!("a union branch {index} of {}", branches.len())))?;
                self.walk(branch, parsed, &format!("{path}[{index}]"))
            }
            Schema::Array(items) => blocks(self, path, |reader, ordinal| {
                reader.walk(items, parsed, &format!("{path}[{ordinal}]"))
            }),
            Schema::Map(values) => blocks(self, path, |reader, ordinal| {
                let key = reader
                    .string()
                    .map_err(|m| format!("{m} at {path} key {ordinal}"))?;
                reader.walk(values, parsed, &format!("{path}[{key:?}]"))
            }),
            Schema::Reference(_) => Err(at("a reference to a reference".to_string())),
        }
    }
}

/// The next `count` bytes, `what` naming them when they are not there.
fn taken<'a>(reader: &mut Cursor<'a>, count: usize, what: &str) -> Result<&'a [u8], String> {
    reader
        .take(count)
        .map_err(|_| format!("{what} runs past the end"))
}

/// A length, non-negative.
fn length(reader: &mut Cursor<'_>, what: &str) -> Result<usize, String> {
    let value = reader.long()?;
    usize::try_from(value).map_err(|_| format!("{what} with a negative length"))
}

/// Block-encoded items: a count, optionally negative with a byte size after
/// it, the items, until a zero count.
fn blocks<'a>(
    reader: &mut Cursor<'a>,
    path: &str,
    mut item: impl FnMut(&mut Cursor<'a>, usize) -> Result<(), String>,
) -> Result<(), String> {
    let mut ordinal = 0usize;
    loop {
        let count = reader.long().map_err(|m| format!("{m} at {path}"))?;
        if count == 0 {
            return Ok(());
        }
        let count = if count < 0 {
            length(reader, "a block").map_err(|m| format!("{m} at {path}"))?;
            count.unsigned_abs()
        } else {
            count.unsigned_abs()
        };
        for _ in 0..count {
            item(reader, ordinal)?;
            ordinal += 1;
        }
    }
}

/// `value` as a zig-zag varint.
#[must_use]
pub fn encode_long(value: i64) -> Vec<u8> {
    varint::encode(varint::zigzag(value))
}

/// `bytes` with their length before them.
#[must_use]
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = encode_long(i64::try_from(bytes.len()).unwrap_or(i64::MAX));
    out.extend_from_slice(bytes);
    out
}

/// `text` as an Avro string.
#[must_use]
pub fn encode_string(text: &str) -> Vec<u8> {
    encode_bytes(text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_zigzag_both_ways() {
        for value in [0, -1, 1, -2, 2, 63, -64, 64, 300, i64::MAX, i64::MIN] {
            let bytes = encode_long(value);
            let mut reader = Cursor::new(&bytes);
            assert_eq!(reader.long().expect("long"), value, "{value}");
            assert!(reader.is_empty());
        }
        assert_eq!(encode_long(-1), [1]);
        assert_eq!(encode_long(1), [2]);
        assert_eq!(encode_long(64), [0x80, 0x01]);
        assert!(Cursor::new(&[0x80; 11]).long().is_err(), "eleven bytes");
        assert!(Cursor::new(&[0x80]).long().is_err(), "cut off");
    }

    #[test]
    fn a_datum_is_walked_by_its_schema_and_departures_are_placed() {
        let parsed = Parsed::parse(crate::schema::tests::ORDER).expect("schema");
        let mut datum = encode_long(4711);
        datum.extend(encode_string("ACME"));
        datum.extend(encode_long(1)); // PAID
        datum.extend(encode_long(2)); // two lines
        for (sku, qty) in [("X001", 2), ("X002", 1)] {
            datum.extend(encode_string(sku));
            datum.extend(encode_long(qty));
            datum.extend(encode_bytes(&[0x03, 0xe8]));
        }
        datum.extend(encode_long(0)); // end of lines
        datum.extend(encode_long(1)); // note: string
        datum.extend(encode_string("rush"));
        datum.extend(encode_long(-1)); // one tag, sized block
        datum.extend(encode_long(5));
        datum.extend(encode_string("vip"));
        datum.push(1);
        datum.extend(encode_long(0));
        datum.extend([0u8; 16]); // hash
        datum.extend(encode_long(0)); // parent: null
        let mut reader = Cursor::new(&datum);
        reader.walk(&parsed.root, &parsed, "order").expect("sound");
        assert!(reader.is_empty());
        assert_eq!(reader.position(), datum.len());

        let mut bad_enum = datum.clone();
        bad_enum[encode_long(4711).len() + encode_string("ACME").len()] = encode_long(2)[0];
        let error = Cursor::new(&bad_enum)
            .walk(&parsed.root, &parsed, "order")
            .expect_err("enum");
        assert_eq!(error, "an enum index 2 of 2 symbols at order.status");

        let short = &datum[..datum.len() - 1];
        let error = Cursor::new(short)
            .walk(&parsed.root, &parsed, "order")
            .expect_err("short");
        assert!(error.ends_with("at order.parent"), "{error}");

        let mut bad_string = datum.clone();
        bad_string[encode_long(4711).len() + 1] = 0xff;
        let error = Cursor::new(&bad_string)
            .walk(&parsed.root, &parsed, "order")
            .expect_err("utf-8");
        assert_eq!(error, "a string that is not UTF-8 at order.customer");
    }

    #[test]
    fn primitives_are_held_to_their_widths() {
        let parsed = Parsed::parse(r#""int""#).expect("int");
        let too_wide = encode_long(i64::from(i32::MAX) + 1);
        let mut reader = Cursor::new(&too_wide);
        assert!(reader.walk(&parsed.root, &parsed, "n").is_err());
        let parsed = Parsed::parse(r#""boolean""#).expect("boolean");
        assert!(Cursor::new(&[2]).walk(&parsed.root, &parsed, "b").is_err());
        assert!(Cursor::new(&[1]).walk(&parsed.root, &parsed, "b").is_ok());
        let parsed = Parsed::parse(r#""double""#).expect("double");
        assert!(
            Cursor::new(&[0; 7])
                .walk(&parsed.root, &parsed, "d")
                .is_err()
        );
        let parsed = Parsed::parse(r#"["null","int"]"#).expect("union");
        assert!(
            Cursor::new(&encode_long(2))
                .walk(&parsed.root, &parsed, "u")
                .is_err()
        );
        assert!(Cursor::new(&[0]).walk(&parsed.root, &parsed, "u").is_ok());
        let parsed = Parsed::parse(r#""bytes""#).expect("bytes");
        assert!(
            Cursor::new(&encode_long(-3))
                .walk(&parsed.root, &parsed, "b")
                .is_err()
        );
    }
}
