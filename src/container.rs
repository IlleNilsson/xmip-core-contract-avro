//! The Avro object container file: magic, a metadata map carrying the
//! schema and the codec, a sync marker, then blocks of datums each closed
//! by that marker.
//!
//! This is the shape an Avro Stream has when it travels on its own — a file
//! is self-describing, so a Location needs no schema registry to hold it to
//! its schema. Only the `null` codec is decoded here; a deflated or snappy
//! block is reported as one, not guessed at.

use contract::ValidationIssue;

use crate::binary::{Reader, encode_bytes, encode_long, encode_string};
use crate::schema::Parsed;

/// `Obj` and the version byte.
pub const MAGIC: [u8; 4] = *b"Obj\x01";

/// What the header said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub schema_json: String,
    pub codec: String,
    pub sync: [u8; 16],
    /// Where the first block begins.
    pub body_at: usize,
}

/// Read the header.
///
/// # Errors
/// No magic, a metadata map that does not read, or no schema in it.
pub fn read_header(bytes: &[u8]) -> Result<Header, String> {
    if !bytes.starts_with(&MAGIC) {
        return Err("the bytes do not open with the Avro magic".to_string());
    }
    let mut reader = Reader::new(&bytes[MAGIC.len()..]);
    let mut schema_json = None;
    let mut codec = "null".to_string();
    loop {
        let count = reader.long().map_err(|m| format!("{m} in the metadata"))?;
        if count == 0 {
            break;
        }
        if count < 0 {
            reader.long().map_err(|m| format!("{m} in the metadata"))?;
        }
        for _ in 0..count.unsigned_abs() {
            let key = reader
                .string()
                .map_err(|m| format!("{m} in a metadata key"))?;
            let value = reader
                .bytes()
                .map_err(|m| format!("{m} in metadata {key:?}"))?;
            match key {
                "avro.schema" => {
                    schema_json = Some(String::from_utf8_lossy(value).into_owned());
                }
                "avro.codec" => codec = String::from_utf8_lossy(value).into_owned(),
                _ => {}
            }
        }
    }
    let schema_json = schema_json.ok_or_else(|| "no avro.schema in the metadata".to_string())?;
    let at = MAGIC.len() + reader.position();
    let sync: [u8; 16] = bytes
        .get(at..at + 16)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| "the sync marker runs past the end".to_string())?;
    Ok(Header {
        schema_json,
        codec,
        sync,
        body_at: at + 16,
    })
}

/// Every departure of `bytes` from a sound container: the header, the
/// schema, every block's count and size, every datum in it, every sync.
/// Returns the parsed schema beside the issues so a caller can hold the
/// container to a bound name.
#[must_use]
pub fn validate(bytes: &[u8]) -> (Option<Parsed>, Vec<ValidationIssue>) {
    let header = match read_header(bytes) {
        Ok(header) => header,
        Err(message) => return (None, vec![malformed(&message, "header")]),
    };
    let parsed = match Parsed::parse(&header.schema_json) {
        Ok(parsed) => parsed,
        Err(message) => return (None, vec![malformed(&message, "avro.schema")]),
    };
    if header.codec != "null" {
        let message = format!("codec {:?} is not decoded here", header.codec);
        return (Some(parsed), vec![malformed(&message, "avro.codec")]);
    }
    let mut at = header.body_at;
    let mut block = 0usize;
    while at < bytes.len() {
        block += 1;
        let path = format!("block {block}");
        match one_block(&bytes[at..], &header.sync, &parsed, &path) {
            Ok(consumed) => at += consumed,
            Err(message) => return (Some(parsed), vec![malformed(&message, &path)]),
        }
    }
    (Some(parsed), Vec::new())
}

/// One block: its count, its size, its datums walked exactly, its sync.
fn one_block(bytes: &[u8], sync: &[u8; 16], parsed: &Parsed, path: &str) -> Result<usize, String> {
    let mut reader = Reader::new(bytes);
    let count = reader.long()?;
    let count = usize::try_from(count).map_err(|_| format!("a count of {count}"))?;
    let size = reader.long()?;
    let size = usize::try_from(size).map_err(|_| format!("a size of {size}"))?;
    let start = reader.position();
    let data = bytes
        .get(start..start + size)
        .ok_or_else(|| "a size that runs past the end".to_string())?;
    let mut datums = Reader::new(data);
    for ordinal in 1..=count {
        datums.skip(&parsed.root, parsed, &format!("{path} datum {ordinal}"))?;
    }
    if !datums.is_done() {
        return Err(format!(
            "{} bytes after the last datum",
            data.len() - datums.position()
        ));
    }
    let end = start + size;
    let marker = bytes
        .get(end..end + 16)
        .ok_or_else(|| "the sync marker runs past the end".to_string())?;
    if marker != sync {
        return Err("a sync marker that is not the header's".to_string());
    }
    Ok(end + 16)
}

/// A container of `datums`, each already encoded to `schema_json`, in one
/// block under the `null` codec.
#[must_use]
pub fn write(schema_json: &str, datums: &[Vec<u8>]) -> Vec<u8> {
    let sync = [0x58u8; 16];
    let mut out = MAGIC.to_vec();
    out.extend(encode_long(2));
    out.extend(encode_string("avro.schema"));
    out.extend(encode_bytes(schema_json.as_bytes()));
    out.extend(encode_string("avro.codec"));
    out.extend(encode_bytes(b"null"));
    out.extend(encode_long(0));
    out.extend_from_slice(&sync);
    if !datums.is_empty() {
        let data: Vec<u8> = datums.iter().flatten().copied().collect();
        out.extend(encode_long(i64::try_from(datums.len()).unwrap_or(i64::MAX)));
        out.extend(encode_long(i64::try_from(data.len()).unwrap_or(i64::MAX)));
        out.extend(data);
        out.extend_from_slice(&sync);
    }
    out
}

fn malformed(message: &str, path: &str) -> ValidationIssue {
    ValidationIssue {
        code: "malformed".to_string(),
        message: message.to_string(),
        path: Some(path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROBE: &str = r#"{"type":"record","name":"Probe","fields":[
        {"name":"n","type":"int"},{"name":"s","type":"string"}]}"#;

    fn probe(n: i64, s: &str) -> Vec<u8> {
        let mut datum = encode_long(n);
        datum.extend(encode_string(s));
        datum
    }

    #[test]
    fn a_written_container_reads_back_sound() {
        let bytes = write(PROBE, &[probe(1, "ping"), probe(2, "pong")]);
        let header = read_header(&bytes).expect("header");
        assert_eq!(header.codec, "null");
        assert_eq!(header.schema_json, PROBE);
        let (parsed, issues) = validate(&bytes);
        assert!(issues.is_empty(), "{issues:?}");
        assert_eq!(parsed.expect("schema").root.name(), Some("Probe"));
        let (_, issues) = validate(&write(PROBE, &[]));
        assert!(issues.is_empty(), "no blocks is sound");
    }

    #[test]
    fn every_departure_is_placed() {
        let sound = write(PROBE, &[probe(1, "ping")]);
        let (_, issues) = validate(b"PK\x03\x04");
        assert_eq!(issues[0].path.as_deref(), Some("header"));
        let mut wrong_sync = sound.clone();
        let last = wrong_sync.len() - 1;
        wrong_sync[last] ^= 0xff;
        let (_, issues) = validate(&wrong_sync);
        assert!(issues[0].message.contains("sync marker"), "{issues:?}");
        assert_eq!(issues[0].path.as_deref(), Some("block 1"));
        let mut truncated = sound.clone();
        truncated.truncate(sound.len() - 20);
        let (_, issues) = validate(&truncated);
        assert!(issues[0].message.contains("past the end"), "{issues:?}");
        let mut bad_datum = sound.clone();
        let header_length = read_header(&sound).expect("header").body_at;
        bad_datum[header_length + 2] = 0xff;
        let (_, issues) = validate(&bad_datum);
        assert!(issues[0].message.contains("block 1 datum 1"), "{issues:?}");
        let mut deflated = write(PROBE, &[]);
        let at = deflated
            .windows(4)
            .position(|w| w == b"null")
            .expect("codec");
        deflated[at..at + 4].copy_from_slice(b"defl");
        let (_, issues) = validate(&deflated);
        assert!(issues[0].message.contains("codec"), "{issues:?}");
        let no_schema = [MAGIC.as_slice(), &[0u8], &[0u8; 16]].concat();
        assert!(read_header(&no_schema).is_err());
    }
}
