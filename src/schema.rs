//! An Avro schema read from its JSON, enough to walk a datum by.
//!
//! Named types — records, enums, fixeds — are kept by full name so a schema
//! may refer to one it declared earlier, or to itself. Logical types are
//! annotations on the primitive they decorate and are read as that
//! primitive. Defaults, aliases and docs are ignored: none of them changes
//! what the bytes are.

use std::collections::HashMap;

use serde_json::Value;

/// One schema node.
#[derive(Clone, Debug, PartialEq)]
pub enum Schema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record {
        name: String,
        fields: Vec<(String, Schema)>,
    },
    Enum {
        name: String,
        symbols: usize,
    },
    Fixed {
        name: String,
        size: usize,
    },
    Array(Box<Schema>),
    Map(Box<Schema>),
    Union(Vec<Schema>),
    /// A named type declared elsewhere in the same schema.
    Reference(String),
}

impl Schema {
    /// The full name where this is a named type.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        match self {
            Schema::Record { name, .. }
            | Schema::Enum { name, .. }
            | Schema::Fixed { name, .. } => Some(name),
            _ => None,
        }
    }
}

/// A schema and the named types it declares.
#[derive(Clone, Debug)]
pub struct Parsed {
    pub root: Schema,
    pub named: HashMap<String, Schema>,
}

impl Parsed {
    /// Read `json` as an Avro schema.
    ///
    /// # Errors
    /// Not JSON, or JSON that is not a schema: an unknown type name, a record
    /// without fields, a union inside a union.
    pub fn parse(json: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(json).map_err(|e| format!("not JSON: {e}"))?;
        let mut named = HashMap::new();
        let root = node(&value, None, &mut named)?;
        Ok(Self { root, named })
    }

    /// `schema` with a reference followed to what it names.
    ///
    /// # Errors
    /// A reference to a name the schema never declared.
    pub fn resolve<'a>(&'a self, schema: &'a Schema) -> Result<&'a Schema, String> {
        match schema {
            Schema::Reference(name) => self
                .named
                .get(name)
                .ok_or_else(|| format!("{name:?} is referred to and never declared")),
            other => Ok(other),
        }
    }
}

fn node(
    value: &Value,
    namespace: Option<&str>,
    named: &mut HashMap<String, Schema>,
) -> Result<Schema, String> {
    match value {
        Value::String(name) => primitive_or_reference(name, namespace, named),
        Value::Array(branches) => {
            let mut schemas = Vec::with_capacity(branches.len());
            for branch in branches {
                let schema = node(branch, namespace, named)?;
                if matches!(schema, Schema::Union(_)) {
                    return Err("a union inside a union".to_string());
                }
                schemas.push(schema);
            }
            Ok(Schema::Union(schemas))
        }
        Value::Object(object) => {
            let kind = object
                .get("type")
                .ok_or_else(|| "a schema object without a type".to_string())?;
            let Value::String(kind) = kind else {
                return node(kind, namespace, named);
            };
            match kind.as_str() {
                "record" | "error" => record(object, namespace, named),
                "enum" => {
                    let name = full_name(object, namespace)?;
                    let symbols = object
                        .get("symbols")
                        .and_then(Value::as_array)
                        .ok_or_else(|| format!("enum {name} without symbols"))?
                        .len();
                    Ok(declare(Schema::Enum { name, symbols }, named))
                }
                "fixed" => {
                    let name = full_name(object, namespace)?;
                    let size = object
                        .get("size")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| format!("fixed {name} without a size"))?;
                    let size =
                        usize::try_from(size).map_err(|_| "a fixed too large".to_string())?;
                    Ok(declare(Schema::Fixed { name, size }, named))
                }
                "array" => {
                    let items = object
                        .get("items")
                        .ok_or_else(|| "an array without items".to_string())?;
                    Ok(Schema::Array(Box::new(node(items, namespace, named)?)))
                }
                "map" => {
                    let values = object
                        .get("values")
                        .ok_or_else(|| "a map without values".to_string())?;
                    Ok(Schema::Map(Box::new(node(values, namespace, named)?)))
                }
                other => primitive_or_reference(other, namespace, named),
            }
        }
        _ => Err("a schema that is neither a name, an object nor a union".to_string()),
    }
}

fn record(
    object: &serde_json::Map<String, Value>,
    namespace: Option<&str>,
    named: &mut HashMap<String, Schema>,
) -> Result<Schema, String> {
    let name = full_name(object, namespace)?;
    let own_namespace = name.rsplit_once('.').map(|(ns, _)| ns.to_string());
    let declared = object
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("record {name} without fields"))?;
    // Declared before its fields are read, so a field may refer back to it.
    named.insert(name.clone(), Schema::Reference(name.clone()));
    let mut fields = Vec::with_capacity(declared.len());
    for field in declared {
        let field_name = field
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("a field of {name} without a name"))?;
        let kind = field
            .get("type")
            .ok_or_else(|| format!("field {field_name} of {name} without a type"))?;
        fields.push((
            field_name.to_string(),
            node(kind, own_namespace.as_deref(), named)?,
        ));
    }
    Ok(declare(Schema::Record { name, fields }, named))
}

fn declare(schema: Schema, named: &mut HashMap<String, Schema>) -> Schema {
    if let Some(name) = schema.name() {
        named.insert(name.to_string(), schema.clone());
    }
    schema
}

fn full_name(
    object: &serde_json::Map<String, Value>,
    namespace: Option<&str>,
) -> Result<String, String> {
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "a named type without a name".to_string())?;
    if name.contains('.') {
        return Ok(name.to_string());
    }
    let namespace = object
        .get("namespace")
        .and_then(Value::as_str)
        .or(namespace)
        .filter(|ns| !ns.is_empty());
    Ok(match namespace {
        Some(namespace) => format!("{namespace}.{name}"),
        None => name.to_string(),
    })
}

fn primitive_or_reference(
    name: &str,
    namespace: Option<&str>,
    named: &HashMap<String, Schema>,
) -> Result<Schema, String> {
    Ok(match name {
        "null" => Schema::Null,
        "boolean" => Schema::Boolean,
        "int" => Schema::Int,
        "long" => Schema::Long,
        "float" => Schema::Float,
        "double" => Schema::Double,
        "bytes" => Schema::Bytes,
        "string" => Schema::String,
        other => {
            let qualified = match namespace {
                Some(ns) if !other.contains('.') && !named.contains_key(other) => {
                    format!("{ns}.{other}")
                }
                _ => other.to_string(),
            };
            if !named.contains_key(&qualified) {
                return Err(format!("{other:?} is not a type this schema declares"));
            }
            Schema::Reference(qualified)
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const ORDER: &str = r#"{
        "type": "record", "name": "Order", "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer", "type": "string"},
            {"name": "status", "type": {"type": "enum", "name": "Status",
                "symbols": ["NEW", "PAID"]}},
            {"name": "lines", "type": {"type": "array", "items": {
                "type": "record", "name": "Line", "fields": [
                    {"name": "sku", "type": "string"},
                    {"name": "qty", "type": "int"},
                    {"name": "price", "type": {"type": "bytes", "logicalType": "decimal",
                        "precision": 9, "scale": 2}}]}}},
            {"name": "note", "type": ["null", "string"]},
            {"name": "tags", "type": {"type": "map", "values": "boolean"}},
            {"name": "hash", "type": {"type": "fixed", "name": "Md5", "size": 16}},
            {"name": "parent", "type": ["null", "Order"]}
        ]}"#;

    #[test]
    fn a_schema_reads_with_its_named_types_and_namespaces() {
        let parsed = Parsed::parse(ORDER).expect("parse");
        assert_eq!(parsed.root.name(), Some("com.example.Order"));
        assert!(parsed.named.contains_key("com.example.Status"));
        assert!(parsed.named.contains_key("com.example.Line"));
        assert!(parsed.named.contains_key("com.example.Md5"));
        let Schema::Record { fields, .. } = &parsed.root else {
            panic!("a record");
        };
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0].1, Schema::Long);
        assert!(matches!(&fields[3].1, Schema::Array(items)
            if matches!(**items, Schema::Record { .. })));
        assert_eq!(
            fields[7].1,
            Schema::Union(vec![
                Schema::Null,
                Schema::Reference("com.example.Order".into())
            ])
        );
        let reference = Schema::Reference("com.example.Order".into());
        let resolved = parsed.resolve(&reference).expect("resolve");
        assert_eq!(resolved.name(), Some("com.example.Order"));
        assert_eq!(
            Parsed::parse(r#""string""#).expect("primitive").root,
            Schema::String
        );
    }

    #[test]
    fn what_is_not_a_schema_is_refused() {
        assert!(Parsed::parse("{").is_err(), "not JSON");
        assert!(Parsed::parse(r#""order""#).is_err(), "unknown name");
        assert!(
            Parsed::parse(r#"{"type":"record","name":"R"}"#).is_err(),
            "no fields"
        );
        assert!(
            Parsed::parse(r#"["null",["int"]]"#).is_err(),
            "nested union"
        );
        assert!(
            Parsed::parse(r#"{"type":"enum","name":"E"}"#).is_err(),
            "no symbols"
        );
        assert!(
            Parsed::parse(r#"{"type":"fixed","name":"F"}"#).is_err(),
            "no size"
        );
        assert!(Parsed::parse(r#"{"type":"array"}"#).is_err(), "no items");
        assert!(Parsed::parse(r#"{"name":"x"}"#).is_err(), "no type");
        assert!(Parsed::parse("7").is_err(), "a number");
        let parsed = Parsed::parse(r#""int""#).expect("int");
        assert!(parsed.resolve(&Schema::Reference("X".into())).is_err());
    }
}
