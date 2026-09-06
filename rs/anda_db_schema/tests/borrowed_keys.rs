use anda_db_schema::{AndaDBSchema, Document, DocumentOwned};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Serialize, AndaDBSchema)]
struct Borrowed<'a> {
    _id: u64,
    labels: BTreeMap<&'a str, u64>,
    byte_keys: BTreeMap<&'a [u8], bool>,
}

#[test]
fn borrowed_keys_store_and_read_as_owned_keys() {
    let label = String::from("b64:ordinary-text-key");
    let bytes = vec![0, 255];
    let value = Borrowed {
        _id: 1,
        labels: BTreeMap::from([(label.as_str(), 7)]),
        byte_keys: BTreeMap::from([(bytes.as_slice(), true)]),
    };
    let schema = Arc::new(Borrowed::schema().unwrap());
    let doc = Document::try_from(schema.clone(), &value).unwrap();
    let mut wire = Vec::new();
    cbor2::to_writer(&doc, &mut wire).unwrap();
    let stored: DocumentOwned = cbor2::from_reader(wire.as_slice()).unwrap();
    let restored = Document::try_from_doc(schema, stored).unwrap();
    assert_eq!(
        restored
            .get_field_as::<BTreeMap<String, u64>>("labels")
            .unwrap(),
        BTreeMap::from([(label, 7)])
    );
    assert_eq!(
        restored
            .get_field_as::<BTreeMap<Vec<u8>, bool>>("byte_keys")
            .unwrap(),
        BTreeMap::from([(bytes, true)])
    );
}
