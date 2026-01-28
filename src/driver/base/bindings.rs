use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    Column,
    driver::primitives::{
        cell::{CellValue, ToCellValue, value_to_name},
        column::ColumnType,
    },
};

// This has the same structure as Column, but I'm seperating them
// because they have different purposes
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BindMetadata {
    #[serde(rename = "type")]
    pub col_type: ColumnType,
    pub name: String,

    pub precision: Option<i64>,
    pub scale: Option<i64>,
    pub nullable: bool,
}

impl From<BindMetadata> for Column {
    fn from(bind: BindMetadata) -> Self {
        Column {
            col_type: bind.col_type,
            name: bind.name,
            precision: bind.precision,
            scale: bind.scale,
            nullable: bind.nullable,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum BindValue {
    Slice(Vec<Option<String>>),
    String(String),
}

#[derive(Serialize, Clone, Debug)]
pub struct Binding {
    #[serde(rename = "type")]
    binding_type: String,

    #[serde(rename = "fmt")]
    format: Option<String>,

    // TODO: Figure schema out
    // schema: Option<BindingSchema>,
    value: Option<BindValue>,
}

impl Binding {
    pub fn new(binding_type: String, format: Option<String>, value: Option<BindValue>) -> Self {
        Self {
            binding_type,
            format,
            value,
        }
    }
}

impl From<CellValue> for Binding {
    fn from(value: CellValue) -> Self {
        let binding_type = value_to_name(&value);

        let format = match value {
            CellValue::Map(_)
            | CellValue::Array(_)
            | CellValue::Slice(_)
            | CellValue::Object(_)
            | CellValue::Variant(_)
            | CellValue::Binary(_) => Some("json".to_string()),
            _ => None,
        };

        let value: Option<String> = value.into();

        Binding {
            binding_type: binding_type.into(),
            format,
            // schema: (),
            value: value.map(|x| BindValue::String(x)),
        }
    }
}

#[derive(Serialize, Clone)]
#[allow(dead_code)]
pub struct BindingSchema {
    #[serde(rename = "type")]
    schema_type: String,

    #[serde(rename = "nullable")]
    is_nullable: bool,

    fields: Vec<FieldMetadata>,
}

#[derive(Serialize, Clone)]
pub struct FieldMetadata {
    name: String,

    #[serde(rename = "type")]
    field_type: String,

    #[serde(rename = "nullable")]
    is_nullable: bool,

    precision: Option<i64>,
    scale: Option<i64>,
    length: Option<i64>,
}

#[derive(Clone)]
pub struct Bindings {
    bindings: HashMap<String, Vec<CellValue>>,
}

impl Bindings {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    pub fn bind_row(&mut self, params: Vec<impl ToCellValue>) {
        for (k, v) in params.into_iter().enumerate() {
            let index = k + 1;
            let existing_vec = if self.bindings.contains_key(&index.to_string()) {
                self.bindings.get_mut(&index.to_string()).unwrap()
            } else {
                self.bindings.insert(index.to_string(), vec![]);
                self.bindings.get_mut(&index.to_string()).unwrap()
            };

            existing_vec.push(v.to_cell_value());
        }
    }

    pub fn bind_row_named(&mut self, params: Vec<(impl ToString, impl ToCellValue)>) {
        for (k, v) in params.into_iter() {
            let existing_vec = if self.bindings.contains_key(&k.to_string()) {
                self.bindings.get_mut(&k.to_string()).unwrap()
            } else {
                self.bindings.insert(k.to_string(), vec![]);
                self.bindings.get_mut(&k.to_string()).unwrap()
            };

            existing_vec.push(v.to_cell_value());
        }
    }

    pub fn get_final_bindings(self) -> HashMap<String, Binding> {
        self.bindings
            .into_iter()
            .map(|(key, mut col_items)| {
                let reference_value = col_items.get(0).unwrap();
                let binding_type = value_to_name(reference_value);

                let format = match reference_value {
                    CellValue::Map(_)
                    | CellValue::Array(_)
                    | CellValue::Slice(_)
                    | CellValue::Object(_)
                    | CellValue::Variant(_)
                    | CellValue::Binary(_) => Some("json".to_string()),
                    _ => None,
                };

                if col_items.len() <= 1 {
                    let value = col_items
                        .pop()
                        .map(|x| {
                            let v: Option<String> = x.into();
                            v
                        })
                        .flatten()
                        .map(|x| BindValue::String(x));

                    (key, Binding::new(binding_type.into(), format, value))
                } else {
                    (
                        key,
                        Binding::new(
                            binding_type.into(),
                            format,
                            Some(BindValue::Slice(
                                col_items
                                    .into_iter()
                                    .map(|x| x.into())
                                    .collect::<Vec<Option<String>>>(),
                            )),
                        ),
                    )
                }
            })
            .collect::<HashMap<String, Binding>>()
    }
}
