//! Recognizer and serializer for the bounded filter-pushdown predicate AST
//! (design D2/D3/D7). One recognizer, [`classify`], decides which
//! DataFusion expressions are representable; it backs both planning-time
//! classification (`supports_filters_pushdown`) and scan-time
//! serialization, so serialization is total by construction.

use std::ffi::CString;

use arrow::datatypes::Schema;
use base64::Engine as _;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct ColumnRef {
    name: String,
    index: usize,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CompareOp {
    Eq,
    Neq,
    Lt,
    Lteq,
    Gt,
    Gteq,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum Literal {
    Bool {
        value: bool,
    },
    Int8 {
        value: i8,
    },
    Int16 {
        value: i16,
    },
    Int32 {
        value: i32,
    },
    Int64 {
        value: i64,
    },
    Uint8 {
        value: u8,
    },
    Uint16 {
        value: u16,
    },
    Uint32 {
        value: u32,
    },
    Uint64 {
        value: u64,
    },
    Float32 {
        value: f32,
    },
    Float64 {
        value: f64,
    },
    Utf8 {
        value: String,
    },
    /// Binary literals cross the wire base64-encoded (standard alphabet).
    Binary {
        value: String,
    },
    Date32 {
        value: i32,
    },
    Date64 {
        value: i64,
    },
    Timestamp {
        value: i64,
        unit: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        tz: Option<String>,
    },
}

/// One node of the wire-format predicate AST, tagged by `kind`. The Go
/// decoder in `datafusion/expr.go` is the other half of this contract;
/// golden fixtures under `datafusion/testdata/pushdown/` pin both sides.
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PushExpr {
    Compare {
        column: ColumnRef,
        op: CompareOp,
        literal: Literal,
    },
    IsNull {
        column: ColumnRef,
        negated: bool,
    },
    Between {
        column: ColumnRef,
        negated: bool,
        low: Literal,
        high: Literal,
    },
    InList {
        column: ColumnRef,
        negated: bool,
        list: Vec<Literal>,
    },
    And {
        left: Box<PushExpr>,
        right: Box<PushExpr>,
    },
    Or {
        left: Box<PushExpr>,
        right: Box<PushExpr>,
    },
    Not {
        expr: Box<PushExpr>,
    },
}

/// Decides whether `expr` is representable in the bounded AST, returning
/// the wire node if so. Conservative by design (design D7): anything not
/// recognized is `None`, which the caller maps to `Unsupported` — always
/// correct, since DataFusion then filters after the scan.
pub(crate) fn classify(expr: &Expr, schema: &Schema) -> Option<PushExpr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Some(PushExpr::And {
                left: Box::new(classify(left, schema)?),
                right: Box::new(classify(right, schema)?),
            }),
            Operator::Or => Some(PushExpr::Or {
                left: Box::new(classify(left, schema)?),
                right: Box::new(classify(right, schema)?),
            }),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                if let (Some(column), Some(literal)) = (column_ref(left, schema), literal(right)) {
                    Some(PushExpr::Compare {
                        column,
                        op: compare_op(*op)?,
                        literal,
                    })
                } else if let (Some(literal), Some(column)) =
                    (literal(left), column_ref(right, schema))
                {
                    // Normalize `lit op col` so Go always sees the column
                    // on the left.
                    Some(PushExpr::Compare {
                        column,
                        op: compare_op(op.swap()?)?,
                        literal,
                    })
                } else {
                    None
                }
            }
            _ => None,
        },
        Expr::IsNull(inner) => Some(PushExpr::IsNull {
            column: column_ref(inner, schema)?,
            negated: false,
        }),
        Expr::IsNotNull(inner) => Some(PushExpr::IsNull {
            column: column_ref(inner, schema)?,
            negated: true,
        }),
        Expr::Not(inner) => Some(PushExpr::Not {
            expr: Box::new(classify(inner, schema)?),
        }),
        Expr::Between(between) => Some(PushExpr::Between {
            column: column_ref(&between.expr, schema)?,
            negated: between.negated,
            low: literal(&between.low)?,
            high: literal(&between.high)?,
        }),
        Expr::InList(in_list) => Some(PushExpr::InList {
            column: column_ref(&in_list.expr, schema)?,
            negated: in_list.negated,
            list: in_list
                .list
                .iter()
                .map(literal)
                .collect::<Option<Vec<_>>>()?,
        }),
        _ => None,
    }
}

/// Serializes the representable subset of `filters` into the one-JSON-
/// document-per-scan wire format (design D3). Returns `None` when nothing
/// is representable (the scan then passes NULL). Filters reaching a scan
/// were already classified representable at planning time (design D7);
/// filtering again here is defensive and always correct, since pushed
/// filters are advisory.
pub(crate) fn filters_to_json(filters: &[Expr], schema: &Schema) -> Option<CString> {
    let nodes: Vec<PushExpr> = filters.iter().filter_map(|f| classify(f, schema)).collect();
    if nodes.is_empty() {
        return None;
    }
    let json = serde_json::to_string(&nodes).expect("PushExpr serialization cannot fail");
    // A JSON string never contains NUL bytes, so this cannot fail.
    Some(CString::new(json).expect("JSON contains no NUL bytes"))
}

fn compare_op(op: Operator) -> Option<CompareOp> {
    match op {
        Operator::Eq => Some(CompareOp::Eq),
        Operator::NotEq => Some(CompareOp::Neq),
        Operator::Lt => Some(CompareOp::Lt),
        Operator::LtEq => Some(CompareOp::Lteq),
        Operator::Gt => Some(CompareOp::Gt),
        Operator::GtEq => Some(CompareOp::Gteq),
        _ => None,
    }
}

fn column_ref(expr: &Expr, schema: &Schema) -> Option<ColumnRef> {
    match expr {
        Expr::Column(column) => {
            let index = schema.index_of(&column.name).ok()?;
            Some(ColumnRef {
                name: column.name.clone(),
                index,
            })
        }
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<Literal> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_literal(scalar),
        _ => None,
    }
}

fn scalar_to_literal(scalar: &ScalarValue) -> Option<Literal> {
    let timestamp = |value: &Option<i64>, unit: &'static str, tz: &Option<std::sync::Arc<str>>| {
        value.map(|value| Literal::Timestamp {
            value,
            unit,
            tz: tz.as_ref().map(|tz| tz.to_string()),
        })
    };
    match scalar {
        ScalarValue::Boolean(Some(value)) => Some(Literal::Bool { value: *value }),
        ScalarValue::Int8(Some(value)) => Some(Literal::Int8 { value: *value }),
        ScalarValue::Int16(Some(value)) => Some(Literal::Int16 { value: *value }),
        ScalarValue::Int32(Some(value)) => Some(Literal::Int32 { value: *value }),
        ScalarValue::Int64(Some(value)) => Some(Literal::Int64 { value: *value }),
        ScalarValue::UInt8(Some(value)) => Some(Literal::Uint8 { value: *value }),
        ScalarValue::UInt16(Some(value)) => Some(Literal::Uint16 { value: *value }),
        ScalarValue::UInt32(Some(value)) => Some(Literal::Uint32 { value: *value }),
        ScalarValue::UInt64(Some(value)) => Some(Literal::Uint64 { value: *value }),
        ScalarValue::Float32(Some(value)) => Some(Literal::Float32 { value: *value }),
        ScalarValue::Float64(Some(value)) => Some(Literal::Float64 { value: *value }),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(Literal::Utf8 {
            value: value.clone(),
        }),
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::BinaryView(Some(value)) => Some(Literal::Binary {
            value: base64::engine::general_purpose::STANDARD.encode(value),
        }),
        ScalarValue::Date32(Some(value)) => Some(Literal::Date32 { value: *value }),
        ScalarValue::Date64(Some(value)) => Some(Literal::Date64 { value: *value }),
        ScalarValue::TimestampSecond(value, tz) => timestamp(value, "s", tz),
        ScalarValue::TimestampMillisecond(value, tz) => timestamp(value, "ms", tz),
        ScalarValue::TimestampMicrosecond(value, tz) => timestamp(value, "us", tz),
        ScalarValue::TimestampNanosecond(value, tz) => timestamp(value, "ns", tz),
        // Null literals, decimals, and nested types are deliberately not
        // admitted in this change (design D2).
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::logical_expr::expr::InList as DfInList;
    use datafusion::logical_expr::{Between as DfBetween, Cast};
    use datafusion::prelude::{col, lit};

    fn people_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/../../datafusion/testdata/pushdown/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let data = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading fixture {path}: {e}"));
        serde_json::from_str(&data).expect("fixture is valid JSON")
    }

    fn serialize(filters: &[Expr], schema: &Schema) -> serde_json::Value {
        let json = filters_to_json(filters, schema).expect("filters should serialize");
        serde_json::from_str(json.to_str().unwrap()).unwrap()
    }

    #[test]
    fn golden_compare_gt_int64() {
        let filters = vec![col("id").gt(lit(ScalarValue::Int64(Some(2))))];
        assert_eq!(
            serialize(&filters, &people_schema()),
            fixture("compare_gt_int64.json")
        );
    }

    #[test]
    fn golden_all_literal_types() {
        let schema = Schema::new(vec![
            Field::new("c_bool", DataType::Boolean, true),
            Field::new("c_i8", DataType::Int8, true),
            Field::new("c_i16", DataType::Int16, true),
            Field::new("c_i32", DataType::Int32, true),
            Field::new("c_i64", DataType::Int64, true),
            Field::new("c_u8", DataType::UInt8, true),
            Field::new("c_u16", DataType::UInt16, true),
            Field::new("c_u32", DataType::UInt32, true),
            Field::new("c_u64", DataType::UInt64, true),
            Field::new("c_f32", DataType::Float32, true),
            Field::new("c_f64", DataType::Float64, true),
            Field::new("c_str", DataType::Utf8, true),
            Field::new("c_bin", DataType::Binary, true),
            Field::new("c_d32", DataType::Date32, true),
            Field::new("c_d64", DataType::Date64, true),
            Field::new("c_ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "c_ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "c_ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "c_ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]);
        let filters = vec![
            col("c_bool").eq(lit(ScalarValue::Boolean(Some(true)))),
            col("c_i8").not_eq(lit(ScalarValue::Int8(Some(-8)))),
            col("c_i16").lt(lit(ScalarValue::Int16(Some(-1600)))),
            col("c_i32").lt_eq(lit(ScalarValue::Int32(Some(-320000)))),
            col("c_i64").gt(lit(ScalarValue::Int64(Some(i64::MAX)))),
            col("c_u8").gt_eq(lit(ScalarValue::UInt8(Some(255)))),
            col("c_u16").eq(lit(ScalarValue::UInt16(Some(65535)))),
            col("c_u32").eq(lit(ScalarValue::UInt32(Some(4294967295)))),
            col("c_u64").eq(lit(ScalarValue::UInt64(Some(u64::MAX)))),
            col("c_f32").gt(lit(ScalarValue::Float32(Some(3.5)))),
            col("c_f64").lt(lit(ScalarValue::Float64(Some(2.25)))),
            col("c_str").eq(lit(ScalarValue::Utf8(Some("hello \"world\"".into())))),
            col("c_bin").eq(lit(ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])))),
            col("c_d32").gt_eq(lit(ScalarValue::Date32(Some(19000)))),
            col("c_d64").lt_eq(lit(ScalarValue::Date64(Some(1700000000000)))),
            col("c_ts_s").gt(lit(ScalarValue::TimestampSecond(Some(1700000000), None))),
            col("c_ts_ms").lt(lit(ScalarValue::TimestampMillisecond(
                Some(1700000000123),
                None,
            ))),
            col("c_ts_us").not_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(1700000000123456),
                None,
            ))),
            col("c_ts_ns").eq(lit(ScalarValue::TimestampNanosecond(
                Some(1700000000123456789),
                Some("UTC".into()),
            ))),
        ];
        assert_eq!(serialize(&filters, &schema), fixture("literal_types.json"));
    }

    #[test]
    fn golden_composite() {
        let schema = people_schema();
        let filters = vec![
            col("id")
                .gt_eq(lit(ScalarValue::Int64(Some(1))))
                .and(
                    col("name")
                        .is_null()
                        .or(datafusion::logical_expr::Expr::Not(Box::new(
                            col("name").eq(lit(ScalarValue::Utf8(Some("x".into())))),
                        ))),
                ),
            Expr::Between(DfBetween::new(
                Box::new(col("id")),
                true,
                Box::new(lit(ScalarValue::Int64(Some(10)))),
                Box::new(lit(ScalarValue::Int64(Some(20)))),
            )),
            Expr::InList(DfInList::new(
                Box::new(col("name")),
                vec![
                    lit(ScalarValue::Utf8(Some("a".into()))),
                    lit(ScalarValue::Utf8(Some("b".into()))),
                ],
                false,
            )),
            col("name").is_not_null(),
        ];
        assert_eq!(serialize(&filters, &schema), fixture("composite.json"));
    }

    #[test]
    fn mirrored_comparison_is_normalized_column_left() {
        let schema = people_schema();
        // 2 < id  ≡  id > 2
        let mirrored = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(ScalarValue::Int64(Some(2)))),
            Operator::Lt,
            Box::new(col("id")),
        ));
        assert_eq!(
            serialize(&[mirrored], &schema),
            fixture("compare_gt_int64.json")
        );
    }

    #[test]
    fn unrepresentable_expressions_are_rejected() {
        let schema = people_schema();
        let cases: Vec<(&str, Expr)> = vec![
            (
                "cast around column",
                Expr::Cast(Cast::new(Box::new(col("id")), DataType::Int32))
                    .eq(lit(ScalarValue::Int32(Some(1)))),
            ),
            (
                "like",
                col("name").like(lit(ScalarValue::Utf8(Some("x%".into())))),
            ),
            (
                "arithmetic",
                (col("id") + lit(ScalarValue::Int64(Some(1)))).gt(lit(ScalarValue::Int64(Some(2)))),
            ),
            (
                "unknown column",
                col("nope").eq(lit(ScalarValue::Int64(Some(1)))),
            ),
            ("null literal", col("id").eq(lit(ScalarValue::Int64(None)))),
            (
                "decimal literal",
                col("id").eq(lit(ScalarValue::Decimal128(Some(150), 10, 2))),
            ),
            ("column vs column", col("id").eq(col("id"))),
            (
                "and with unrepresentable side",
                col("id")
                    .gt(lit(ScalarValue::Int64(Some(2))))
                    .and(col("name").like(lit(ScalarValue::Utf8(Some("x%".into()))))),
            ),
            (
                "not over unrepresentable",
                Expr::Not(Box::new(
                    col("name").like(lit(ScalarValue::Utf8(Some("x%".into())))),
                )),
            ),
            (
                "between with non-literal bound",
                Expr::Between(DfBetween::new(
                    Box::new(col("id")),
                    false,
                    Box::new(col("id")),
                    Box::new(lit(ScalarValue::Int64(Some(20)))),
                )),
            ),
            (
                "in_list with non-literal element",
                Expr::InList(DfInList::new(
                    Box::new(col("id")),
                    vec![lit(ScalarValue::Int64(Some(1))), col("id")],
                    false,
                )),
            ),
        ];
        for (name, expr) in cases {
            assert!(
                classify(&expr, &schema).is_none(),
                "{name} must be rejected"
            );
        }
    }

    #[test]
    fn empty_or_fully_unrepresentable_filters_serialize_to_none() {
        let schema = people_schema();
        assert!(filters_to_json(&[], &schema).is_none());
        let like = col("name").like(lit(ScalarValue::Utf8(Some("x%".into()))));
        assert!(filters_to_json(&[like], &schema).is_none());
    }
}
