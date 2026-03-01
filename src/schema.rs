use arrow_schema::{DataType, Field, Schema};
use std::sync::OnceLock;

static RAW_SCHEMA: OnceLock<Schema> = OnceLock::new();
static SNAP_1S_SCHEMA: OnceLock<Schema> = OnceLock::new();

pub fn raw_schema() -> &'static Schema {
    RAW_SCHEMA.get_or_init(|| {
        Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("seq_id", DataType::Int64, false),
            Field::new("prev_seq_id", DataType::Int64, false),
            Field::new(
                "bid_prices",
                DataType::List(std::sync::Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "bid_qtys",
                DataType::List(std::sync::Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "ask_prices",
                DataType::List(std::sync::Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "ask_qtys",
                DataType::List(std::sync::Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
        ])
    })
}

pub fn snap_1s_schema() -> &'static Schema {
    SNAP_1S_SCHEMA.get_or_init(|| {
        let mut fields = vec![
            Field::new("ts_us", DataType::Int64, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
        ];

        // bid_px_0..9
        for i in 0..10 {
            fields.push(Field::new(format!("bid_px_{i}"), DataType::Float64, true));
        }
        // ask_px_0..9
        for i in 0..10 {
            fields.push(Field::new(format!("ask_px_{i}"), DataType::Float64, true));
        }
        // bid_sz_0..9
        for i in 0..10 {
            fields.push(Field::new(format!("bid_sz_{i}"), DataType::Float64, true));
        }
        // ask_sz_0..9
        for i in 0..10 {
            fields.push(Field::new(format!("ask_sz_{i}"), DataType::Float64, true));
        }

        fields.extend([
            Field::new("mid_px", DataType::Float64, true),
            Field::new("microprice", DataType::Float64, true),
            Field::new("spread_bps", DataType::Float32, true),
            Field::new("imbalance_l1", DataType::Float32, true),
            Field::new("imbalance_l5", DataType::Float32, true),
            Field::new("imbalance_l10", DataType::Float32, true),
            Field::new("bid_depth_l5", DataType::Float64, true),
            Field::new("bid_depth_l10", DataType::Float64, true),
            Field::new("ask_depth_l5", DataType::Float64, true),
            Field::new("ask_depth_l10", DataType::Float64, true),
            Field::new("ofi_l1", DataType::Float64, true),
            Field::new("churn_bid", DataType::Float64, true),
            Field::new("churn_ask", DataType::Float64, true),
            Field::new("intra_sigma", DataType::Float32, true),
            Field::new("open_px", DataType::Float64, true),
            Field::new("close_px", DataType::Float64, true),
            Field::new("n_events", DataType::UInt32, false),
        ]);

        Schema::new(fields)
    })
}
