use fathom::connection::{DepthUpdate, SnapshotRest, parse_level};

// ── parse_level ─────────────────────────────────────────────────────────────

#[test]
fn test_parse_level_valid() {
    let v = [
        serde_json::Value::String("3000.50".into()),
        serde_json::Value::String("1.25".into()),
    ];
    let result = parse_level(&v);
    assert_eq!(result, Some((3000.50, 1.25)));
}

#[test]
fn test_parse_level_invalid_price() {
    let v = [
        serde_json::Value::String("not_a_number".into()),
        serde_json::Value::String("1.25".into()),
    ];
    assert!(parse_level(&v).is_none());
}

#[test]
fn test_parse_level_invalid_qty() {
    let v = [
        serde_json::Value::String("3000.50".into()),
        serde_json::Value::String("abc".into()),
    ];
    assert!(parse_level(&v).is_none());
}

#[test]
fn test_parse_level_non_string_values() {
    // Binance always sends strings, but test graceful None if not
    let v = [serde_json::json!(3000.50), serde_json::json!(1.25)];
    assert!(parse_level(&v).is_none());
}

#[test]
fn test_parse_level_zero_qty() {
    let v = [
        serde_json::Value::String("3000.00".into()),
        serde_json::Value::String("0.00000000".into()),
    ];
    let result = parse_level(&v);
    assert_eq!(result, Some((3000.0, 0.0)));
}

// ── DepthUpdate deserialization ─────────────────────────────────────────────

#[test]
fn test_depth_update_spot_no_pu() {
    let json = r#"{
        "e": "depthUpdate",
        "E": 1700000001000,
        "s": "ETHUSDT",
        "U": 100,
        "u": 102,
        "b": [["3000.00", "5.00"]],
        "a": [["3001.00", "4.00"]]
    }"#;
    let depth: DepthUpdate = serde_json::from_str(json).unwrap();
    assert_eq!(depth.event_time_ms, 1_700_000_001_000);
    assert_eq!(depth.first_update_id, 100);
    assert_eq!(depth.final_update_id, 102);
    assert!(depth.prev_final_update_id.is_none(), "spot has no pu");
    assert_eq!(depth.bids.len(), 1);
    assert_eq!(depth.asks.len(), 1);
}

#[test]
fn test_depth_update_perp_with_pu() {
    let json = r#"{
        "e": "depthUpdate",
        "E": 1700000001000,
        "s": "ETHUSDT",
        "U": 100,
        "u": 102,
        "pu": 99,
        "b": [["3000.00", "5.00"]],
        "a": []
    }"#;
    let depth: DepthUpdate = serde_json::from_str(json).unwrap();
    assert_eq!(depth.prev_final_update_id, Some(99));
    assert!(depth.asks.is_empty());
}

#[test]
fn test_depth_update_empty_bids_asks() {
    let json = r#"{
        "e": "depthUpdate",
        "E": 1700000001000,
        "s": "ETHUSDT",
        "U": 100,
        "u": 102,
        "b": [],
        "a": []
    }"#;
    let depth: DepthUpdate = serde_json::from_str(json).unwrap();
    assert!(depth.bids.is_empty());
    assert!(depth.asks.is_empty());
}

// ── SnapshotRest deserialization ────────────────────────────────────────────

#[test]
fn test_snapshot_rest_deserialize() {
    let json = r#"{
        "lastUpdateId": 12345,
        "bids": [["3000.00", "5.00"], ["2999.00", "3.00"]],
        "asks": [["3001.00", "4.00"]]
    }"#;
    let snap: SnapshotRest = serde_json::from_str(json).unwrap();
    assert_eq!(snap.last_update_id, 12345);
    assert_eq!(snap.bids.len(), 2);
    assert_eq!(snap.asks.len(), 1);
}

// ── sleep_backoff ───────────────────────────────────────────────────────────

/// tokio::time::pause() makes sleep instant — no real wall-clock waiting.
#[tokio::test(start_paused = true)]
async fn test_sleep_backoff_doubles() {
    let mut backoff = 1_000;
    fathom::connection::sleep_backoff(&mut backoff).await;
    assert_eq!(backoff, 2_000, "backoff should double");
    fathom::connection::sleep_backoff(&mut backoff).await;
    assert_eq!(backoff, 4_000);
    fathom::connection::sleep_backoff(&mut backoff).await;
    assert_eq!(backoff, 8_000);
}

#[tokio::test(start_paused = true)]
async fn test_sleep_backoff_capped() {
    let mut backoff = 40_000;
    fathom::connection::sleep_backoff(&mut backoff).await;
    assert_eq!(backoff, 60_000, "backoff should cap at 60s");
    fathom::connection::sleep_backoff(&mut backoff).await;
    assert_eq!(backoff, 60_000, "backoff should remain at cap");
}
