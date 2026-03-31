//! Integration test: user_states() fetches clearinghouseState for multiple users.
//!
//! Hyperliquid's public /info API currently returns null for a
//! batchClearinghouseStates request, so user_states() is intentionally a
//! client-side helper over the supported clearinghouseState request.
//!
//! Run with:
//! HYPERLIQUID_TEST_LIVE=1 HYPERLIQUID_TEST_USERS=0x...,0x... cargo test test_user_states_live --test batch_clearinghouse_states -- --ignored

use alloy::primitives::Address;
use hyperliquid_rust_sdk::{BaseUrl, InfoClient};

#[tokio::test]
#[ignore = "hits live API; run with HYPERLIQUID_TEST_LIVE=1 cargo test --test batch_clearinghouse_states -- --ignored"]
async fn test_user_states_live() {
    if std::env::var("HYPERLIQUID_TEST_LIVE").ok().as_deref() != Some("1") {
        eprintln!("Skipping live test (set HYPERLIQUID_TEST_LIVE=1 to run)");
        return;
    }
    let users = std::env::var("HYPERLIQUID_TEST_USERS")
        .expect("set HYPERLIQUID_TEST_USERS to a comma-separated list of 0x addresses");
    let addrs: Vec<Address> = users
        .split(',')
        .map(|user| user.trim().parse().expect("valid address"))
        .collect();
    assert!(!addrs.is_empty(), "provide at least one test user");

    let client = InfoClient::new(None, Some(BaseUrl::Mainnet))
        .await
        .expect("InfoClient::new");
    let states = client
        .user_states(addrs.clone())
        .await
        .expect("user_states");
    assert_eq!(
        states.len(),
        addrs.len(),
        "user_states returns one clearinghouse state per user in request order"
    );
    for (i, state) in states.iter().enumerate() {
        let account_value = state
            .margin_summary
            .account_value
            .parse::<f64>()
            .expect("account_value should parse as f64");
        assert!(account_value >= 0.0, "user {} has valid margin_summary", i);
    }
}
