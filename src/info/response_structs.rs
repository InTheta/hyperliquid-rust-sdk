use serde::{Deserialize, Serialize};

use alloy::primitives::Address;

use crate::{
    info::{AssetPosition, Level, MarginSummary},
    DailyUserVlm, Delta, FeeSchedule, Leverage, OrderInfo, Referrer, ReferrerState,
    UserTokenBalance,
};

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserStateResponse {
    pub asset_positions: Vec<AssetPosition>,
    pub cross_margin_summary: MarginSummary,
    pub margin_summary: MarginSummary,
    pub withdrawable: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UserTokenBalanceResponse {
    pub balances: Vec<UserTokenBalance>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserFeesResponse {
    pub active_referral_discount: String,
    pub daily_user_vlm: Vec<DailyUserVlm>,
    pub fee_schedule: FeeSchedule,
    pub user_add_rate: String,
    pub user_cross_rate: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrdersResponse {
    pub coin: String,
    pub limit_px: String,
    pub oid: u64,
    pub side: String,
    pub sz: String,
    pub timestamp: u64,
    pub cloid: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserFillsResponse {
    pub closed_pnl: String,
    pub coin: String,
    pub crossed: bool,
    pub dir: String,
    pub hash: String,
    pub oid: u64,
    pub px: String,
    pub side: String,
    pub start_position: String,
    pub sz: String,
    pub time: u64,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub twap_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FundingHistoryResponse {
    pub coin: String,
    pub funding_rate: String,
    pub premium: String,
    pub time: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UserFundingResponse {
    pub time: u64,
    pub hash: String,
    pub delta: Delta,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct L2SnapshotResponse {
    pub coin: String,
    pub levels: Vec<Vec<Level>>,
    pub time: u64,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RecentTradesResponse {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub hash: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CandlesSnapshotResponse {
    #[serde(rename = "t")]
    pub time_open: u64,
    #[serde(rename = "T")]
    pub time_close: u64,
    #[serde(rename = "s")]
    pub coin: String,
    #[serde(rename = "i")]
    pub candle_interval: String,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "v")]
    pub vlm: String,
    #[serde(rename = "n")]
    pub num_trades: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct OrderStatusResponse {
    pub status: String,
    /// `None` if the order is not found
    #[serde(default)]
    pub order: Option<OrderInfo>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReferralResponse {
    pub referred_by: Option<Referrer>,
    pub cum_vlm: String,
    pub unclaimed_rewards: String,
    pub claimed_rewards: String,
    pub referrer_state: ReferrerState,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ActiveAssetDataResponse {
    pub user: Address,
    pub coin: String,
    pub leverage: Leverage,
    pub max_trade_szs: Vec<String>,
    pub available_to_trade: Vec<String>,
    pub mark_px: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeMetaResponse {
    pub outcomes: Vec<OutcomeMetaOutcome>,
    #[serde(default)]
    pub questions: Vec<OutcomeQuestion>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeMetaOutcome {
    pub outcome: u32,
    pub name: String,
    pub description: String,
    pub side_specs: Vec<OutcomeSideSpec>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeSideSpec {
    pub name: String,
    pub token: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeQuestion {
    pub question: u32,
    pub name: String,
    pub description: String,
    pub fallback_outcome: u32,
    pub named_outcomes: Vec<u32>,
    pub settled_named_outcomes: Vec<u32>,
}
