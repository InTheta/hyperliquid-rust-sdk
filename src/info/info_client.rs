use std::collections::HashMap;

use alloy::primitives::Address;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    info::{
        ActiveAssetDataResponse, CandlesSnapshotResponse, FundingHistoryResponse,
        L2SnapshotResponse, OpenOrdersResponse, OrderInfo, OutcomeMetaResponse,
        RecentTradesResponse, UserFillsResponse, UserStateResponse,
    },
    meta::{AssetContext, Meta, PerpDex, SpotMeta, SpotMetaAndAssetCtxs},
    prelude::*,
    req::HttpClient,
    ws::{Subscription, WsManager},
    BaseUrl, Error, Message, OrderStatusResponse, ReferralResponse, UserFeesResponse,
    UserFundingResponse, UserTokenBalanceResponse,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CandleSnapshotRequest {
    coin: String,
    interval: String,
    start_time: u64,
    end_time: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum InfoRequest {
    #[serde(rename = "clearinghouseState")]
    UserState {
        user: Address,
    },
    #[serde(rename = "spotClearinghouseState")]
    UserTokenBalances {
        user: Address,
    },
    UserFees {
        user: Address,
    },
    OpenOrders {
        user: Address,
    },
    OrderStatus {
        user: Address,
        oid: u64,
    },
    Meta,
    MetaAndAssetCtxs,
    SpotMeta,
    SpotMetaAndAssetCtxs,
    OutcomeMeta,
    PerpDexs,
    AllMids,
    UserFills {
        user: Address,
    },
    #[serde(rename_all = "camelCase")]
    FundingHistory {
        coin: String,
        start_time: u64,
        end_time: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    UserFunding {
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    },
    L2Book {
        coin: String,
    },
    RecentTrades {
        coin: String,
    },
    #[serde(rename_all = "camelCase")]
    CandleSnapshot {
        req: CandleSnapshotRequest,
    },
    Referral {
        user: Address,
    },
    HistoricalOrders {
        user: Address,
    },
    ActiveAssetData {
        user: Address,
        coin: String,
    },
}

#[derive(Debug)]
pub struct InfoClient {
    pub http_client: HttpClient,
    pub(crate) ws_manager: Option<WsManager>,
    reconnect: bool,
}

impl InfoClient {
    pub async fn new(client: Option<Client>, base_url: Option<BaseUrl>) -> Result<InfoClient> {
        Self::new_internal(client, base_url, false).await
    }

    pub async fn with_reconnect(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
    ) -> Result<InfoClient> {
        Self::new_internal(client, base_url, true).await
    }

    async fn new_internal(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
        reconnect: bool,
    ) -> Result<InfoClient> {
        let client = client.unwrap_or_default();
        let base_url = base_url.unwrap_or(BaseUrl::Mainnet).get_url();

        Ok(InfoClient {
            http_client: HttpClient { client, base_url },
            ws_manager: None,
            reconnect,
        })
    }

    pub async fn subscribe(
        &mut self,
        subscription: Subscription,
        sender_channel: UnboundedSender<Message>,
    ) -> Result<u32> {
        if self.ws_manager.is_none() {
            let ws_manager = WsManager::new(
                format!("ws{}/ws", &self.http_client.base_url[4..]),
                self.reconnect,
            )
            .await?;
            self.ws_manager = Some(ws_manager);
        }

        let identifier =
            serde_json::to_string(&subscription).map_err(|e| Error::JsonParse(e.to_string()))?;

        self.ws_manager
            .as_mut()
            .ok_or(Error::WsManagerNotFound)?
            .add_subscription(identifier, sender_channel)
            .await
    }

    pub async fn unsubscribe(&mut self, subscription_id: u32) -> Result<()> {
        if self.ws_manager.is_none() {
            let ws_manager = WsManager::new(
                format!("ws{}/ws", &self.http_client.base_url[4..]),
                self.reconnect,
            )
            .await?;
            self.ws_manager = Some(ws_manager);
        }

        self.ws_manager
            .as_mut()
            .ok_or(Error::WsManagerNotFound)?
            .remove_subscription(subscription_id)
            .await
    }

    async fn send_info_request<T: for<'a> Deserialize<'a>>(
        &self,
        info_request: InfoRequest,
    ) -> Result<T> {
        let data =
            serde_json::to_string(&info_request).map_err(|e| Error::JsonParse(e.to_string()))?;

        let return_data = self.http_client.post("/info", data).await?;
        serde_json::from_str(&return_data).map_err(|e| Error::JsonParse(e.to_string()))
    }

    async fn send_info_value<T: for<'a> Deserialize<'a>>(
        &self,
        info_request: serde_json::Value,
    ) -> Result<T> {
        let data =
            serde_json::to_string(&info_request).map_err(|e| Error::JsonParse(e.to_string()))?;

        let return_data = self.http_client.post("/info", data).await?;
        serde_json::from_str(&return_data).map_err(|e| Error::JsonParse(e.to_string()))
    }

    pub async fn open_orders(&self, address: Address) -> Result<Vec<OpenOrdersResponse>> {
        let input = InfoRequest::OpenOrders { user: address };
        self.send_info_request(input).await
    }

    pub async fn user_state(&self, address: Address) -> Result<UserStateResponse> {
        let input = InfoRequest::UserState { user: address };
        self.send_info_request(input).await
    }

    /// Fetches clearinghouse state for multiple users.
    ///
    /// Hyperliquid's public `/info` API does not currently expose a server-side
    /// batch clearinghouse endpoint, so this helper calls `clearinghouseState`
    /// once per address and returns responses in request order.
    pub async fn user_states(&self, addresses: Vec<Address>) -> Result<Vec<UserStateResponse>> {
        let mut states = Vec::with_capacity(addresses.len());
        for address in addresses {
            states.push(self.user_state(address).await?);
        }
        Ok(states)
    }

    pub async fn user_token_balances(&self, address: Address) -> Result<UserTokenBalanceResponse> {
        let input = InfoRequest::UserTokenBalances { user: address };
        self.send_info_request(input).await
    }

    pub async fn user_fees(&self, address: Address) -> Result<UserFeesResponse> {
        let input = InfoRequest::UserFees { user: address };
        self.send_info_request(input).await
    }

    pub async fn meta(&self) -> Result<Meta> {
        let input = InfoRequest::Meta;
        self.send_info_request(input).await
    }

    pub async fn meta_for_dex(&self, dex: String) -> Result<Meta> {
        self.send_info_value(serde_json::json!({
            "type": "meta",
            "dex": dex,
        }))
        .await
    }

    pub async fn perp_dexs(&self) -> Result<Vec<Option<PerpDex>>> {
        let input = InfoRequest::PerpDexs;
        self.send_info_request(input).await
    }

    pub async fn meta_and_asset_contexts(&self) -> Result<(Meta, Vec<AssetContext>)> {
        let input = InfoRequest::MetaAndAssetCtxs;
        self.send_info_request(input).await
    }

    pub async fn spot_meta(&self) -> Result<SpotMeta> {
        let input = InfoRequest::SpotMeta;
        self.send_info_request(input).await
    }

    pub async fn spot_meta_and_asset_contexts(&self) -> Result<Vec<SpotMetaAndAssetCtxs>> {
        let input = InfoRequest::SpotMetaAndAssetCtxs;
        self.send_info_request(input).await
    }

    pub async fn outcome_meta(&self) -> Result<OutcomeMetaResponse> {
        let input = InfoRequest::OutcomeMeta;
        self.send_info_request(input).await
    }

    pub async fn all_mids(&self) -> Result<HashMap<String, String>> {
        let input = InfoRequest::AllMids;
        self.send_info_request(input).await
    }

    pub async fn user_fills(&self, address: Address) -> Result<Vec<UserFillsResponse>> {
        let input = InfoRequest::UserFills { user: address };
        self.send_info_request(input).await
    }

    pub async fn funding_history(
        &self,
        coin: String,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Result<Vec<FundingHistoryResponse>> {
        let input = InfoRequest::FundingHistory {
            coin,
            start_time,
            end_time,
        };
        self.send_info_request(input).await
    }

    pub async fn user_funding_history(
        &self,
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Result<Vec<UserFundingResponse>> {
        let input = InfoRequest::UserFunding {
            user,
            start_time,
            end_time,
        };
        self.send_info_request(input).await
    }

    pub async fn recent_trades(&self, coin: String) -> Result<Vec<RecentTradesResponse>> {
        let input = InfoRequest::RecentTrades { coin };
        self.send_info_request(input).await
    }

    pub async fn l2_snapshot(&self, coin: String) -> Result<L2SnapshotResponse> {
        let input = InfoRequest::L2Book { coin };
        self.send_info_request(input).await
    }

    pub async fn candles_snapshot(
        &self,
        coin: String,
        interval: String,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<CandlesSnapshotResponse>> {
        let input = InfoRequest::CandleSnapshot {
            req: CandleSnapshotRequest {
                coin,
                interval,
                start_time,
                end_time,
            },
        };
        self.send_info_request(input).await
    }

    pub async fn query_order_by_oid(
        &self,
        address: Address,
        oid: u64,
    ) -> Result<OrderStatusResponse> {
        let input = InfoRequest::OrderStatus { user: address, oid };
        self.send_info_request(input).await
    }

    pub async fn query_referral_state(&self, address: Address) -> Result<ReferralResponse> {
        let input = InfoRequest::Referral { user: address };
        self.send_info_request(input).await
    }

    pub async fn historical_orders(&self, address: Address) -> Result<Vec<OrderInfo>> {
        let input = InfoRequest::HistoricalOrders { user: address };
        self.send_info_request(input).await
    }

    pub async fn active_asset_data(
        &self,
        user: Address,
        coin: String,
    ) -> Result<ActiveAssetDataResponse> {
        let input = InfoRequest::ActiveAssetData { user, coin };
        self.send_info_request(input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_perp_dexs_request() {
        let json = serde_json::to_value(InfoRequest::PerpDexs).unwrap();
        assert_eq!(json, serde_json::json!({ "type": "perpDexs" }));
    }

    #[test]
    fn serializes_outcome_meta_request() {
        let json = serde_json::to_value(InfoRequest::OutcomeMeta).unwrap();
        assert_eq!(json, serde_json::json!({ "type": "outcomeMeta" }));
    }

    #[test]
    fn deserializes_outcome_meta_response() {
        let payload = serde_json::json!({
            "outcomes": [{
                "outcome": 2,
                "name": "Recurring",
                "description": "class:priceBinary|underlying:BTC",
                "sideSpecs": [
                    { "name": "Yes", "token": 20 },
                    { "name": "No" }
                ]
            }],
            "questions": [{
                "question": 7,
                "name": "BTC daily",
                "description": "question",
                "fallbackOutcome": 2,
                "namedOutcomes": [2],
                "settledNamedOutcomes": []
            }]
        });
        let meta: OutcomeMetaResponse = serde_json::from_value(payload).unwrap();
        assert_eq!(meta.outcomes[0].outcome, 2);
        assert_eq!(meta.outcomes[0].side_specs[0].token, Some(20));
        assert_eq!(meta.outcomes[0].side_specs[1].token, None);
        assert_eq!(meta.questions[0].fallback_outcome, 2);
    }

    #[test]
    fn clearinghouse_state_serializes_single_user_request() {
        for user in [Address::ZERO, Address::from([1u8; 20])] {
            let json = serde_json::to_value(InfoRequest::UserState { user }).unwrap();
            assert_eq!(json["type"], "clearinghouseState");
            assert_eq!(json["user"], serde_json::to_value(user).unwrap());
            assert!(json.get("users").is_none());
        }
    }
}
