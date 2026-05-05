use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use reqwest::Client;
use tokio::task::JoinHandle;

use crate::{
    info::info_client::InfoClient,
    info::{OutcomeMetaResponse, OutcomeQuestion},
    meta::{Meta, PerpDex, SpotMeta},
    prelude::*,
    BaseUrl, Error,
};

const OUTCOME_ASSET_ID_OFFSET: u32 = 100_000_000;

#[derive(Debug, Clone, Default)]
pub enum MarketMetaStoreDexs {
    #[default]
    None,
    All,
    Selected(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct MarketMetaStoreOptions {
    pub dexs: MarketMetaStoreDexs,
    pub outcomes: bool,
    pub auto_refresh: bool,
    pub refresh_interval: Duration,
}

impl Default for MarketMetaStoreOptions {
    fn default() -> Self {
        Self {
            dexs: MarketMetaStoreDexs::None,
            outcomes: false,
            auto_refresh: false,
            refresh_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeMarketRecord {
    pub coin: String,
    pub encoding: u32,
    pub token_name: String,
    pub asset_id: u32,
    pub outcome_id: u32,
    pub side_index: u32,
    pub side_name: String,
    pub outcome_name: String,
    pub outcome_description: String,
    pub question_id: Option<u32>,
    pub question_name: Option<String>,
    pub fallback_outcome: Option<u32>,
    pub is_settled: bool,
    pub mid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeOrderInfo {
    pub coin: String,
    pub encoding: u32,
    pub token_name: String,
    pub asset_id: u32,
    pub outcome_id: u32,
    pub side_index: u32,
    pub side_name: String,
    pub outcome_name: String,
    pub mid: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketMetaStoreData {
    pub coin_to_asset: HashMap<String, u32>,
    pub coin_to_sz_decimals: HashMap<String, u32>,
    pub spot_pair_to_symbol: HashMap<String, String>,
    pub symbol_to_spot_pair: HashMap<String, String>,
    pub raw_outcome_meta: Option<OutcomeMetaResponse>,
    pub outcome_markets_by_coin: HashMap<String, OutcomeMarketRecord>,
    pub outcome_alias_to_coin: HashMap<String, String>,
    pub outcome_markets_by_outcome: HashMap<u32, Vec<OutcomeMarketRecord>>,
    pub outcome_markets_by_question: HashMap<u32, Vec<OutcomeMarketRecord>>,
    mid_key_by_symbol: HashMap<String, String>,
}

#[derive(Debug)]
pub struct MarketMetaStore {
    client: Client,
    base_url: BaseUrl,
    options: MarketMetaStoreOptions,
    data: Arc<RwLock<MarketMetaStoreData>>,
    auto_refresh_handle: Option<JoinHandle<()>>,
}

impl MarketMetaStore {
    pub async fn new(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
        options: Option<MarketMetaStoreOptions>,
    ) -> Result<Self> {
        let client = client.unwrap_or_default();
        let base_url = base_url.unwrap_or(BaseUrl::Mainnet);
        let options = options.unwrap_or_default();
        let mut store = Self {
            client,
            base_url,
            options,
            data: Arc::new(RwLock::new(MarketMetaStoreData::default())),
            auto_refresh_handle: None,
        };
        store.reload().await?;
        if store.options.auto_refresh {
            store.start_auto_refresh()?;
        }
        Ok(store)
    }

    pub fn from_data(data: MarketMetaStoreData) -> Self {
        Self {
            client: Client::new(),
            base_url: BaseUrl::Mainnet,
            options: MarketMetaStoreOptions::default(),
            data: Arc::new(RwLock::new(data)),
            auto_refresh_handle: None,
        }
    }

    pub fn build_data(
        meta: &Meta,
        spot_meta: &SpotMeta,
        builder_metas: &[(usize, String, Meta)],
    ) -> MarketMetaStoreData {
        Self::build_data_with_outcomes(meta, spot_meta, builder_metas, None, None)
    }

    pub fn build_data_with_outcomes(
        meta: &Meta,
        spot_meta: &SpotMeta,
        builder_metas: &[(usize, String, Meta)],
        outcome_meta: Option<OutcomeMetaResponse>,
        all_mids: Option<&HashMap<String, String>>,
    ) -> MarketMetaStoreData {
        let mut data = MarketMetaStoreData::default();
        process_default_perps(&mut data, meta);
        process_spot_assets(&mut data, spot_meta);
        for (perp_dex_index, dex_name, dex_meta) in builder_metas {
            process_builder_dex(&mut data, *perp_dex_index, dex_name, dex_meta);
        }
        if let Some(outcome_meta) = outcome_meta {
            process_outcomes(&mut data, outcome_meta, all_mids);
        }
        data
    }

    pub async fn reload(&mut self) -> Result<()> {
        let data = fetch_data(
            &self.client,
            self.base_url,
            &self.options.dexs,
            self.options.outcomes,
        )
        .await?;
        self.replace_data(data)?;
        Ok(())
    }

    pub async fn refresh_now(&mut self) -> Result<()> {
        self.reload().await
    }

    pub fn start_auto_refresh(&mut self) -> Result<()> {
        if self.options.refresh_interval.is_zero() {
            return Err(Error::GenericRequest(
                "Refresh interval must be greater than zero".to_string(),
            ));
        }
        self.stop_auto_refresh();

        let client = self.client.clone();
        let base_url = self.base_url;
        let dexs = self.options.dexs.clone();
        let outcomes = self.options.outcomes;
        let refresh_interval = self.options.refresh_interval;
        let data = Arc::clone(&self.data);

        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::GenericRequest(format!("Tokio runtime required for auto refresh: {e}"))
        })?;

        self.auto_refresh_handle = Some(handle.spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            loop {
                interval.tick().await;
                if let Ok(next_data) = fetch_data(&client, base_url, &dexs, outcomes).await {
                    if let Ok(mut guard) = data.write() {
                        *guard = next_data;
                    }
                }
            }
        }));
        Ok(())
    }

    pub fn stop_auto_refresh(&mut self) {
        if let Some(handle) = self.auto_refresh_handle.take() {
            handle.abort();
        }
    }

    pub fn is_auto_refresh_enabled(&self) -> bool {
        self.auto_refresh_handle.is_some()
    }

    pub fn replace_data(&mut self, data: MarketMetaStoreData) -> Result<()> {
        let mut guard = self
            .data
            .write()
            .map_err(|e| Error::GenericRequest(format!("Market meta store lock poisoned: {e}")))?;
        *guard = data;
        Ok(())
    }

    pub fn snapshot(&self) -> MarketMetaStoreData {
        self.data
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub fn coin_to_asset(&self) -> HashMap<String, u32> {
        self.snapshot().coin_to_asset
    }

    pub fn coin_to_sz_decimals(&self) -> HashMap<String, u32> {
        self.snapshot().coin_to_sz_decimals
    }

    pub fn asset_id(&self, symbol: &str) -> Option<u32> {
        self.data.read().ok().and_then(|guard| {
            guard
                .coin_to_asset
                .get(symbol)
                .copied()
                .or_else(|| resolve_outcome_asset_id_from_identifier(symbol))
        })
    }

    pub fn sz_decimals(&self, symbol: &str) -> Option<u32> {
        self.data
            .read()
            .ok()
            .and_then(|guard| guard.coin_to_sz_decimals.get(symbol).copied())
    }

    pub fn spot_pair_id(&self, symbol: &str) -> Option<String> {
        let normalized = normalize_spot_pair_id(symbol);
        self.data.read().ok().and_then(|guard| {
            guard
                .symbol_to_spot_pair
                .get(symbol)
                .or_else(|| guard.symbol_to_spot_pair.get(&normalized))
                .cloned()
        })
    }

    pub fn symbol_by_spot_pair_id(&self, pair_id: &str) -> Option<String> {
        let normalized = normalize_spot_pair_id(pair_id);
        self.data
            .read()
            .ok()
            .and_then(|guard| guard.spot_pair_to_symbol.get(&normalized).cloned())
    }

    pub fn mid_key(&self, symbol: &str) -> Option<String> {
        let normalized = normalize_spot_pair_id(symbol);
        self.data.read().ok().and_then(|guard| {
            guard
                .mid_key_by_symbol
                .get(symbol)
                .or_else(|| guard.mid_key_by_symbol.get(&normalized))
                .cloned()
        })
    }

    pub fn apply_all_mids(&mut self, mids: &HashMap<String, String>) -> Result<()> {
        let mut guard = self
            .data
            .write()
            .map_err(|e| Error::GenericRequest(format!("Market meta store lock poisoned: {e}")))?;
        apply_outcome_mids(&mut guard, mids);
        Ok(())
    }

    pub fn resolve_outcome_market(&self, coin_or_id: &str) -> Option<OutcomeMarketRecord> {
        self.data.read().ok().and_then(|guard| {
            let alias = outcome_alias_candidates(coin_or_id)
                .into_iter()
                .find_map(|candidate| guard.outcome_alias_to_coin.get(&candidate).cloned());
            alias.and_then(|coin| guard.outcome_markets_by_coin.get(&coin).cloned())
        })
    }

    pub fn outcome_markets(&self) -> Vec<OutcomeMarketRecord> {
        self.data
            .read()
            .map(|guard| guard.outcome_markets_by_coin.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn outcome_markets_by_outcome(&self, outcome_id: u32) -> Vec<OutcomeMarketRecord> {
        self.data
            .read()
            .ok()
            .and_then(|guard| guard.outcome_markets_by_outcome.get(&outcome_id).cloned())
            .unwrap_or_default()
    }

    pub fn outcome_markets_by_question(&self, question_id: u32) -> Vec<OutcomeMarketRecord> {
        self.data
            .read()
            .ok()
            .and_then(|guard| guard.outcome_markets_by_question.get(&question_id).cloned())
            .unwrap_or_default()
    }

    pub fn outcome_asset_id(&self, coin_or_id: &str) -> Option<u32> {
        self.resolve_outcome_market(coin_or_id)
            .map(|record| record.asset_id)
            .or_else(|| resolve_outcome_asset_id_from_identifier(coin_or_id))
    }

    pub fn outcome_token_name(&self, coin_or_id: &str) -> Option<String> {
        self.resolve_outcome_market(coin_or_id)
            .map(|record| record.token_name)
            .or_else(|| {
                outcome_encoding_from_identifier(coin_or_id).map(|encoding| format!("+{encoding}"))
            })
    }

    pub fn outcome_encoding(&self, coin_or_id: &str) -> Option<u32> {
        self.resolve_outcome_market(coin_or_id)
            .map(|record| record.encoding)
            .or_else(|| outcome_encoding_from_identifier(coin_or_id))
    }

    pub fn outcome_order_info(&self, coin_or_id: &str) -> Option<OutcomeOrderInfo> {
        self.resolve_outcome_market(coin_or_id)
            .map(|record| OutcomeOrderInfo {
                coin: record.coin,
                encoding: record.encoding,
                token_name: record.token_name,
                asset_id: record.asset_id,
                outcome_id: record.outcome_id,
                side_index: record.side_index,
                side_name: record.side_name,
                outcome_name: record.outcome_name,
                mid: record.mid,
            })
    }
}

impl Drop for MarketMetaStore {
    fn drop(&mut self) {
        self.stop_auto_refresh();
    }
}

async fn fetch_data(
    client: &Client,
    base_url: BaseUrl,
    dexs: &MarketMetaStoreDexs,
    outcomes: bool,
) -> Result<MarketMetaStoreData> {
    let info = InfoClient::new(Some(client.clone()), Some(base_url)).await?;
    let meta = info.meta().await?;
    let spot_meta = info.spot_meta().await?;
    let builder_metas = fetch_builder_metas(&info, dexs).await?;
    let outcome_meta = if outcomes {
        Some(info.outcome_meta().await?)
    } else {
        None
    };
    Ok(MarketMetaStore::build_data_with_outcomes(
        &meta,
        &spot_meta,
        &builder_metas,
        outcome_meta,
        None,
    ))
}

async fn fetch_builder_metas(
    info: &InfoClient,
    dexs: &MarketMetaStoreDexs,
) -> Result<Vec<(usize, String, Meta)>> {
    match dexs {
        MarketMetaStoreDexs::None => Ok(Vec::new()),
        MarketMetaStoreDexs::All | MarketMetaStoreDexs::Selected(_) => {
            let perp_dexs = info.perp_dexs().await?;
            let selected = match dexs {
                MarketMetaStoreDexs::Selected(names) => Some(names),
                _ => None,
            };

            let mut out = Vec::new();
            for (index, dex) in perp_dexs.into_iter().enumerate() {
                if index == 0 {
                    continue;
                }
                let Some(PerpDex { name }) = dex else {
                    continue;
                };
                if name.trim().is_empty() {
                    continue;
                }
                if let Some(selected) = selected {
                    if !selected.iter().any(|candidate| candidate == &name) {
                        continue;
                    }
                }
                let dex_meta = info.meta_for_dex(name.clone()).await?;
                out.push((index, name, dex_meta));
            }
            Ok(out)
        }
    }
}

fn process_default_perps(data: &mut MarketMetaStoreData, meta: &Meta) {
    for (index, asset) in meta.universe.iter().enumerate() {
        let asset_id = index as u32;
        set_aliases(
            data,
            &[
                asset.name.clone(),
                format!("{}-PERP", asset.name),
                format!("main:{}", asset.name),
                format!("main:{}-PERP", asset.name),
            ],
            asset_id,
            asset.sz_decimals,
            &asset.name,
            false,
        );
    }
}

fn process_spot_assets(data: &mut MarketMetaStoreData, spot_meta: &SpotMeta) {
    let token_map: HashMap<usize, (&str, u32)> = spot_meta
        .tokens
        .iter()
        .map(|token| (token.index, (token.name.as_str(), token.sz_decimals as u32)))
        .collect();

    for market in spot_meta.universe.iter() {
        let Some((base_name, sz_decimals)) = token_map.get(&market.tokens[0]) else {
            continue;
        };
        let Some((quote_name, _)) = token_map.get(&market.tokens[1]) else {
            continue;
        };

        let asset_id = 10000 + market.index as u32;
        let symbol = format!("{base_name}/{quote_name}");
        let spot_pair_id = normalize_spot_pair_id(if market.name.trim().is_empty() {
            market.index.to_string()
        } else {
            market.name.clone()
        });
        let canonical_pair_id = format!("@{}", market.index);
        let numeric_pair_id = market.index.to_string();
        let asset_id_alias = asset_id.to_string();

        let mut aliases = vec![
            symbol.clone(),
            spot_pair_id.clone(),
            canonical_pair_id.clone(),
            numeric_pair_id.clone(),
            asset_id_alias.clone(),
        ];
        if *quote_name == "USDC" {
            aliases.push(format!("{base_name}-SPOT"));
        }

        set_aliases(data, &aliases, asset_id, *sz_decimals, &spot_pair_id, true);
        for alias in unique_names(aliases) {
            data.symbol_to_spot_pair.insert(alias, spot_pair_id.clone());
        }
        data.spot_pair_to_symbol
            .insert(spot_pair_id.clone(), symbol.clone());
        data.spot_pair_to_symbol
            .insert(canonical_pair_id.clone(), symbol.clone());
        data.spot_pair_to_symbol.insert(numeric_pair_id, symbol);
    }
}

fn process_builder_dex(
    data: &mut MarketMetaStoreData,
    perp_dex_index: usize,
    dex_name: &str,
    dex_meta: &Meta,
) {
    let normalized_dex = dex_name.trim();
    if normalized_dex.is_empty() {
        return;
    }
    let offset = 100000 + (perp_dex_index as u32) * 10000;

    for (index, asset) in dex_meta.universe.iter().enumerate() {
        let asset_id = offset + index as u32;
        let coin = asset
            .name
            .strip_prefix(&format!("{normalized_dex}:"))
            .unwrap_or(&asset.name);
        let qualified_name = format!("{normalized_dex}:{coin}");

        set_aliases(
            data,
            &[
                qualified_name.clone(),
                format!("{qualified_name}-PERP"),
                asset.name.clone(),
                format!("{}-PERP", asset.name),
                coin.to_string(),
                format!("{coin}-PERP"),
            ],
            asset_id,
            asset.sz_decimals,
            &qualified_name,
            true,
        );
    }
}

fn process_outcomes(
    data: &mut MarketMetaStoreData,
    outcome_meta: OutcomeMetaResponse,
    all_mids: Option<&HashMap<String, String>>,
) {
    let questions = outcome_meta.questions.clone();
    data.raw_outcome_meta = Some(outcome_meta.clone());

    for outcome in outcome_meta.outcomes {
        for (side_index, side) in outcome.side_specs.iter().enumerate() {
            if side_index > 1 {
                continue;
            }
            let side_index = side_index as u32;
            let encoding = outcome
                .outcome
                .saturating_mul(10)
                .saturating_add(side_index);
            let Some(asset_id) = outcome_asset_id_from_encoding(encoding) else {
                continue;
            };

            let coin = format!("#{encoding}");
            let token_name = format!("+{encoding}");
            let question = question_for_outcome(&questions, outcome.outcome);
            let is_settled = question
                .map(|question| question.settled_named_outcomes.contains(&outcome.outcome))
                .unwrap_or(false);
            let mid = all_mids.and_then(|mids| mids.get(&coin).cloned());

            let record = OutcomeMarketRecord {
                coin: coin.clone(),
                encoding,
                token_name: token_name.clone(),
                asset_id,
                outcome_id: outcome.outcome,
                side_index,
                side_name: side.name.clone(),
                outcome_name: outcome.name.clone(),
                outcome_description: outcome.description.clone(),
                question_id: question.map(|question| question.question),
                question_name: question.map(|question| question.name.clone()),
                fallback_outcome: question.map(|question| question.fallback_outcome),
                is_settled,
                mid,
            };

            data.coin_to_asset.insert(coin.clone(), asset_id);
            data.coin_to_asset.insert(token_name.clone(), asset_id);
            data.coin_to_asset.insert(asset_id.to_string(), asset_id);
            for alias in outcome_alias_candidates(&coin) {
                data.outcome_alias_to_coin.insert(alias, coin.clone());
            }
            data.outcome_markets_by_coin.insert(coin, record);
        }
    }

    rebuild_outcome_indexes(data);
}

fn question_for_outcome(
    questions: &[OutcomeQuestion],
    outcome_id: u32,
) -> Option<&OutcomeQuestion> {
    questions.iter().find(|question| {
        question.fallback_outcome == outcome_id
            || question.named_outcomes.contains(&outcome_id)
            || question.settled_named_outcomes.contains(&outcome_id)
    })
}

fn apply_outcome_mids(data: &mut MarketMetaStoreData, mids: &HashMap<String, String>) {
    for (coin, mid) in mids {
        let Some(alias) = outcome_alias_candidates(coin)
            .into_iter()
            .find_map(|candidate| data.outcome_alias_to_coin.get(&candidate).cloned())
        else {
            continue;
        };
        if let Some(record) = data.outcome_markets_by_coin.get_mut(&alias) {
            record.mid = Some(mid.clone());
        }
    }
    rebuild_outcome_indexes(data);
}

fn rebuild_outcome_indexes(data: &mut MarketMetaStoreData) {
    data.outcome_markets_by_outcome.clear();
    data.outcome_markets_by_question.clear();

    let mut records: Vec<_> = data.outcome_markets_by_coin.values().cloned().collect();
    records.sort_by_key(|record| record.encoding);

    for record in records {
        data.outcome_markets_by_outcome
            .entry(record.outcome_id)
            .or_default()
            .push(record.clone());
        if let Some(question_id) = record.question_id {
            data.outcome_markets_by_question
                .entry(question_id)
                .or_default()
                .push(record);
        }
    }
}

fn outcome_asset_id_from_encoding(encoding: u32) -> Option<u32> {
    let side = encoding % 10;
    let outcome = encoding / 10;
    if outcome == 0 || side > 1 {
        return None;
    }
    OUTCOME_ASSET_ID_OFFSET.checked_add(encoding)
}

pub fn outcome_encoding_from_identifier(value: &str) -> Option<u32> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }

    let numeric = text
        .strip_prefix('#')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);
    if !numeric.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let value = numeric.parse::<u32>().ok()?;
    let encoding = if value >= OUTCOME_ASSET_ID_OFFSET {
        value.checked_sub(OUTCOME_ASSET_ID_OFFSET)?
    } else {
        value
    };
    outcome_asset_id_from_encoding(encoding)?;
    Some(encoding)
}

pub fn resolve_outcome_asset_id_from_identifier(value: &str) -> Option<u32> {
    outcome_encoding_from_identifier(value).and_then(outcome_asset_id_from_encoding)
}

fn outcome_alias_candidates(value: &str) -> Vec<String> {
    let text = value.trim();
    let Some(encoding) = outcome_encoding_from_identifier(text) else {
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![text.to_string()]
        };
    };
    let asset_id = outcome_asset_id_from_encoding(encoding).unwrap_or_default();
    unique_names([
        text.to_string(),
        format!("#{encoding}"),
        format!("+{encoding}"),
        encoding.to_string(),
        asset_id.to_string(),
    ])
}

fn set_aliases(
    data: &mut MarketMetaStoreData,
    aliases: &[String],
    asset_id: u32,
    sz_decimals: u32,
    mid_key: &str,
    preserve_existing: bool,
) {
    for alias in unique_names(aliases.iter().map(String::as_str)) {
        if preserve_existing && data.coin_to_asset.contains_key(&alias) {
            continue;
        }
        data.coin_to_asset.insert(alias.clone(), asset_id);
        data.coin_to_sz_decimals.insert(alias.clone(), sz_decimals);
        data.mid_key_by_symbol.insert(alias, mid_key.to_string());
    }
}

fn normalize_spot_pair_id(value: impl ToString) -> String {
    let text = value.to_string().trim().to_string();
    if text.chars().all(|ch| ch.is_ascii_digit()) && !text.is_empty() {
        format!("@{text}")
    } else {
        text
    }
}

fn unique_names<I, S>(values: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut out = Vec::new();
    for value in values {
        let text = value.as_ref().trim();
        if text.is_empty() || out.iter().any(|existing| existing == text) {
            continue;
        }
        out.push(text.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::{OutcomeMetaOutcome, OutcomeSideSpec};
    use crate::meta::{AssetMeta, SpotAssetMeta, TokenInfo};
    use alloy::primitives::B128;

    fn meta(names: &[(&str, u32)]) -> Meta {
        Meta {
            universe: names
                .iter()
                .map(|(name, sz_decimals)| AssetMeta {
                    name: (*name).to_string(),
                    sz_decimals: *sz_decimals,
                    max_leverage: 50,
                    only_isolated: None,
                })
                .collect(),
        }
    }

    fn spot_meta() -> SpotMeta {
        SpotMeta {
            tokens: vec![
                TokenInfo {
                    name: "PURR".to_string(),
                    sz_decimals: 0,
                    wei_decimals: 6,
                    index: 0,
                    token_id: B128::ZERO,
                    is_canonical: true,
                },
                TokenInfo {
                    name: "USDC".to_string(),
                    sz_decimals: 6,
                    wei_decimals: 6,
                    index: 1,
                    token_id: B128::ZERO,
                    is_canonical: true,
                },
                TokenInfo {
                    name: "HYPE".to_string(),
                    sz_decimals: 2,
                    wei_decimals: 8,
                    index: 2,
                    token_id: B128::ZERO,
                    is_canonical: true,
                },
            ],
            universe: vec![
                SpotAssetMeta {
                    tokens: [0, 1],
                    name: "PURR/USDC".to_string(),
                    index: 0,
                    is_canonical: true,
                },
                SpotAssetMeta {
                    tokens: [2, 1],
                    name: "@107".to_string(),
                    index: 107,
                    is_canonical: true,
                },
            ],
        }
    }

    fn outcome_meta() -> OutcomeMetaResponse {
        OutcomeMetaResponse {
            outcomes: vec![
                OutcomeMetaOutcome {
                    outcome: 2,
                    name: "Recurring".to_string(),
                    description: "class:priceBinary|underlying:BTC".to_string(),
                    side_specs: vec![
                        OutcomeSideSpec {
                            name: "Yes".to_string(),
                            token: Some(20),
                        },
                        OutcomeSideSpec {
                            name: "No".to_string(),
                            token: None,
                        },
                    ],
                },
                OutcomeMetaOutcome {
                    outcome: 3,
                    name: "Settled".to_string(),
                    description: "class:binary".to_string(),
                    side_specs: vec![
                        OutcomeSideSpec {
                            name: "Yes".to_string(),
                            token: None,
                        },
                        OutcomeSideSpec {
                            name: "No".to_string(),
                            token: None,
                        },
                    ],
                },
            ],
            questions: vec![
                OutcomeQuestion {
                    question: 7,
                    name: "BTC daily".to_string(),
                    description: "question".to_string(),
                    fallback_outcome: 2,
                    named_outcomes: vec![2],
                    settled_named_outcomes: vec![],
                },
                OutcomeQuestion {
                    question: 8,
                    name: "Settled question".to_string(),
                    description: "question".to_string(),
                    fallback_outcome: 3,
                    named_outcomes: vec![],
                    settled_named_outcomes: vec![3],
                },
            ],
        }
    }

    #[test]
    fn maps_perp_spot_and_builder_aliases() {
        let data = MarketMetaStore::build_data(
            &meta(&[("BTC", 5), ("ETH", 4)]),
            &spot_meta(),
            &[
                (1, "dexA".to_string(), meta(&[("dexA:AAA", 1)])),
                (2, "dexB".to_string(), meta(&[("BBB", 2)])),
                (3, "dexC".to_string(), meta(&[("BTC", 0)])),
            ],
        );

        assert_eq!(data.coin_to_asset.get("BTC"), Some(&0));
        assert_eq!(data.coin_to_asset.get("BTC-PERP"), Some(&0));
        assert_eq!(data.coin_to_asset.get("main:BTC"), Some(&0));
        assert_eq!(data.coin_to_asset.get("main:BTC-PERP"), Some(&0));
        assert_eq!(data.coin_to_sz_decimals.get("ETH"), Some(&4));

        assert_eq!(data.coin_to_asset.get("PURR/USDC"), Some(&10000));
        assert_eq!(data.coin_to_asset.get("HYPE/USDC"), Some(&10107));
        assert_eq!(data.coin_to_asset.get("HYPE-SPOT"), Some(&10107));
        assert_eq!(data.coin_to_asset.get("@107"), Some(&10107));
        assert_eq!(data.coin_to_asset.get("107"), Some(&10107));
        assert_eq!(data.coin_to_asset.get("10107"), Some(&10107));
        assert_eq!(data.coin_to_sz_decimals.get("@107"), Some(&2));
        assert_eq!(
            data.symbol_to_spot_pair.get("HYPE/USDC"),
            Some(&"@107".to_string())
        );
        assert_eq!(
            data.spot_pair_to_symbol.get("@107"),
            Some(&"HYPE/USDC".to_string())
        );

        assert_eq!(data.coin_to_asset.get("dexA:AAA"), Some(&110000));
        assert_eq!(data.coin_to_asset.get("dexA:AAA-PERP"), Some(&110000));
        assert_eq!(data.coin_to_asset.get("dexB:BBB"), Some(&120000));
        assert_eq!(data.coin_to_asset.get("BBB"), Some(&120000));
        assert_eq!(data.coin_to_asset.get("dexC:BTC"), Some(&130000));
        assert_eq!(
            data.coin_to_asset.get("BTC"),
            Some(&0),
            "builder DEX bare aliases must not override main perps"
        );
    }

    #[test]
    fn lookups_normalize_spot_pair_ids_and_replace_atomically() {
        let mut store = MarketMetaStore::from_data(MarketMetaStore::build_data(
            &meta(&[("BTC", 5)]),
            &spot_meta(),
            &[],
        ));

        assert_eq!(store.asset_id("HYPE-SPOT"), Some(10107));
        assert_eq!(store.sz_decimals("107"), Some(2));
        assert_eq!(store.spot_pair_id("107"), Some("@107".to_string()));
        assert_eq!(
            store.symbol_by_spot_pair_id("107"),
            Some("HYPE/USDC".to_string())
        );
        assert_eq!(store.mid_key("HYPE/USDC"), Some("@107".to_string()));

        let mut next = MarketMetaStoreData::default();
        next.coin_to_asset.insert("NEW".to_string(), 7);
        store.replace_data(next).unwrap();
        assert_eq!(store.asset_id("BTC"), None);
        assert_eq!(store.asset_id("NEW"), Some(7));
    }

    #[test]
    fn maps_hip4_outcome_markets_and_overlays_mids() {
        let mids = HashMap::from([
            ("#20".to_string(), "0.604185".to_string()),
            ("#21".to_string(), "0.395815".to_string()),
        ]);
        let data = MarketMetaStore::build_data_with_outcomes(
            &meta(&[("BTC", 5)]),
            &spot_meta(),
            &[],
            Some(outcome_meta()),
            Some(&mids),
        );
        let mut store = MarketMetaStore::from_data(data);

        assert_eq!(store.outcome_asset_id("#20"), Some(100000020));
        assert_eq!(store.outcome_asset_id("+20"), Some(100000020));
        assert_eq!(store.outcome_asset_id("20"), Some(100000020));
        assert_eq!(store.outcome_asset_id("100000020"), Some(100000020));
        assert_eq!(store.outcome_token_name("#20"), Some("+20".to_string()));
        assert_eq!(store.outcome_encoding("100000021"), Some(21));

        let yes = store.resolve_outcome_market("#20").unwrap();
        assert_eq!(yes.coin, "#20");
        assert_eq!(yes.encoding, 20);
        assert_eq!(yes.asset_id, 100000020);
        assert_eq!(yes.outcome_id, 2);
        assert_eq!(yes.side_index, 0);
        assert_eq!(yes.side_name, "Yes");
        assert_eq!(yes.question_id, Some(7));
        assert_eq!(yes.mid, Some("0.604185".to_string()));

        let no = store.resolve_outcome_market("+21").unwrap();
        assert_eq!(no.side_name, "No");
        assert_eq!(no.asset_id, 100000021);
        assert_eq!(store.outcome_markets_by_outcome(2).len(), 2);
        assert_eq!(store.outcome_markets_by_question(7).len(), 2);
        assert!(store.resolve_outcome_market("#30").unwrap().is_settled);

        store
            .apply_all_mids(&HashMap::from([("#20".to_string(), "0.61".to_string())]))
            .unwrap();
        assert_eq!(
            store.outcome_order_info("#20").unwrap().mid,
            Some("0.61".to_string())
        );
    }

    #[test]
    fn parses_hip4_outcome_identifiers() {
        assert_eq!(outcome_encoding_from_identifier("#20"), Some(20));
        assert_eq!(outcome_encoding_from_identifier("+21"), Some(21));
        assert_eq!(outcome_encoding_from_identifier("100000020"), Some(20));
        assert_eq!(
            resolve_outcome_asset_id_from_identifier("21"),
            Some(100000021)
        );
        assert_eq!(resolve_outcome_asset_id_from_identifier("#22"), None);
        assert_eq!(resolve_outcome_asset_id_from_identifier("#9"), None);
    }
}
