use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use reqwest::Client;
use tokio::task::JoinHandle;

use crate::{
    info::info_client::InfoClient,
    meta::{Meta, PerpDex, SpotMeta},
    prelude::*,
    BaseUrl, Error,
};

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
    pub auto_refresh: bool,
    pub refresh_interval: Duration,
}

impl Default for MarketMetaStoreOptions {
    fn default() -> Self {
        Self {
            dexs: MarketMetaStoreDexs::None,
            auto_refresh: false,
            refresh_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketMetaStoreData {
    pub coin_to_asset: HashMap<String, u32>,
    pub coin_to_sz_decimals: HashMap<String, u32>,
    pub spot_pair_to_symbol: HashMap<String, String>,
    pub symbol_to_spot_pair: HashMap<String, String>,
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
        let mut data = MarketMetaStoreData::default();
        process_default_perps(&mut data, meta);
        process_spot_assets(&mut data, spot_meta);
        for (perp_dex_index, dex_name, dex_meta) in builder_metas {
            process_builder_dex(&mut data, *perp_dex_index, dex_name, dex_meta);
        }
        data
    }

    pub async fn reload(&mut self) -> Result<()> {
        let data = fetch_data(&self.client, self.base_url, &self.options.dexs).await?;
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
        let refresh_interval = self.options.refresh_interval;
        let data = Arc::clone(&self.data);

        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::GenericRequest(format!("Tokio runtime required for auto refresh: {e}"))
        })?;

        self.auto_refresh_handle = Some(handle.spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            loop {
                interval.tick().await;
                if let Ok(next_data) = fetch_data(&client, base_url, &dexs).await {
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
        self.data
            .read()
            .ok()
            .and_then(|guard| guard.coin_to_asset.get(symbol).copied())
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
) -> Result<MarketMetaStoreData> {
    let info = InfoClient::new(Some(client.clone()), Some(base_url)).await?;
    let meta = info.meta().await?;
    let spot_meta = info.spot_meta().await?;
    let builder_metas = fetch_builder_metas(&info, dexs).await?;
    Ok(MarketMetaStore::build_data(
        &meta,
        &spot_meta,
        &builder_metas,
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
}
