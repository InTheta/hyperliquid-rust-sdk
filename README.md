# hyperliquid-rust-sdk

SDK for Hyperliquid API trading with Rust.

## Usage Examples

See `src/bin` for examples. You can run any example with `cargo run --bin [EXAMPLE]`.

### HIP-4 outcomes

The SDK exposes `InfoClient::outcome_meta()` and outcome helpers through
`MarketMetaStore`. Enable outcome metadata explicitly when building a store:

```rust
use hyperliquid_rust_sdk::{MarketMetaStore, MarketMetaStoreOptions};

let store = MarketMetaStore::new(
    None,
    None,
    Some(MarketMetaStoreOptions {
        outcomes: true,
        ..Default::default()
    }),
)
.await?;

let yes = store.resolve_outcome_market("#20");
let asset_id = store.outcome_asset_id("#20"); // 100000020
```

Outcome identifiers follow Hyperliquid's HIP-4 asset ID rules:
`encoding = 10 * outcome + side`, coin `#<encoding>`, token `+<encoding>`,
and order/cancel asset ID `100_000_000 + encoding`. Direct limit order,
modify, and cancel helpers can resolve prefixed HIP-4 identifiers such as
`#20` or `+20`, as well as asset IDs such as `100000020`. Bare numeric
encodings like `20` are supported by the outcome helpers but may overlap
existing spot aliases in order conversion. Market-order slippage helpers remain
conservative until Hyperliquid publishes stable outcome size precision.

## Installation

`cargo add hyperliquid_rust_sdk`

## License

This project is licensed under the terms of the `MIT` license. See [LICENSE](LICENSE.md) for more details.

```bibtex
@misc{hyperliquid-rust-sdk,
  author = {Hyperliquid},
  title = {SDK for Hyperliquid API trading with Rust.},
  year = {2024},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/hyperliquid-dex/hyperliquid-rust-sdk}}
}
```
