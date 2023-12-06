# Dune Dashboard for Range Protocol

Dashboard [Link](https://dune.com/range_protocol/range-protocol)

# Development

## Overview

1. Take note of the [Dune tables](https://dune.com/docs/data-tables/raw/)

2. Range Protocol stats is uploaded and accessible via: "range_protocol_<chain>". 
Example semantics in "./raw_recipes/UserCount.sql". More information about [Dune decoded table](https://dune.com/docs/data-tables/decoded/evm/)

3. SQL files named according to dashboard:

    + `./chain_stats` contains query for each chain. Only daily stats is used now.
    + Each chain stats are [materialized using Dune](https://dune.com/docs/query/materialized-views/?h=mater#to-create-a-materialized-view) => Protocol aggregated data re-use these tables

## Adding a vault

Currently, I manually includes all vaults in the queries:

1. Developing query for an example vault, e.g. `./chain_stats/ethereum/vaults/USDC_WETH_5bps_active.sql`. Take note of contract address, 
token symbol from querying Dune's `prices.usd` table, and token decimals.

2. Include it into chain query, Reference file `./chain_stats/ethereum/daily_stats.sql`:
    + Create vault CTE (Example **Line 17-184**)
    + Include CTE in join statement at the end (Example line 2641 onward)
    + Include vault stats into all aggregated rows (see line 2386-2640)

*NOTE*: some token does not included in Dune's `prices.usd` table --> Those vaults will be empty --> all stats = 0

## Improve current queries

### Refractor queries

1. Get all vaults each chain (query `range_protocol_<chain>.RangeProtocolFactory_evt_VaultCreated`)
2. Get all tokens information for each vault (? - not sure how*)
3. Get all current price and decimals for token pair of each vault (query `prices.usd`)

_**Note**: In step 2 Dune does not provide blockchain read function -> can't read token address from Vault. 
Another solutions is decode vault creation calldata (get create tx_hash -> query `<chain>.transactions`) 
but decoding in Dune SQL is very complex_

_=> Find token pairs information via DEX pool address_

### Develop spell books

Protocols can develop their own custom data tables apart from decoded tables. Example: [Uniswap spell books](https://github.com/duneanalytics/spellbook/tree/main/models/uniswap)

Docs on how to develop spell books: [github.com/duneanalytics/spellbook/tree/main/models/uniswap](https://github.com/duneanalytics/spellbook/tree/main/models/uniswap)

=> Develop custom table for each chain to show vault stats

_**Note:** Still need to find a way to get token pairs of a Range vault_
