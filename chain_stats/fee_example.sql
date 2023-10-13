WITH
    WsETH_WETH_fees AS (
        SELECT
            evt_block_time as time,
            feesEarned0 / pow (10, 18) as fee0, -- symbol = wstETH
            feesEarned1 / pow (10, 18) as fee1 -- symbol = WETH
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
        WHERE
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ),
    token0 AS (
        SELECT
            price as latest_price
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
            AND symbol = 'wstETH'
        ORDER BY
            minute DESC
        LIMIT
            1
    ),
    token1 AS (
        SELECT
            price as latest_price
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
            AND symbol = 'WETH'
        ORDER BY
            minute DESC
        LIMIT
            1
    )
SELECT
    w.time,
    SUM(w.fee0) OVER (
        ORDER BY
            w.time
    ) as cumulative_fee0,
    SUM(w.fee0 * token0.latest_price) OVER (
        ORDER BY
            w.time
    ) as cumulative_fee0_usd,
    SUM(w.fee1) OVER (
        ORDER BY
            w.time
    ) as cumulative_fee1,
    SUM(w.fee1 * token1.latest_price) OVER (
        ORDER BY
            w.time
    ) as cumulative_fee1_usd,
    SUM(
        w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
    ) OVER (
        ORDER BY
            w.time
    ) as cumulative_total_fee_usd
    SUM(
        w.fee0 * token0.latest_price + w.fee1 * token1.latest_price * 10000 -- Fee tier = 0.01%
    ) OVER (
        ORDER BY
            w.time
    ) as cumulative_total_volume_usd
FROM
    WsETH_WETH_fees w
    CROSS JOIN token0
    CROSS JOIN token1
ORDER BY
    w.time;