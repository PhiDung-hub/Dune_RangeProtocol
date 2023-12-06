WITH
    wstETH_WETH_fees AS (
        SELECT
            evt_block_time as time,
            feesEarned0 / pow (10, 18) as fee0, -- symbol = wstETH
            feesEarned1 / pow (10, 18) as fee1 -- symbol = WETH
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
        WHERE
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ),
    wstETH_WETH_mints AS (
        SELECT
            evt_block_time as time,
            amount0In / pow (10, 18) as in0, -- symbol = wstETH
            amount1In / pow (10, 18) as in1 -- symbol = WETH
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        WHERE
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ),
    wstETH_WETH_burns AS (
        SELECT
            evt_block_time as time,
            amount0Out / pow (10, 18) as out0, -- symbol = wstETH
            amount1Out / pow (10, 18) as out1 -- symbol = WETH
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
        WHERE
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ),
    wstETH_WETH_nets AS (
        SELECT
            w.time,
            (
                COALESCE(w.in0, 0) + COALESCE(w.fee0, 0) - COALESCE(w.out0, 0)
            ) AS net0,
            (
                COALESCE(w.in1, 0) + COALESCE(w.fee1, 0) - COALESCE(w.out1, 0)
            ) AS net1
        FROM
            (
                SELECT
                    time,
                    SUM(in0) AS in0,
                    SUM(in1) AS in1,
                    SUM(fee0) AS fee0,
                    SUM(fee1) AS fee1,
                    SUM(out0) AS out0,
                    SUM(out1) AS out1
                FROM
                    (
                        SELECT
                            time,
                            in0,
                            in1,
                            NULL AS fee0,
                            NULL AS fee1,
                            NULL AS out0,
                            NULL AS out1
                        FROM
                            wstETH_WETH_mints
                        UNION ALL
                        SELECT
                            time,
                            NULL AS in0,
                            NULL AS in1,
                            fee0,
                            fee1,
                            NULL AS out0,
                            NULL AS out1
                        FROM
                            wstETH_WETH_fees
                        UNION ALL
                        SELECT
                            time,
                            NULL AS in0,
                            NULL AS in1,
                            NULL AS fee0,
                            NULL AS fee1,
                            out0,
                            out1
                        FROM
                            wstETH_WETH_burns
                    ) AS combined_data
                GROUP BY
                    time
            ) AS w
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
    SUM(w.net0) OVER (
        ORDER BY
            w.time
    ) as tvl0,
    SUM(w.net0 * token0.latest_price) OVER (
        ORDER BY
            w.time
    ) as tvl0_usd,
    SUM(w.net1) OVER (
        ORDER BY
            w.time
    ) as tvl1,
    SUM(w.net1 * token1.latest_price) OVER (
        ORDER BY
            w.time
    ) as tvl1_usd,
    SUM(
        w.net0 * token0.latest_price + w.net1 * token1.latest_price
    ) OVER (
        ORDER BY
            w.time
    ) as total_tvl
FROM
    wstETH_WETH_nets w
    CROSS JOIN token0
    CROSS JOIN token1
ORDER BY
    w.time;