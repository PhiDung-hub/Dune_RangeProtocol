-- ORDER: 
-- 01. BTCb_WETH_30bps AS (),
-- 02. MAGIC_WETH_30bps AS (),
-- 03. PENDLE_WETH_30bps AS (), !! missing
-- 04. RDNT_WETH_30bps AS (), !! missing
-- 05. RDPX_WETH_100bps AS (),
-- 06. USDC_USDT_1bps AS (),
-- 07. WETH_ARB_30bps AS (),
-- 08. WETH_GMX_30bps AS (),
-- 09. WETH_rETH_5bps AS (),
-- 10. wstETH_WETH_1bps AS (),
-- 11. MAGIC_WETH_30bps_sushiswap AS (),
-- 12. WETH_ARB_30bps_sushiswap AS (),
-- 13. WETH_SUSHI_30bps_sushiswap AS (),
-- 14. WETH_USDC_5bps_sushiswap AS (),
-- 15. ARB_USDC_camelot AS (),
-- 16. USDC_USDT_camelot AS (),
-- 17. WETH_USDC_camelot AS (),
WITH
    -- ADDRESS = 0x38260933F55db1e115Df33237bf502E1D9D33eEE
    -- !!! BTCb price IS NOT YET AVAILABLE on Dune `prices` Spellbook.
    -- Use the price of WBTC instead.
    BTCb_WETH_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 8) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x38260933F55db1e115Df33237bf502E1D9D33eEE
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 8) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x38260933F55db1e115Df33237bf502E1D9D33eEE
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 8) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x38260933F55db1e115Df33237bf502E1D9D33eEE
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WBTC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    BTCb_WETH_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            BTCb_WETH_30bps_daily
    ),
    -- ADDRESS = 0x38DbE61d7bE5f3579089e60c7ae7064012BcCC7A
    MAGIC_WETH_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x38DbE61d7bE5f3579089e60c7ae7064012BcCC7A
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x38DbE61d7bE5f3579089e60c7ae7064012BcCC7A
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x38DbE61d7bE5f3579089e60c7ae7064012BcCC7A
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'MAGIC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    MAGIC_WETH_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            MAGIC_WETH_30bps_daily
    ),
    -- ADDRESS = 0xDA578B1e1F07549259140081CC73926B5a49232d
    -- !!! PENDLE price IS NOT YET AVAILABLE on Dune `prices` Spellbook.
    PENDLE_WETH_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 6) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xDA578B1e1F07549259140081CC73926B5a49232d
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 6) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xDA578B1e1F07549259140081CC73926B5a49232d
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 6) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xDA578B1e1F07549259140081CC73926B5a49232d
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'PENDLE'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDT'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    PENDLE_WETH_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            PENDLE_WETH_30bps_daily
    ),
    -- ADDRESS = 0xD3387fcCD6ADaaBa06995058Eba21E199323F256
    -- !!! RDNT price IS NOT YET AVAILABLE on Dune `prices` Spellbook.
    RDNT_WETH_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xD3387fcCD6ADaaBa06995058Eba21E199323F256
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xD3387fcCD6ADaaBa06995058Eba21E199323F256
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xD3387fcCD6ADaaBa06995058Eba21E199323F256
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'RDNT'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    RDNT_WETH_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            RDNT_WETH_30bps_daily
    ),
    -- ADDRESS = 0xe34ACd7D13CA652A9743b27f275adB09Ebc6Fe8c
    RDPX_WETH_100bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 100
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xe34ACd7D13CA652A9743b27f275adB09Ebc6Fe8c
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xe34ACd7D13CA652A9743b27f275adB09Ebc6Fe8c
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xe34ACd7D13CA652A9743b27f275adB09Ebc6Fe8c
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'RDPX'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    RDPX_WETH_100bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            RDPX_WETH_100bps_daily
    ),
    -- ADDRESS = 0x58959803C761BfC29B056E74EE554110Db17Ecd7
    USDC_USDT_1bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 6) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x58959803C761BfC29B056E74EE554110Db17Ecd7
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 6) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x58959803C761BfC29B056E74EE554110Db17Ecd7
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 6) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x58959803C761BfC29B056E74EE554110Db17Ecd7
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDT'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    USDC_USDT_1bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            USDC_USDT_1bps_daily
    ),
    -- ADDRESS = 0x48E76CC26f53DF0Ebb98C050d83c650bFC6de46d
    WETH_ARB_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x48E76CC26f53DF0Ebb98C050d83c650bFC6de46d
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x48E76CC26f53DF0Ebb98C050d83c650bFC6de46d
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x48E76CC26f53DF0Ebb98C050d83c650bFC6de46d
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'ARB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_ARB_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_ARB_30bps_daily
    ),
    -- ADDRESS = 0x59469Bf08CDaAF043aE5DAF80641BEDfdd0455B2
    WETH_GMX_30bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x59469Bf08CDaAF043aE5DAF80641BEDfdd0455B2
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x59469Bf08CDaAF043aE5DAF80641BEDfdd0455B2
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x59469Bf08CDaAF043aE5DAF80641BEDfdd0455B2
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'GMX'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_GMX_30bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_GMX_30bps_daily
    ),
    -- ADDRESS = 0xABda61ECDbd45a02bFc5fcE2141f76D50D19bFBD
    WETH_rETH_5bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 5
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xABda61ECDbd45a02bFc5fcE2141f76D50D19bFBD
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xABda61ECDbd45a02bFc5fcE2141f76D50D19bFBD
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xABda61ECDbd45a02bFc5fcE2141f76D50D19bFBD
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'rETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_rETH_5bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_rETH_5bps_daily
    ),
    -- ADDRESS = 0x7548a71f63a2402413E9647798084E8802C288c2
    wstETH_WETH_1bps_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x7548a71f63a2402413E9647798084E8802C288c2
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x7548a71f63a2402413E9647798084E8802C288c2
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x7548a71f63a2402413E9647798084E8802C288c2
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'wstETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    wstETH_WETH_1bps AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            wstETH_WETH_1bps_daily
    ),
    -- ADDRESS = 0xFFEDFcA104772AdBC3454F7658008DB3480369B6
    MAGIC_WETH_30bps_sushiswap_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xFFEDFcA104772AdBC3454F7658008DB3480369B6
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xFFEDFcA104772AdBC3454F7658008DB3480369B6
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xFFEDFcA104772AdBC3454F7658008DB3480369B6
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'MAGIC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    MAGIC_WETH_30bps_sushiswap AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            MAGIC_WETH_30bps_sushiswap_daily
    ),
    -- ADDRESS = 0xB65Dc1b52827c1C1d743E3ddd62fF4B7998Eb3AA
    WETH_ARB_30bps_sushiswap_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xB65Dc1b52827c1C1d743E3ddd62fF4B7998Eb3AA
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xB65Dc1b52827c1C1d743E3ddd62fF4B7998Eb3AA
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xB65Dc1b52827c1C1d743E3ddd62fF4B7998Eb3AA
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'ARB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_ARB_30bps_sushiswap AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_ARB_30bps_sushiswap_daily
    ),
    -- ADDRESS = 0x24d56598Ac401dC4AcfBeb7B28a8cAef518684D6
    WETH_SUSHI_30bps_sushiswap_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 30
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x24d56598Ac401dC4AcfBeb7B28a8cAef518684D6
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x24d56598Ac401dC4AcfBeb7B28a8cAef518684D6
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x24d56598Ac401dC4AcfBeb7B28a8cAef518684D6
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'SUSHI'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_SUSHI_30bps_sushiswap AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_SUSHI_30bps_sushiswap_daily
    ),
    -- ADDRESS = 0xaA149AaC0dbaBF5B73bC81EB013463ef795Dd479
    WETH_USDC_5bps_sushiswap_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 5
            ) as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xaA149AaC0dbaBF5B73bC81EB013463ef795Dd479
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xaA149AaC0dbaBF5B73bC81EB013463ef795Dd479
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xaA149AaC0dbaBF5B73bC81EB013463ef795Dd479
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_USDC_5bps_sushiswap AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_USDC_5bps_sushiswap_daily
    ),
    -- ADDRESS = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    ARB_USDC_camelot_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            0 as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'ARB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    ARB_USDC_camelot AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            ARB_USDC_camelot_daily
    ),
    -- ADDRESS = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    USDC_USDT_camelot_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            0 as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 6) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 6) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 6) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDT'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    USDC_USDT_camelot AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            USDC_USDT_camelot_daily
    ),
    -- ADDRESS = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    WETH_USDC_camelot_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            0 as total_volume_usd,
            -- TVLs
            SUM(
                w.net0 * token0.latest_price + w.net1 * token1.latest_price
            ) as tvl_usd
        FROM
            -- combined mints + fees - burns
            (
                SELECT
                    time,
                    (
                        COALESCE(in0, 0) + COALESCE(fee0, 0) - COALESCE(out0, 0)
                    ) AS net0,
                    (
                        COALESCE(in1, 0) + COALESCE(fee1, 0) - COALESCE(out1, 0)
                    ) AS net1,
                    fee0,
                    fee1
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
                                    -- mints
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0In / pow (10, 18) as in0,
                                            amount1In / pow (10, 6) as in1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- fees earned
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            feesEarned0 / pow (10, 18) as fee0,
                                            feesEarned1 / pow (10, 6) as fee1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
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
                                    -- burns
                                    (
                                        SELECT
                                            evt_block_time as time,
                                            amount0Out / pow (10, 18) as out0,
                                            amount1Out / pow (10, 6) as out1
                                        FROM
                                            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
                                    )
                            ) AS combined_data
                        GROUP BY
                            time
                    )
            ) w
            -- token 0
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'WETH'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token0
            -- token 1
            CROSS JOIN (
                SELECT
                    price as latest_price
                FROM
                    prices.usd
                WHERE
                    blockchain = 'arbitrum'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('day', w.time)
        ORDER BY
            DATE_TRUNC ('day', w.time)
    ),
    WETH_USDC_camelot AS (
        SELECT
            day_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    day_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        day_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    day_start
            ) as total_volume_usd
        FROM
            WETH_USDC_camelot_daily
    ),
    day_series AS (
        SELECT
            day_start,
            NULL as tvl_usd,
            NULL as total_fee_usd,
            NULL as total_volume_usd
        FROM
            unnest (
                sequence (
                    DATE_TRUNC ('day', timestamp '2023-05-15'),
                    CAST(NOW () AS TIMESTAMP),
                    INTERVAL '1' day
                )
            ) AS s (day_start)
    )
SELECT
    day_series.day_start AS day_start,
    COALESCE(
        last_value (BTCb_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (PENDLE_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDNT_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDPX_WETH_100bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_1bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GMX_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_rETH_5bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (wstETH_WETH_1bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps_sushiswap.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps_sushiswap.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_SUSHI_30bps_sushiswap.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_5bps_sushiswap.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (ARB_USDC_camelot.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_camelot.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_camelot.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS tvl_usd,
    COALESCE(
        last_value (BTCb_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (PENDLE_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDNT_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDPX_WETH_100bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_1bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GMX_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_rETH_5bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (wstETH_WETH_1bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps_sushiswap.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps_sushiswap.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_SUSHI_30bps_sushiswap.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_5bps_sushiswap.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (ARB_USDC_camelot.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_camelot.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_camelot.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS total_volume_usd,
    COALESCE(
        last_value (BTCb_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (PENDLE_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDNT_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RDPX_WETH_100bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_1bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GMX_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_rETH_5bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (wstETH_WETH_1bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (MAGIC_WETH_30bps_sushiswap.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_ARB_30bps_sushiswap.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_SUSHI_30bps_sushiswap.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_5bps_sushiswap.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (ARB_USDC_camelot.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_USDT_camelot.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_USDC_camelot.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS total_fee_usd
FROM
    day_series
    FULL OUTER JOIN BTCb_WETH_30bps ON day_series.day_start = BTCb_WETH_30bps.day_start
    FULL OUTER JOIN MAGIC_WETH_30bps ON day_series.day_start = MAGIC_WETH_30bps.day_start
    FULL OUTER JOIN PENDLE_WETH_30bps ON day_series.day_start = PENDLE_WETH_30bps.day_start
    FULL OUTER JOIN RDNT_WETH_30bps ON day_series.day_start = RDNT_WETH_30bps.day_start
    FULL OUTER JOIN RDPX_WETH_100bps ON day_series.day_start = RDPX_WETH_100bps.day_start
    FULL OUTER JOIN USDC_USDT_1bps ON day_series.day_start = USDC_USDT_1bps.day_start
    FULL OUTER JOIN WETH_ARB_30bps ON day_series.day_start = WETH_ARB_30bps.day_start
    FULL OUTER JOIN WETH_GMX_30bps ON day_series.day_start = WETH_GMX_30bps.day_start
    FULL OUTER JOIN WETH_rETH_5bps ON day_series.day_start = WETH_rETH_5bps.day_start
    FULL OUTER JOIN wstETH_WETH_1bps ON day_series.day_start = wstETH_WETH_1bps.day_start
    FULL OUTER JOIN MAGIC_WETH_30bps_sushiswap ON day_series.day_start = MAGIC_WETH_30bps_sushiswap.day_start
    FULL OUTER JOIN WETH_ARB_30bps_sushiswap ON day_series.day_start = WETH_ARB_30bps_sushiswap.day_start
    FULL OUTER JOIN WETH_SUSHI_30bps_sushiswap ON day_series.day_start = WETH_SUSHI_30bps_sushiswap.day_start
    FULL OUTER JOIN WETH_USDC_5bps_sushiswap ON day_series.day_start = WETH_USDC_5bps_sushiswap.day_start
    FULL OUTER JOIN ARB_USDC_camelot ON day_series.day_start = ARB_USDC_camelot.day_start
    FULL OUTER JOIN USDC_USDT_camelot ON day_series.day_start = USDC_USDT_camelot.day_start
    FULL OUTER JOIN WETH_USDC_camelot ON day_series.day_start = WETH_USDC_camelot.day_start
ORDER BY
    day_start
    -- ORDER: 
    -- 01. BTCb_WETH_30bps AS (),
    -- 02. MAGIC_WETH_30bps AS (),
    -- 03. PENDLE_WETH_30bps AS (), !! missing
    -- 04. RDNT_WETH_30bps AS (), !! missing
    -- 05. RDPX_WETH_100bps AS (),
    -- 06. USDC_USDT_1bps AS (),
    -- 07. WETH_ARB_30bps AS (),
    -- 08. WETH_GMX_30bps AS (),
    -- 09. WETH_rETH_5bps AS (),
    -- 10. wstETH_WETH_1bps AS (),
    -- 11. MAGIC_WETH_30bps_sushiswap AS (),
    -- 12. WETH_ARB_30bps_sushiswap AS (),
    -- 13. WETH_SUSHI_30bps_sushiswap AS (),
    -- 14. WETH_USDC_5bps_sushiswap AS (),
    -- 15. ARB_USDC_camelot AS (),
    -- 16. USDC_USDT_camelot AS (),
    -- 17. WETH_USDC_camelot AS (),