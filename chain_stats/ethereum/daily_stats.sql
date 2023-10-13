-- ORDER: 
-- 01. ANKR_WETH_30bps AS (),
-- 02. APE_WETH_30bps AS (),
-- 03. FXS_WETH_30bps AS (),
-- 04. GAL_WETH_100bps AS (),
-- 05. LDO_WETH_30bps AS (),
-- 06. LQTY_WETH_30bps AS (),
-- 07. RNDR_WETH_100bps AS (),
-- 08. USDC_USDT_1bps AS (),
-- 09. USDC_WETH_5bps_active AS (),
-- 10. USDC_WETH_5bps_passive AS (),
-- 11. WETH_GALA_30bps AS (),
-- 12. WETH_GRT_30bps AS (),
-- 13. wstETH_WETH_1bps AS (),
-- 14. wstETH_WETH_5bps AS (),
WITH
    -- ADDRESS = 0x91e0FBD44472511f815680382f5E781C3B8285B4
    ANKR_WETH_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x91e0FBD44472511f815680382f5E781C3B8285B4
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x91e0FBD44472511f815680382f5E781C3B8285B4
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x91e0FBD44472511f815680382f5E781C3B8285B4
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
                    blockchain = 'ethereum'
                    AND symbol = 'ANKR'
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
                    blockchain = 'ethereum'
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
    ANKR_WETH_30bps AS (
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
            ANKR_WETH_30bps_daily
    ),
    -- ADDRESS = 0x74e3D57025Bb1fB972eE336C93dF87c179250F5E
    APE_WETH_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x74e3D57025Bb1fB972eE336C93dF87c179250F5E
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x74e3D57025Bb1fB972eE336C93dF87c179250F5E
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x74e3D57025Bb1fB972eE336C93dF87c179250F5E
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
                    blockchain = 'ethereum'
                    AND symbol = 'APE'
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
                    blockchain = 'ethereum'
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
    APE_WETH_30bps AS (
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
            APE_WETH_30bps_daily
    ),
    -- ADDRESS = 0x965e7249CfBDa46120C88EFcCdE1D9bD02AD7e2F
    FXS_WETH_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x965e7249CfBDa46120C88EFcCdE1D9bD02AD7e2F
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x965e7249CfBDa46120C88EFcCdE1D9bD02AD7e2F
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x965e7249CfBDa46120C88EFcCdE1D9bD02AD7e2F
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
                    blockchain = 'ethereum'
                    AND symbol = 'FXS'
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
                    blockchain = 'ethereum'
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
    FXS_WETH_30bps AS (
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
            FXS_WETH_30bps_daily
    ),
    -- ADDRESS = 0xc1fD7257645a3A93989Bab15ba32EA315C8f3117
    GAL_WETH_100bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xc1fD7257645a3A93989Bab15ba32EA315C8f3117
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xc1fD7257645a3A93989Bab15ba32EA315C8f3117
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xc1fD7257645a3A93989Bab15ba32EA315C8f3117
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
                    blockchain = 'ethereum'
                    AND symbol = 'GAL'
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
                    blockchain = 'ethereum'
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
    GAL_WETH_100bps AS (
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
            GAL_WETH_100bps_daily
    ),
    -- ADDRESS = 0x52fC153d440c669c1Fc8779A0508795832a51167
    LDO_WETH_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x52fC153d440c669c1Fc8779A0508795832a51167
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x52fC153d440c669c1Fc8779A0508795832a51167
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x52fC153d440c669c1Fc8779A0508795832a51167
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
                    blockchain = 'ethereum'
                    AND symbol = 'LDO'
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
                    blockchain = 'ethereum'
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
    LDO_WETH_30bps AS (
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
            LDO_WETH_30bps_daily
    ),
    -- ADDRESS = 0x350D81A7733Ee6b001966e0844A0ebb096FAbF0f
    LQTY_WETH_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x350D81A7733Ee6b001966e0844A0ebb096FAbF0f
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x350D81A7733Ee6b001966e0844A0ebb096FAbF0f
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x350D81A7733Ee6b001966e0844A0ebb096FAbF0f
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
                    blockchain = 'ethereum'
                    AND symbol = 'LQTY'
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
                    blockchain = 'ethereum'
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
    LQTY_WETH_30bps AS (
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
            LQTY_WETH_30bps_daily
    ),
    -- ADDRESS = 0x06Bb3234927Fd175dFB77225DC434A2BfaB42977
    RNDR_WETH_100bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x06Bb3234927Fd175dFB77225DC434A2BfaB42977
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x06Bb3234927Fd175dFB77225DC434A2BfaB42977
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x06Bb3234927Fd175dFB77225DC434A2BfaB42977
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
                    blockchain = 'ethereum'
                    AND symbol = 'RNDR'
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
                    blockchain = 'ethereum'
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
    RNDR_WETH_100bps AS (
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
            RNDR_WETH_100bps_daily
    ),
    -- ADDRESS = 0xd40A5C0642721c0A6C6db381ccd868aa646AE10a
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xd40A5C0642721c0A6C6db381ccd868aa646AE10a
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xd40A5C0642721c0A6C6db381ccd868aa646AE10a
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xd40A5C0642721c0A6C6db381ccd868aa646AE10a
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
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
    -- ADDRESS = 0x9Ad8d0df2dA118DcE898b7F5BD9Ab749c593A5d9
    USDC_WETH_5bps_active_daily AS (
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
                                            amount0In / pow (10, 6) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x9Ad8d0df2dA118DcE898b7F5BD9Ab749c593A5d9
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
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x9Ad8d0df2dA118DcE898b7F5BD9Ab749c593A5d9
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
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x9Ad8d0df2dA118DcE898b7F5BD9Ab749c593A5d9
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
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
    USDC_WETH_5bps_active AS (
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
            USDC_WETH_5bps_active_daily
    ),
    -- ADDRESS = 0x3c0ACF2AC603837eFA8B247A54b42b71e706ef71
    USDC_WETH_5bps_passive_daily AS (
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
                                            amount0In / pow (10, 6) as in0,
                                            amount1In / pow (10, 18) as in1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x3c0ACF2AC603837eFA8B247A54b42b71e706ef71
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
                                            feesEarned1 / pow (10, 18) as fee1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x3c0ACF2AC603837eFA8B247A54b42b71e706ef71
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
                                            amount1Out / pow (10, 18) as out1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x3c0ACF2AC603837eFA8B247A54b42b71e706ef71
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
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
    USDC_WETH_5bps_passive AS (
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
            USDC_WETH_5bps_passive_daily
    ),
    -- ADDRESS = 0x252B35419180f0f1a1B287C637f475fBaF62B053
    WETH_GALA_30bps_daily AS (
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
                                            amount1In / pow (10, 8) as in1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x252B35419180f0f1a1B287C637f475fBaF62B053
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
                                            feesEarned1 / pow (10, 8) as fee1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x252B35419180f0f1a1B287C637f475fBaF62B053
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
                                            amount1Out / pow (10, 8) as out1
                                        FROM
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x252B35419180f0f1a1B287C637f475fBaF62B053
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
                    AND symbol = 'GALA'
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
    WETH_GALA_30bps AS (
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
            WETH_GALA_30bps_daily
    ),
    -- ADDRESS = 0x8Ae8b0C4e804A87CA20BB14DBDbFEFf2f2f1BD44
    WETH_GRT_30bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x8Ae8b0C4e804A87CA20BB14DBDbFEFf2f2f1BD44
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x8Ae8b0C4e804A87CA20BB14DBDbFEFf2f2f1BD44
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x8Ae8b0C4e804A87CA20BB14DBDbFEFf2f2f1BD44
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
                    AND symbol = 'GRT'
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
    WETH_GRT_30bps AS (
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
            WETH_GRT_30bps_daily
    ),
    -- ADDRESS = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
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
    -- ADDRESS = 0x3d0D622513191E8CF2ED5A340A9180bbfA2Ca95D
    wstETH_WETH_5bps_daily AS (
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x3d0D622513191E8CF2ED5A340A9180bbfA2Ca95D
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x3d0D622513191E8CF2ED5A340A9180bbfA2Ca95D
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
                                            range_protocol_ethereum.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x3d0D622513191E8CF2ED5A340A9180bbfA2Ca95D
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
                    blockchain = 'ethereum'
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
                    blockchain = 'ethereum'
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
    wstETH_WETH_5bps AS (
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
            wstETH_WETH_5bps_daily
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
        last_value (ANKR_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (APE_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (FXS_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (GAL_WETH_100bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LDO_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LQTY_WETH_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RNDR_WETH_100bps.tvl_usd) IGNORE NULLS OVER (
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
        last_value (USDC_WETH_5bps_active.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_WETH_5bps_passive.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GALA_30bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GRT_30bps.tvl_usd) IGNORE NULLS OVER (
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
        last_value (wstETH_WETH_5bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS tvl_usd,
    COALESCE(
        last_value (ANKR_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (APE_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (FXS_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (GAL_WETH_100bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LDO_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LQTY_WETH_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RNDR_WETH_100bps.total_volume_usd) IGNORE NULLS OVER (
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
        last_value (USDC_WETH_5bps_active.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_WETH_5bps_passive.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GALA_30bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GRT_30bps.total_volume_usd) IGNORE NULLS OVER (
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
        last_value (wstETH_WETH_5bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS total_volume_usd,
    COALESCE(
        last_value (ANKR_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (APE_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (FXS_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (GAL_WETH_100bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LDO_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (LQTY_WETH_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (RNDR_WETH_100bps.total_fee_usd) IGNORE NULLS OVER (
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
        last_value (USDC_WETH_5bps_active.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (USDC_WETH_5bps_passive.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GALA_30bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_GRT_30bps.total_fee_usd) IGNORE NULLS OVER (
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
        last_value (wstETH_WETH_5bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS total_fee_usd
FROM
    day_series
    FULL OUTER JOIN wstETH_WETH_1bps ON day_series.day_start = wstETH_WETH_1bps.day_start
    FULL OUTER JOIN wstETH_WETH_5bps ON day_series.day_start = wstETH_WETH_5bps.day_start
    FULL OUTER JOIN USDC_USDT_1bps ON day_series.day_start = USDC_USDT_1bps.day_start
    FULL OUTER JOIN ANKR_WETH_30bps ON day_series.day_start = ANKR_WETH_30bps.day_start
    FULL OUTER JOIN APE_WETH_30bps ON day_series.day_start = APE_WETH_30bps.day_start
    FULL OUTER JOIN FXS_WETH_30bps ON day_series.day_start = FXS_WETH_30bps.day_start
    FULL OUTER JOIN GAL_WETH_100bps ON day_series.day_start = GAL_WETH_100bps.day_start
    FULL OUTER JOIN LDO_WETH_30bps ON day_series.day_start = LDO_WETH_30bps.day_start
    FULL OUTER JOIN LQTY_WETH_30bps ON day_series.day_start = LQTY_WETH_30bps.day_start
    FULL OUTER JOIN RNDR_WETH_100bps ON day_series.day_start = RNDR_WETH_100bps.day_start
    FULL OUTER JOIN USDC_WETH_5bps_active ON day_series.day_start = USDC_WETH_5bps_active.day_start
    FULL OUTER JOIN USDC_WETH_5bps_passive ON day_series.day_start = USDC_WETH_5bps_passive.day_start
    FULL OUTER JOIN WETH_GALA_30bps ON day_series.day_start = WETH_GALA_30bps.day_start
    FULL OUTER JOIN WETH_GRT_30bps ON day_series.day_start = WETH_GRT_30bps.day_start
ORDER BY
    day_start
    -- ORDER: 
    -- 01. ANKR_WETH_30bps AS (),
    -- 02. APE_WETH_30bps AS (),
    -- 03. FXS_WETH_30bps AS (),
    -- 04. GAL_WETH_100bps AS (),
    -- 05. LDO_WETH_30bps AS (),
    -- 06. LQTY_WETH_30bps AS (),
    -- 07. RNDR_WETH_100bps AS (),
    -- 08. USDC_USDT_1bps AS (),
    -- 09. USDC_WETH_5bps_active AS (),
    -- 10. USDC_WETH_5bps_passive AS (),
    -- 11. WETH_GALA_30bps AS (),
    -- 12. WETH_GRT_30bps AS (),
    -- 13. wstETH_WETH_1bps AS (),
    -- 14. wstETH_WETH_5bps AS (),