-- ORDER: 
-- 01. CAKE_WBNB_25bps_active AS (),
-- 02. CAKE_WBNB_25bps_passive AS (),
-- 03. USDT_BUSD_1bps AS (), !! missing
-- 04. USDT_USDC_1bps AS (), !! missing
-- 05. USDT_WBNB_5bps AS (),
-- 06. WBNB_BUSD_5bps AS (),
-- 07. WETH_WBNB_5bps_active AS (),
-- 08. WETH_WBNB_5bps_passive AS (),
WITH
    -- ADDRESS = 0x7deA5e8d6269a02220608d07Ae5feaE7de856868
    CAKE_WBNB_25bps_active_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 25
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x7deA5e8d6269a02220608d07Ae5feaE7de856868
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x7deA5e8d6269a02220608d07Ae5feaE7de856868
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x7deA5e8d6269a02220608d07Ae5feaE7de856868
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
                    blockchain = 'bnb'
                    AND symbol = 'CAKE'
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    CAKE_WBNB_25bps_active AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            CAKE_WBNB_25bps_active_weekly
    ),
    -- ADDRESS = 0x5db61A5f05580Cf620a9d0f9266E7432811DC309
    CAKE_WBNB_25bps_passive_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
            SUM(
                (
                    w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
                ) * 10000 / 25
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x5db61A5f05580Cf620a9d0f9266E7432811DC309
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x5db61A5f05580Cf620a9d0f9266E7432811DC309
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x5db61A5f05580Cf620a9d0f9266E7432811DC309
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
                    blockchain = 'bnb'
                    AND symbol = 'CAKE'
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    CAKE_WBNB_25bps_passive AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            CAKE_WBNB_25bps_passive_weekly
    ),
    -- ADDRESS = 0xc2cdc718908667F1ce99E69d02a7F420bCDb34C2
    USDT_BUSD_1bps_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xc2cdc718908667F1ce99E69d02a7F420bCDb34C2
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xc2cdc718908667F1ce99E69d02a7F420bCDb34C2
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xc2cdc718908667F1ce99E69d02a7F420bCDb34C2
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
                    blockchain = 'bnb'
                    AND symbol = 'USDT'
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
                    blockchain = 'bnb'
                    AND symbol = 'BUSD'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    USDT_BUSD_1bps AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            USDT_BUSD_1bps_weekly
    ),
    -- ADDRESS = 0xFEB0819A3d00EACf1D8F593D2538C33d34b76274
    USDT_USDC_1bps_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xFEB0819A3d00EACf1D8F593D2538C33d34b76274
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xFEB0819A3d00EACf1D8F593D2538C33d34b76274
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xFEB0819A3d00EACf1D8F593D2538C33d34b76274
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
                    blockchain = 'bnb'
                    AND symbol = 'USDT'
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
                    blockchain = 'bnb'
                    AND symbol = 'USDC'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    USDT_USDC_1bps AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            USDT_USDC_1bps_weekly
    ),
    -- ADDRESS = 0xB99F1Ce0f1C95422913FAF5b1ea980BbC580c14a
    USDT_WBNB_5bps_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0xB99F1Ce0f1C95422913FAF5b1ea980BbC580c14a
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0xB99F1Ce0f1C95422913FAF5b1ea980BbC580c14a
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0xB99F1Ce0f1C95422913FAF5b1ea980BbC580c14a
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
                    blockchain = 'bnb'
                    AND symbol = 'USDT'
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    USDT_WBNB_5bps AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            USDT_WBNB_5bps_weekly
    ),
    -- ADDRESS = 0x4915342AAD6F4D2882cc41543BCE527448507930
    WBNB_BUSD_5bps_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x4915342AAD6F4D2882cc41543BCE527448507930
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x4915342AAD6F4D2882cc41543BCE527448507930
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x4915342AAD6F4D2882cc41543BCE527448507930
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
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
                    blockchain = 'bnb'
                    AND symbol = 'BUSD'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    WBNB_BUSD_5bps AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            WBNB_BUSD_5bps_weekly
    ),
    -- ADDRESS = 0x39721e66193D8a6eF8052246e95c23B6fBdD5fe5
    WETH_WBNB_5bps_active_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x39721e66193D8a6eF8052246e95c23B6fBdD5fe5
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x39721e66193D8a6eF8052246e95c23B6fBdD5fe5
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x39721e66193D8a6eF8052246e95c23B6fBdD5fe5
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
                    blockchain = 'bnb'
                    AND symbol = 'ETH'
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    WETH_WBNB_5bps_active AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            WETH_WBNB_5bps_active_weekly
    ),
    -- ADDRESS = 0x57eee8ca983FcA38dFE767070F2cE251D4e5E9e1
    WETH_WBNB_5bps_passive_weekly AS (
        SELECT
            DATE_TRUNC ('week', w.time) AS week_start,
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x57eee8ca983FcA38dFE767070F2cE251D4e5E9e1
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
                                            range_protocol_bnb.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x57eee8ca983FcA38dFE767070F2cE251D4e5E9e1
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
                                            range_protocol_bnb.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x57eee8ca983FcA38dFE767070F2cE251D4e5E9e1
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
                    blockchain = 'bnb'
                    AND symbol = 'ETH'
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
                    blockchain = 'bnb'
                    AND symbol = 'WBNB'
                ORDER BY
                    minute DESC
                LIMIT
                    1
            ) token1
        GROUP BY
            DATE_TRUNC ('week', w.time)
        ORDER BY
            DATE_TRUNC ('week', w.time)
    ),
    WETH_WBNB_5bps_passive AS (
        SELECT
            week_start,
            SUM(total_fee_usd) OVER (
                ORDER BY
                    week_start
            ) as total_fee_usd,
            CASE
                WHEN SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                ) < 0 THEN 0
                ELSE SUM(tvl_usd) OVER (
                    ORDER BY
                        week_start
                )
            END as tvl_usd,
            SUM(total_volume_usd) OVER (
                ORDER BY
                    week_start
            ) as total_volume_usd
        FROM
            WETH_WBNB_5bps_passive_weekly
    ),
    week_series AS (
        SELECT
            week_start,
            NULL as tvl_usd,
            NULL as total_fee_usd,
            NULL as total_volume_usd
        FROM
            unnest (
                sequence (
                    DATE_TRUNC ('week', timestamp '2023-05-12'),
                    CAST(NOW () AS TIMESTAMP),
                    INTERVAL '7' day
                )
            ) AS s (week_start)
    )
SELECT
    week_series.week_start AS week_start,
    COALESCE(
        last_value (CAKE_WBNB_25bps_active.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (CAKE_WBNB_25bps_passive.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_BUSD_1bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_USDC_1bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_WBNB_5bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WBNB_BUSD_5bps.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_active.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_passive.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS tvl_usd,
    COALESCE(
        last_value (CAKE_WBNB_25bps_active.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (CAKE_WBNB_25bps_passive.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_BUSD_1bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_USDC_1bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_WBNB_5bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WBNB_BUSD_5bps.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_active.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_passive.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS total_volume_usd,
    COALESCE(
        last_value (CAKE_WBNB_25bps_active.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (CAKE_WBNB_25bps_passive.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_BUSD_1bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_USDC_1bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (USDT_WBNB_5bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WBNB_BUSD_5bps.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_active.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (WETH_WBNB_5bps_passive.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS total_fee_usd
FROM
    week_series
    FULL OUTER JOIN CAKE_WBNB_25bps_active ON week_series.week_start = CAKE_WBNB_25bps_active.week_start
    FULL OUTER JOIN CAKE_WBNB_25bps_passive ON week_series.week_start = CAKE_WBNB_25bps_passive.week_start
    FULL OUTER JOIN USDT_BUSD_1bps ON week_series.week_start = USDT_BUSD_1bps.week_start
    FULL OUTER JOIN USDT_USDC_1bps ON week_series.week_start = USDT_USDC_1bps.week_start
    FULL OUTER JOIN USDT_WBNB_5bps ON week_series.week_start = USDT_WBNB_5bps.week_start
    FULL OUTER JOIN WBNB_BUSD_5bps ON week_series.week_start = WBNB_BUSD_5bps.week_start
    FULL OUTER JOIN WETH_WBNB_5bps_active ON week_series.week_start = WETH_WBNB_5bps_active.week_start
    FULL OUTER JOIN WETH_WBNB_5bps_passive ON week_series.week_start = WETH_WBNB_5bps_passive.week_start
ORDER BY
    week_start
    -- ORDER: 
    -- 01. CAKE_WBNB_25bps_active AS (),
    -- 02. CAKE_WBNB_25bps_passive AS (),
    -- 03. USDT_BUSD_1bps AS (), !! missing
    -- 04. USDT_USDC_1bps AS (), !! missing
    -- 05. USDT_WBNB_5bps AS (),
    -- 06. WBNB_BUSD_5bps AS (),
    -- 07. WETH_WBNB_5bps_active AS (),
    -- 08. WETH_WBNB_5bps_passive AS (),