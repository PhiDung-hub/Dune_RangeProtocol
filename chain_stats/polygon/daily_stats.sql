-- ORDER: 
-- 01. USDC_WETH AS (),
-- 02. WMATIC_WETH AS (),
WITH
    -- ADDRESS = 0x04f7a8FD669B6e84c3A642f6f48B1200A4B1E1E2
    USDC_WETH_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
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
                                            range_protocol_polygon.RangeProtocolVault_evt_Minted
                                        WHERE
                                            contract_address = 0x04f7a8FD669B6e84c3A642f6f48B1200A4B1E1E2
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
                                            range_protocol_polygon.RangeProtocolVault_evt_FeesEarned
                                        WHERE
                                            contract_address = 0x04f7a8FD669B6e84c3A642f6f48B1200A4B1E1E2
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
                                            range_protocol_polygon.RangeProtocolVault_evt_Burned
                                        WHERE
                                            contract_address = 0x04f7a8FD669B6e84c3A642f6f48B1200A4B1E1E2
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
                    blockchain = 'polygon'
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
                    blockchain = 'polygon'
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
    USDC_WETH AS (
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
            END as tvl_usd
        FROM
            USDC_WETH_daily
    ),
    -- ADDRESS = 0xB99F1Ce0f1C95422913FAF5b1ea980BbC580c14a
    WMATIC_WETH_daily AS (
        SELECT
            DATE_TRUNC ('day', w.time) AS day_start,
            -- Fees
            SUM(w.fee0) as fee0,
            SUM(w.fee1) as fee1,
            SUM(
                w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
            ) as total_fee_usd,
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
                                            range_protocol_polygon.RangeProtocolVault_evt_Minted
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
                                            range_protocol_polygon.RangeProtocolVault_evt_FeesEarned
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
                                            range_protocol_polygon.RangeProtocolVault_evt_Burned
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
                    blockchain = 'polygon'
                    AND symbol = 'WMATIC'
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
                    blockchain = 'polygon'
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
    WMATIC_WETH AS (
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
            END as tvl_usd
        FROM
            WMATIC_WETH_daily
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
        last_value (USDC_WETH.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WMATIC_WETH.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS tvl_usd,
    COALESCE(
        last_value (USDC_WETH.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) + COALESCE(
        last_value (WMATIC_WETH.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS total_fee_usd
FROM
    day_series
    FULL OUTER JOIN USDC_WETH ON day_series.day_start = USDC_WETH.day_start
    FULL OUTER JOIN WMATIC_WETH ON day_series.day_start = WMATIC_WETH.day_start
ORDER BY
    day_start
    -- ORDER: 
    -- 01. USDC_WETH AS (),
    -- 02. WMATIC_WETH AS (),