-- ADDRESS = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
WITH
    -- token0 = ARB, token1 = USDC
    fees AS (
        SELECT
            evt_block_time as time,
            feesEarned0 / pow (10, 18) as fee0,
            feesEarned1 / pow (10, 6) as fee1
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_FeesEarned
        WHERE
            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    ),
    mints AS (
        SELECT
            evt_block_time as time,
            amount0In / pow (10, 18) as in0,
            amount1In / pow (10, 6) as in1
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
        WHERE
            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    ),
    burns AS (
        SELECT
            evt_block_time as time,
            amount0Out / pow (10, 18) as out0,
            amount1Out / pow (10, 6) as out1
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
        WHERE
            contract_address = 0x110f77421A7EA1BcEb041b4C1c6f59A9df708e99
    ),
    nets AS (
        SELECT
            w.time,
            (
                COALESCE(w.in0, 0) + COALESCE(w.fee0, 0) - COALESCE(w.out0, 0)
            ) AS net0,
            (
                COALESCE(w.in1, 0) + COALESCE(w.fee1, 0) - COALESCE(w.out1, 0)
            ) AS net1,
            w.fee0 as fee0,
            w.fee1 as fee1
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
                            mints
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
                            fees
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
                            burns
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
            blockchain = 'arbitrum'
            AND symbol = 'ARB'
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
            blockchain = 'arbitrum'
            AND symbol = 'USDC'
        ORDER BY
            minute DESC
        LIMIT
            1
    )
SELECT
    w.time,
    -- Fees
    SUM(w.fee0) OVER (
        ORDER BY
            w.time
    ) as fee0,
    SUM(w.fee1) OVER (
        ORDER BY
            w.time
    ) as fee1,
    SUM(
        w.fee0 * token0.latest_price + w.fee1 * token1.latest_price
    ) OVER (
        ORDER BY
            w.time
    ) as total_fee_usd,
    -- TVLs
    SUM(
        w.net0 * token0.latest_price + w.net1 * token1.latest_price
    ) OVER (
        ORDER BY
            w.time
    ) as tvl_usd
FROM
    nets w
    CROSS JOIN token0
    CROSS JOIN token1
ORDER BY
    w.time;