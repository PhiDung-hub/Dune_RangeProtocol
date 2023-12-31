WITH
    arbitrum_daily_stats AS (
        SELECT
            *
        FROM
            dune.rangeprotocol_user.result_range_protocol_arbitrum
    ),
    base_daily_stats AS (
        SELECT
            *
        FROM
            dune.rangeprotocol_user.result_range_protocol_base
    ),
    bnb_daily_stats AS (
        SELECT
            *
        FROM
            dune.rangeprotocol_user.result_range_protocol_bnb
    ),
    ethereum_daily_stats AS (
        SELECT
            *
        FROM
            dune.rangeprotocol_user.result_range_protocol_ethereum
    ),
    polygon_daily_stats AS (
        SELECT
            *,
            NULL as total_volume_usd
        FROM
            dune.rangeprotocol_user.result_range_protocol_polygon
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
                    DATE_TRUNC ('day', timestamp '2023-05-10'),
                    CAST(NOW () AS TIMESTAMP),
                    INTERVAL '1' day
                )
            ) AS s (day_start)
    )
SELECT
    day_series.day_start AS day_start,
    COALESCE(
        last_value (arbitrum_daily_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) as arbitrum_daily_tvl,
    COALESCE(
        last_value (base_daily_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS base_daily_tvl,
    COALESCE(
        last_value (bnb_daily_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS bnb_daily_tvl,
    COALESCE(
        last_value (ethereum_daily_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS ethereum_daily_tvl,
    COALESCE(
        last_value (polygon_daily_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS polygon_daily_tvl,
    COALESCE(
        last_value (arbitrum_daily_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) as arbitrum_daily_fee,
    COALESCE(
        last_value (base_daily_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS base_daily_fee,
    COALESCE(
        last_value (bnb_daily_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS bnb_daily_fee,
    COALESCE(
        last_value (ethereum_daily_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS ethereum_daily_fee,
    COALESCE(
        last_value (polygon_daily_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS polygon_daily_fee,
    COALESCE(
        last_value (arbitrum_daily_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) as arbitrum_daily_volume,
    COALESCE(
        last_value (base_daily_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS base_daily_volume,
    COALESCE(
        last_value (bnb_daily_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS bnb_daily_volume,
    COALESCE(
        last_value (ethereum_daily_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                day_series.day_start
        ),
        0
    ) AS ethereum_daily_volume
FROM
    day_series
    FULL OUTER JOIN arbitrum_daily_stats ON day_series.day_start = arbitrum_daily_stats.day_start
    FULL OUTER JOIN base_daily_stats ON day_series.day_start = base_daily_stats.day_start
    FULL OUTER JOIN bnb_daily_stats ON day_series.day_start = bnb_daily_stats.day_start
    FULL OUTER JOIN ethereum_daily_stats ON day_series.day_start = ethereum_daily_stats.day_start
    FULL OUTER JOIN polygon_daily_stats ON day_series.day_start = polygon_daily_stats.day_start
ORDER BY
    day_start
