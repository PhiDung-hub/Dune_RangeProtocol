-- Due to materialized views limitation (5 for FREE PLAN), only reports daily
WITH
    arbitrum_weekly_stats AS (
        SELECT
            *
        FROM
            dune.pdung001.result_arbitrum_weekly
    ),
    base_weekly_stats AS (
        SELECT
            *
        FROM
            dune.pdung001.result_base_weekly
    ),
    bnb_weekly_stats AS (
        SELECT
            *
        FROM
            dune.pdung001.result_bnb_weekly
    ),
    ethereum_weekly_stats AS (
        SELECT
            *
        FROM
            dune.pdung001.result_ethereum_weekly
    ),
    polygon_weekly_stats AS (
        SELECT
            *,
            NULL as total_volume_usd
        FROM
            dune.pdung001.result_polygon_weekly
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
        last_value (arbitrum_weekly_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (base_weekly_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (bnb_weekly_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (ethereum_weekly_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (polygon_weekly_stats.tvl_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS tvl_usd,
    COALESCE(
        last_value (arbitrum_weekly_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (base_weekly_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (bnb_weekly_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (ethereum_weekly_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (polygon_weekly_stats.total_volume_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS total_volume_usd,
    COALESCE(
        last_value (arbitrum_weekly_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (base_weekly_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (bnb_weekly_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (ethereum_weekly_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) + COALESCE(
        last_value (polygon_weekly_stats.total_fee_usd) IGNORE NULLS OVER (
            ORDER BY
                week_series.week_start
        ),
        0
    ) AS total_fee_usd
FROM
    week_series
    FULL OUTER JOIN arbitrum_weekly_stats ON week_series.week_start = arbitrum_weekly_stats.week_start
    FULL OUTER JOIN base_weekly_stats ON week_series.week_start = base_weekly_stats.week_start
    FULL OUTER JOIN bnb_weekly_stats ON week_series.week_start = bnb_weekly_stats.week_start
    FULL OUTER JOIN ethereum_weekly_stats ON week_series.week_start = ethereum_weekly_stats.week_start
    FULL OUTER JOIN polygon_weekly_stats ON week_series.week_start = polygon_weekly_stats.week_start
ORDER BY
    week_start