WITH
    ethereum_active_users AS (
        SELECT
            DATE_TRUNC ('month', time) AS month,
            COUNT(DISTINCT user) AS ethereum_user_count
        FROM
            (
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_ethereum.RangeProtocolVault_evt_Minted
                UNION -- remove duplicate
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_ethereum.RangeProtocolVault_evt_Burned
            )
        GROUP BY
            DATE_TRUNC ('month', time)
        ORDER BY
            month
    ),
    arbitrum_active_users AS (
        SELECT
            DATE_TRUNC ('month', time) AS month,
            COUNT(DISTINCT user) AS arbitrum_user_count
        FROM
            (
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_arbitrum.RangeProtocolVault_evt_Minted
                UNION -- remove duplicate
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_arbitrum.RangeProtocolVault_evt_Burned
            )
        GROUP BY
            DATE_TRUNC ('month', time)
        ORDER BY
            month
    ),
    bnb_active_users AS (
        SELECT
            DATE_TRUNC ('month', time) AS month,
            COUNT(DISTINCT user) AS bnb_user_count
        FROM
            (
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_bnb.RangeProtocolVault_evt_Minted
                UNION -- remove duplicate
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_bnb.RangeProtocolVault_evt_Burned
            )
        GROUP BY
            DATE_TRUNC ('month', time)
        ORDER BY
            month
    ),
    base_active_users AS (
        SELECT
            DATE_TRUNC ('month', time) AS month,
            COUNT(DISTINCT user) AS base_user_count
        FROM
            (
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_base.RangeProtocolVault_evt_Minted
                UNION -- remove duplicate
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_base.RangeProtocolVault_evt_Burned
            )
        GROUP BY
            DATE_TRUNC ('month', time)
        ORDER BY
            month
    ),
    polygon_active_users AS (
        SELECT
            DATE_TRUNC ('month', time) AS month,
            COUNT(DISTINCT user) AS polygon_user_count
        FROM
            (
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_polygon.RangeProtocolVault_evt_Minted
                UNION -- remove duplicate
                SELECT
                    evt_block_time as time,
                    receiver as user
                FROM
                    range_protocol_polygon.RangeProtocolVault_evt_Burned
            )
        GROUP BY
            DATE_TRUNC ('month', time)
        ORDER BY
            month
    ),
    user_count_by_chain AS (
        SELECT
            ethereum_active_users.month as month,
            COALESCE(ethereum_user_count, 0) as ethereum,
            COALESCE(arbitrum_user_count, 0) as arbitrum,
            COALESCE(bnb_user_count, 0) as bnb,
            COALESCE(base_user_count, 0) as base,
            COALESCE(polygon_user_count, 0) as polygon
        FROM
            ethereum_active_users
            LEFT JOIN arbitrum_active_users ON ethereum_active_users.month = arbitrum_active_users.month
            LEFT JOIN base_active_users ON ethereum_active_users.month = base_active_users.month
            LEFT JOIN bnb_active_users ON ethereum_active_users.month = bnb_active_users.month
            LEFT JOIN polygon_active_users ON ethereum_active_users.month = polygon_active_users.month
    ),
    all_blockchains AS (
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_bnb.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_bnb.RangeProtocolVault_evt_Burned
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_base.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_base.RangeProtocolVault_evt_Burned
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_polygon.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            DATE_TRUNC ('month', evt_block_time) AS month,
            receiver AS user
        FROM
            range_protocol_polygon.RangeProtocolVault_evt_Burned
    ),
    all_active_users AS (
        SELECT
            month,
            COUNT(DISTINCT user) AS total_user_count
        FROM
            all_blockchains
        GROUP BY
            month
        ORDER BY
            month
    )
SELECT
    chains.month,
    SUM(total_user_count) OVER (
        ORDER BY
            chains.month
    ) AS all_chains,
    SUM(ethereum) OVER (
        ORDER BY
            chains.month
    ) AS ethereum,
    SUM(arbitrum) OVER (
        ORDER BY
            chains.month
    ) AS arbitrum,
    SUM(bnb) OVER (
        ORDER BY
            chains.month
    ) AS bnb,
    SUM(base) OVER (
        ORDER BY
            chains.month
    ) AS base,
    SUM(polygon) OVER (
        ORDER BY
            chains.month
    ) AS polygon
FROM
    user_count_by_chain chains
    JOIN all_active_users ON chains.month = all_active_users.month
ORDER BY
    month ASC;