WITH
    users AS (
        SELECT
            receiver,
            contract_address,
            evt_block_time AS time
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address,
            evt_block_time AS time
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
    ),
    monthly_users AS (
        SELECT
            DATE_TRUNC('month', time) AS month,
            COUNT(DISTINCT receiver) AS user_count
        FROM
            users
        GROUP BY
            DATE_TRUNC('month', time)
        ORDER BY
            month
    )
SELECT
    *,
    SUM(user_count) OVER (ORDER BY month) AS cumulative_user_count
FROM
    monthly_users;
