-- calculate the exact user whenever a mint event is emitted
WITH
    users AS (
        SELECT
            receiver,
            contract_address,
            evt_block_time as time
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
    )
SELECT
    t1.time,
    COUNT(DISTINCT t2.receiver) AS user_count
FROM
    (
        SELECT
            *
        FROM
            users
        WHERE
            -- NOTE: change contract address
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ) AS t1
    LEFT JOIN (
        SELECT
            *
        FROM
            users
        WHERE
            -- NOTE: change contract address
            contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
    ) AS t2 ON t2.time <= t1.time
GROUP BY
    t1.time,
    t1.receiver
ORDER BY
    t1.time
