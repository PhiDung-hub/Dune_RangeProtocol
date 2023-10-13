WITH
    users AS (
        SELECT
            receiver,
            contract_address,
            evt_block_time as time
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address,
            evt_block_time as time
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
    )
SELECT
    t1.time,
    COUNT(DISTINCT t2.receiver) AS user_count
FROM
    users t1
    LEFT JOIN users t2 ON t2.time <= t1.time
GROUP BY
    t1.time,
    t1.receiver
ORDER BY
    t1.time