WITH
    users AS (
        SELECT
            receiver, contract_address
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver, contract_address
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
    )
SELECT
    COUNT(receiver)
FROM
    users AS user_count;