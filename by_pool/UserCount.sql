WITH
    users AS (
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_ethereum.RangeProtocolVault_evt_Burned
    )
SELECT
    COUNT(receiver)
FROM
    users AS user_count
WHERE
    -- NOTE: change contract address;
    contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1;