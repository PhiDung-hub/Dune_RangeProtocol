WITH
    ethereum_users AS (
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
    ),
    arbitrum_users AS (
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_arbitrum.RangeProtocolVault_evt_Burned
    ),
    base_users AS (
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_base.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_base.RangeProtocolVault_evt_Burned
    ),
    bnb_users AS (
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_bnb.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_bnb.RangeProtocolVault_evt_Burned
    ),
    polygon_users AS (
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_polygon.RangeProtocolVault_evt_Minted
        UNION
        SELECT
            receiver,
            contract_address
        FROM
            range_protocol_polygon.RangeProtocolVault_evt_Burned
    )
SELECT
    'Ethereum' AS chain,
    COUNT(DISTINCT receiver) AS user_count
FROM
    ethereum_users
UNION ALL
SELECT
    'Arbitrum' AS chain,
    COUNT(DISTINCT receiver) AS user_count
FROM
    arbitrum_users
UNION ALL
SELECT
    'Base' AS chain,
    COUNT(DISTINCT receiver) AS user_count
FROM
    base_users
UNION ALL
SELECT
    'Binance' AS chain,
    COUNT(DISTINCT receiver) AS user_count
FROM
    bnb_users
UNION ALL
SELECT
    'Polygon' AS chain,
    COUNT(DISTINCT receiver) AS user_count
FROM
    polygon_users;