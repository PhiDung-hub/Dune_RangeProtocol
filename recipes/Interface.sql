-- RANGE SIGNATURES
-- Keccak-256 calculator: https://emn178.github.io/online-tools/keccak_256.html
--
-- Range Vault is live on ethereum: https://dune.com/queries/3063888?category=decoded_project&namespace=range_protocol&blockchains=ethereum&contract=RangeProtocolVault
--
-- event Minted(address indexed receiver, uint256 mintAmount, uint256 amount0In,  uint256 amount1In);
-- 0x5a3358a3d27a5373c0df2604662088d37894d56b7cfd27f315770440f4e0d919
WITH
  mints AS (
    SELECT
      block_time as time,
      topic1 as minter
    FROM
      ethereum.logs
    WHERE
      contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
      AND
      -- Event: Minted(msg.sender, mintAmount, amount0, amount1)
      topic0 = 0x5a3358a3d27a5373c0df2604662088d37894d56b7cfd27f315770440f4e0d919
  );

-- event Burned(address indexed receiver, uint256 mintAmount, uint256 amount0Out,  uint256 amount1Out);
-- 0x4c60206a5c1de41f3376d1d60f0949d96cb682033c90b1c2d9d9a62d4c4120c0
WITH
  burns AS (
    SELECT
      block_time as time,
      topic1 as minter
    FROM
      ethereum.logs
    WHERE
      contract_address = 0xF9ab542616A0C8fA94e41c968622C3b2367F5ad1
      AND
      -- Event: Minted(msg.sender, mintAmount, amount0, amount1)
      topic0 = 0x4c60206a5c1de41f3376d1d60f0949d96cb682033c90b1c2d9d9a62d4c4120c0
  );
