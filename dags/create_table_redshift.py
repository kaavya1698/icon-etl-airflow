

blocks_table_create= ("""CREATE TABLE IF NOT EXISTS icon.blocks (
  number            BIGINT         NOT NULL,     -- The block number
  hash              VARCHAR(65535) NOT NULL,     -- Hash of the block
  parent_hash       VARCHAR(65535) NOT NULL,     -- Hash of the parent block
  merkle_root_hash  VARCHAR(65535) NOT NULL,     -- The root of the merkle trie of the block
  timestamp         BIGINT         NOT NULL,     -- The unix timestamp when the block was collated
  transaction_count BIGINT         NOT NULL,     -- The number of transactions the block
  version           VARCHAR(65535) NOT NULL,     -- The version of the block
  signature         VARCHAR(65535) NOT NULL,     -- The blocks signature
  next_leader       VARCHAR(65535) NOT NULL,     -- The next leader
  PRIMARY KEY (number)
);
""")
