
# Global configuration settings for the RunDB server
class Config:
    HOST = "0.0.0.0"                                     # host IP address
    PORT = 7379                                          # host port
    KEY_LIMIT = 100                                      # Max number of keys
    MAX_CLIENTS = 10_000                                 # Max number of clients 
    CRON_FREQ_INTERVAL = 1                               # Periodic Time interval (in secs) for checking for expired keys 
    AOF_FILE = "run-master.aof"                          # File name for AOF Logging
    EVICTION_STRATEGY = "allkeys-lru"                    # Eviction strategy name       
    EVICTION_RATIO = 0.1                                 # Eviction ratio (% keys evicted)
    DB_COUNT = 4                                         # DB count
    LRU_BITS_MASK = 0x00FFFFFF                           # 24 bit mask for LRU Clock (DONT CHANGE)
    EVICTION_POOL_SIZE = 16                              # Eviction pool size for all-keys LRU eviction strategy
    EVICTION_SAMPLE_SIZE = 5