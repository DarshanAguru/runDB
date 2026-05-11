
# Global configuration settings for the RunDB server
class Config:
    HOST = "0.0.0.0"
    PORT = 7379
    KEY_LIMIT = 20_000
    MAX_CLIENTS = 10_000
    AOF_FILE = "run-master.aof"
    EVICTION_STRATEGY = "simple-first"