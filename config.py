
# Global configuration settings for the RunDB server
class Config:
    HOST = "0.0.0.0"                                     # host IP address
    PORT = 7379                                          # host port
    KEY_LIMIT = 100                                      # Max number of keys
    MAX_CLIENTS = 10_000                                 # Max number of clients 
    AOF_FILE = "run-master.aof"                          # File name for AOF Logging
    EVICTION_STRATEGY = "allkeys-random"                 # Eviction strategy name       
    EVICTION_RATIO = 0.25                                # Eviction ratio (% keys evicted)
    DB_COUNT = 4                                         # DB count