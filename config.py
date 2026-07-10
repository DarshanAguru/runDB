
import os

# Global configuration settings for the RunDB server
class Config:
    HOST = os.getenv("RUNDB_HOST", "0.0.0.0")                                    # host IP address
    PORT = int(os.getenv("RUNDB_PORT", "7379"))                                  # host port
    MEMORY_LIMIT = int(os.getenv("RUNDB_MEMORY_LIMIT", str(1 * 1024 * 1024)))    # Max memory limit in bytes (1 MB just for testing)
    MAX_CLIENTS = int(os.getenv("RUNDB_MAX_CLIENTS", "10000"))                   # Max number of clients 
    CRON_FREQ_INTERVAL = float(os.getenv("RUNDB_CRON_FREQ_INTERVAL", "1.0"))     # Periodic Time interval (in secs) for checking for expired keys 
    AOF_FILE = os.getenv("RUNDB_AOF_FILE", "run-master.aof")                     # File name for AOF Logging
    EVICTION_STRATEGY = os.getenv("RUNDB_EVICTION_STRATEGY", "allkeys-lru")      # Eviction strategy name       
    EVICTION_RATIO = float(os.getenv("RUNDB_EVICTION_RATIO", "0.1"))             # Eviction ratio (% keys evicted)
    DB_COUNT = int(os.getenv("RUNDB_DB_COUNT", "4"))                             # DB count
    LRU_BITS_MASK = 0x00FFFFFF                                                   # 24 bit mask for LRU Clock (DONT CHANGE)
    EVICTION_POOL_SIZE = int(os.getenv("RUNDB_EVICTION_POOL_SIZE", "16"))        # Eviction pool size for all-keys LRU eviction strategy
    EVICTION_SAMPLE_SIZE = int(os.getenv("RUNDB_EVICTION_SAMPLE_SIZE", "5"))

    @classmethod
    def load_from_file(cls, filepath: str):
        if not os.path.exists(filepath):
            return
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                # Support "key value" or "key = value"
                if '=' in line:
                    parts = line.split('=', 1)
                else:
                    parts = line.split(None, 1)
                
                if len(parts) != 2:
                    continue
                
                key = parts[0].strip().upper()
                val = parts[1].strip()
                
                # Remove enclosing single/double quotes if present
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val = val[1:-1]
                
                if key == "HOST":
                    cls.HOST = val
                elif key == "PORT":
                    cls.PORT = int(val)
                elif key == "MEMORY_LIMIT":
                    cls.MEMORY_LIMIT = int(val)
                elif key == "MAX_CLIENTS":
                    cls.MAX_CLIENTS = int(val)
                elif key == "CRON_FREQ_INTERVAL":
                    cls.CRON_FREQ_INTERVAL = float(val)
                elif key == "AOF_FILE":
                    cls.AOF_FILE = val
                elif key == "EVICTION_STRATEGY":
                    cls.EVICTION_STRATEGY = val
                elif key == "EVICTION_RATIO":
                    cls.EVICTION_RATIO = float(val)
                elif key == "DB_COUNT":
                    cls.DB_COUNT = int(val)
                elif key == "EVICTION_POOL_SIZE":
                    cls.EVICTION_POOL_SIZE = int(val)
                elif key == "EVICTION_SAMPLE_SIZE":
                    cls.EVICTION_SAMPLE_SIZE = int(val)