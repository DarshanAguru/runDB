from typing import List, Dict
from collections import defaultdict
from config import Config

# Track and manage keyspace statistics across multiple Redis databases
class Stats:    
    # Store list of stats dicts per database, mapping metric names (e.g. "keys") to their integer values
    KeyspaceStat: List[Dict[str, int]] = [defaultdict(int) for _ in range(Config.DB_COUNT)]
    
    # Updates a specific metric value for a database
    @classmethod
    def updateDBstat(cls, num: int, metric: str, val: int):
        cls.KeyspaceStat[num][metric] = val
    
    # Increments a database metric counter by 1
    @classmethod
    def increment(cls, num: int, metric: str):
        cls.KeyspaceStat[num][metric] += 1
    
    # Decrements a database metric counter by 1
    @classmethod
    def decrement(cls, num: int, metric: str):
        cls.KeyspaceStat[num][metric] -= 1
    
    # Retrieves the current value of a database metric counter
    @classmethod
    def getDBstat(cls, num: int, metric: str) -> int:
        return cls.KeyspaceStat[num][metric]