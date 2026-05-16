import logging
import io
import os
from config import Config
from .store import Store
from .redisObject import RedisObject
from .encoding import Encoder

logger = logging.getLogger(__name__)

class AOF:
    # Dumps a single key-value pair as a SET command in RESP format
    @staticmethod
    def dumpKey(file: io.BufferedWriter, key: str, value: RedisObject) -> None:
        # AOF store state as a series of SET commands
        tokens = ["SET", key, str(value.getValue())]
        encoded_cmd = Encoder.encode(tokens)
        file.write(encoded_cmd)

    # Rewrites the entire AOF file based on current store contents
    @staticmethod
    def dumpAllAOF() -> None:
        try:
            temp_file = Config.AOF_FILE + ".tmp"
            with open(temp_file, 'wb') as file:
                logger.info(f"Child process rewriting AOF file at {temp_file}")
                for k, val in Store.store.items():
                    AOF.dumpKey(file, k, val)
            
            # Atomic swap of the AOF file
            os.replace(temp_file, Config.AOF_FILE)
            logger.info("AOF file rewrite complete (background)")
        except Exception as e:
            logger.error(f"Error in background AOF dump: {e}")
