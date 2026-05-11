import logging
import io
import os
from config import Config
from .store import Store, Value

logger = logging.getLogger(__name__)

class AOF:

    # Dumps a single key-value pair into the AOF file as a SET command
    @staticmethod
    def dumpKey(file: io.BufferedWriter, key: str, value: Value) -> None:
        # importing inside function to avoid circular imports
        from .evaluator import Evaluator
        tokens = ["SET", key, str(value.getValue())]
        encoded_cmd = Evaluator.encode(tokens)
        file.write(encoded_cmd)

    # Rewrites the entire AOF file based on the current in-memory state
    @staticmethod
    def dumpAllAOF() -> None:
        try:
            # We use a temporary file and then rename it
            temp_file = Config.AOF_FILE + ".tmp"
            with open(temp_file, 'wb') as file:
                logger.info(f"Child process rewriting AOF file at {temp_file}")
                for k, val in Store.store.items():
                    AOF.dumpKey(file, k, val)
            
            # Atomic rename to replace the old AOF file
            os.replace(temp_file, Config.AOF_FILE)
            logger.info("AOF file rewrite complete (background)")
        except Exception as e:
            logger.error(f"Error in background AOF dump: {e}")
