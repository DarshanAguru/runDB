import logging
import io
import os
import time
from config import Config
from .Store import Store
from .RedisObject import RedisObject
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
                logger.debug(f"Child process rewriting AOF file at {temp_file}")
                for k, val in Store.store.items():
                    AOF.dumpKey(file, k, val)
            
            # Atomic swap of the AOF file
            os.replace(temp_file, Config.AOF_FILE)
            logger.debug("AOF file rewrite complete (background)")
        except Exception as e:
            logger.error(f"Error in background AOF dump: {e}")

    # Loads and restores database state from the AOF file
    @staticmethod
    def loadAllAOF() -> None:
        if not os.path.exists(Config.AOF_FILE):
            logger.info("No AOF file found. Starting with empty database.")
            return

        logger.info(f"Loading database from AOF file: {Config.AOF_FILE}")
        try:
            with open(Config.AOF_FILE, 'rb') as file:
                data = file.read()

            if not data:
                logger.info("AOF file is empty.")
                return

            from .resp import RESPProcessor
            from .RedisCmd import RedisCmd
            from .evaluator import Evaluator

            # Dummy connection to satisfy the evaluator's output interface
            class DummyConnection:
                def send(self, data: bytes) -> int:
                    return len(data)

            dummy_conn = DummyConnection()

            vals, consumed, err = RESPProcessor.decode(data)
            if err is not None and consumed == 0:
                logger.error(f"Error parsing AOF file: {err}")
                return

            if vals:
                cmds = []
                for v in vals:
                    if not isinstance(v, list) or len(v) == 0:
                        continue
                    cmds.append(RedisCmd(
                        str(v[0]).upper(),
                        [str(arg) for arg in v[1:]]
                    ))

                if cmds:
                    eval_err = Evaluator.evalAndRespond(cmds, dummy_conn)
                    if eval_err is not None:
                        logger.error(f"Error restoring AOF commands: {eval_err}")
                        return
            
            # Extract statistics for logs
            from .internals.Malloc_internal import MemTracker
            mtime = os.path.getmtime(Config.AOF_FILE)
            saved_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
            mem_mb = MemTracker.allocated / (1024 * 1024)
            logger.info(f"Restored from AOF file: {Config.AOF_FILE}")
            logger.info(f"Checkpoint: AOF file saved time was {saved_time}")
            logger.info(f"Successfully restored/executed {len(vals) if vals else 0} commands from the AOF checkpoint.")
            logger.info(f"Memory restored: {MemTracker.allocated} bytes ({mem_mb:.4f} MB).")
        except Exception as e:
            logger.error(f"Error loading AOF file: {e}")
