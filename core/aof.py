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
    # Dumps a single key-value pair as a SET/RPUSH command in RESP format
    @staticmethod
    def dumpKey(file: io.BufferedWriter, key: str, value: int, db_idx: int) -> None:
        if Store.hasExpired(value, db_idx):
            return
        
        # Wrap the raw pointer in a temporary RedisObject wrapper
        robj = RedisObject(ptr=value)
        
        from .RedisObject import REDIS_OBJECT_TYPES
        if robj.getType() == REDIS_OBJECT_TYPES.TYPE_LIST:
            elements = list(robj.getValue())
            tokens = ["RPUSH", key] + [el.decode() for el in elements]
            encoded_cmd = Encoder.encode(tokens)
            file.write(encoded_cmd)
        else:
            tokens = ["SET", key, str(robj.getValue())]
            encoded_cmd = Encoder.encode(tokens)
            file.write(encoded_cmd)

        # Check if the key has an expiry set
        expiry = Store.getExpiry(value, db_idx)
        if expiry != -1:
            remaining_ttl = expiry - int(time.time())
            if remaining_ttl > 0:
                expire_cmd = Encoder.encode(["EXPIRE", key, str(remaining_ttl)])
                file.write(expire_cmd)

    # Rewrites the entire AOF file based on current store contents
    @staticmethod
    def dumpAllAOF() -> None:
        try:
            temp_file = Config.AOF_FILE + ".tmp"
            with open(temp_file, 'wb') as file:
                logger.debug(f"Child process rewriting AOF file at {temp_file}")
                for db_idx, store in enumerate(Store.stores):
                    if not store:
                        continue
                    # Write SELECT command to switch to the correct database
                    select_cmd = Encoder.encode(["SELECT", str(db_idx)])
                    file.write(select_cmd)
                    for k, val in store.items():
                        AOF.dumpKey(file, k, val, db_idx)
            
            # Atomic swap of the AOF file
            os.replace(temp_file, Config.AOF_FILE)
            logger.debug("AOF file rewrite complete (background)")
        except Exception as e:
            logger.error(f"Error in background AOF dump: {e}")

    # Loads and restores database state from the AOF file
    @staticmethod
    def loadAllAOF() -> None:
        from server.util import Printer
        if not os.path.exists(Config.AOF_FILE):
            Printer.printAOFEmpty()
            return

        Printer.printAOFLoading(Config.AOF_FILE)
        try:
            with open(Config.AOF_FILE, 'rb') as file:
                data = file.read()

            if not data:
                Printer.printAOFEmpty()
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
            Printer.printAOFRestored(Config.AOF_FILE, saved_time, len(vals) if vals else 0, MemTracker.allocated)
        except Exception as e:
            logger.error(f"Error loading AOF file: {e}")
