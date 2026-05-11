from typing import Protocol, List, Any
from .RedisCmd import RedisCmd
import time
from .store import Value, Store

class SupportsSend(Protocol):
    def send(self, data: bytes) -> int:
        ...

# RESP Constants
RESP_MINUS_TWO = b":-2\r\n"
RESP_MINUS_ONE = b":-1\r\n"
RESP_NIL = b"$-1\r\n"
RESP_OK = b"+OK\r\n"
RESP_ZERO = b":0\r\n"
RESP_ONE = b":1\r\n"

class Evaluator:
    # Main entry point to evaluate a list of commands and send a single response (pipelining)
    @staticmethod
    def evalAndRespond(cmds: list[RedisCmd], conn: SupportsSend) -> Exception | None:
        buffer = bytearray()
        for cmd in cmds:
            if cmd.cmd == "PING":
                res = Evaluator.__evalPING(cmd.args)
            elif cmd.cmd == "SET":
                res = Evaluator.__evalSET(cmd.args)
            elif cmd.cmd == "GET":
                res = Evaluator.__evalGET(cmd.args)
            elif cmd.cmd == "TTL":
                res = Evaluator.__evalTTL(cmd.args)
            elif cmd.cmd == "DEL":
                res = Evaluator.__evalDEL(cmd.args)
            elif cmd.cmd == "EXPIRE":
                res = Evaluator.__evalEXPIRE(cmd.args)
            else:
                res = Evaluator.__getErrorResponse("ERR unknown command '" + cmd.cmd + "'")
            
            buffer.extend(res)
        
        try:
            conn.send(bytes(buffer))
            return None
        except Exception as err:
            return err
    
    @staticmethod
    def __getErrorResponse(msg: str) -> bytes:
        return f"-{msg}\r\n".encode("utf-8")

    # Handles the SET command with optional EX (expiration in seconds)
    @staticmethod
    def __evalSET(args: List[str]) -> bytes:
        if len(args) <= 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'set' command")

        ex_duration_ms = -1
        key, value = args[0], args[1]

        i = 2
        while i < len(args):
            arg = args[i].lower()
            if arg == "ex":
                i += 1
                if i == len(args):
                    return Evaluator.__getErrorResponse("ERR syntax error")
                try:
                    ex_duration_sec = int(args[i])
                    ex_duration_ms = ex_duration_sec * 1000
                except Exception:
                    return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
            else:
                return Evaluator.__getErrorResponse("ERR syntax error")
            i += 1

        Store.put(key, Value(value, ex_duration_ms))
        return RESP_OK

    # Handles the GET command, checking for existence and expiration
    @staticmethod
    def __evalGET(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'get' command")
        
        key = args[0]
        val = Store.get(key)

        if val is None or val.isExpired():
            return RESP_NIL
        
        return Evaluator.__encode(val.getValue(), bulk=True)
    
    # Returns the remaining Time To Live in seconds for a key
    @staticmethod
    def __evalTTL(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'ttl' command")
        
        key = args[0]
        val = Store.get(key)
        
        if val is None:
            return RESP_MINUS_TWO
    
        if val.getExpiresAt() == -1:
            return RESP_MINUS_ONE
    
        if val.isExpired():
            return RESP_MINUS_TWO

        duration_ms = val.getExpiresAt() - time.time() * 1000
        return Evaluator.__encode(int(duration_ms // 1000))
    
    # Deletes one or more keys and returns the count of deleted keys
    @staticmethod
    def __evalDEL(args: List[str]) -> bytes:
        count_deleted = 0
        for key in args:
            if Store.delete(key):
                count_deleted += 1
        return Evaluator.__encode(count_deleted)
    
    # Sets or updates the expiration of an existing key
    @staticmethod
    def __evalEXPIRE(args: List[str]) -> bytes:
        if len(args) <= 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'expire' command")
        
        key = args[0]
        try:
            ex_duration_sec = int(args[1])
        except Exception:
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
        
        val = Store.get(key)
        if val is None:
            return RESP_ZERO
        
        val.setExpiresAt(ex_duration_sec * 1000)
        return RESP_ONE

    # Simple PING/PONG for connectivity checks
    @staticmethod
    def __evalPING(args: List[str]) -> bytes:
        if len(args) >= 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'ping' command")
        
        if len(args) == 0:
            return b"+PONG\r\n"
        else:
            return Evaluator.__encode(args[0], bulk=True)
    
    # Encodes data into RESP format based on type and flags
    @staticmethod
    def __encode(val: Any, bulk: bool = False) -> bytes:
        if isinstance(val, int):
            return f":{val}\r\n".encode("utf-8")
        
        if bulk:
            return f"${len(val)}\r\n{val}\r\n".encode("utf-8")
        else:
            return f"+{val}\r\n".encode("utf-8")
