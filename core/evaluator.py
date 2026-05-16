from typing import Protocol, List, Any
from .RedisCmd import RedisCmd
import time
from .store import Store
from multiprocessing import Process
from .redisObject import RedisObject, REDIS_OBJECT_ENCODINGS, REDIS_OBJECT_TYPES
from .assertions import RedisAssertions
from .encoding import Encoder

class SupportsSend(Protocol):
    def send(self, data: bytes) -> int:
        ...

# Standard RESP responses
class RESP_RESPONSES:
    RESP_MINUS_TWO: bytes = b":-2\r\n"
    RESP_MINUS_ONE: bytes = b":-1\r\n"
    RESP_NIL: bytes = b"$-1\r\n"
    RESP_OK: bytes = b"+OK\r\n"
    RESP_ZERO: bytes = b":0\r\n"
    RESP_ONE: bytes = b":1\r\n"

class Evaluator:
    # Main entry point to evaluate commands and send pipelined responses
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
            elif cmd.cmd == "BGREWRITEAOF":
                res = Evaluator.__evalBGREWRITEAOF(cmd.args)
            elif cmd.cmd == "INCR":
                res = Evaluator.__evalINCR(cmd.args)
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

    # Handles SET command with optional EX duration
    @staticmethod
    def __evalSET(args: List[str]) -> bytes:
        if len(args) <= 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'set' command")

        ex_duration_ms = -1
        key, value = args[0], args[1]
        o_type, o_encoding = Encoder.deduceTypeEncoding(value)

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

        Store.put(key, RedisObject(value, ex_duration_ms, o_type, o_encoding))
        return RESP_RESPONSES.RESP_OK

    # Handles GET command with expiration check
    @staticmethod
    def __evalGET(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'get' command")
        
        key = args[0]
        val = Store.get(key)

        if val is None or val.isExpired():
            return RESP_RESPONSES.RESP_NIL
        
        return Encoder.encode(val.getValue(), bulk=True)
    
    # Returns remaining TTL for a key
    @staticmethod
    def __evalTTL(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'ttl' command")
        
        key = args[0]
        val = Store.get(key)
        
        if val is None:
            return RESP_RESPONSES.RESP_MINUS_TWO
    
        if val.getExpiresAt() == -1:
            return RESP_RESPONSES.RESP_MINUS_ONE
    
        if val.isExpired():
            return RESP_RESPONSES.RESP_MINUS_TWO

        duration_ms = val.getExpiresAt() - time.time() * 1000
        return Encoder.encode(int(duration_ms // 1000))
    
    # Deletes keys and returns count of deleted items
    @staticmethod
    def __evalDEL(args: List[str]) -> bytes:
        count_deleted = 0
        for key in args:
            if Store.delete(key):
                count_deleted += 1
        return Encoder.encode(count_deleted)
    
    # Sets expiration for an existing key
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
            return RESP_RESPONSES.RESP_ZERO
        
        val.setExpiresAt(ex_duration_sec * 1000)
        return RESP_RESPONSES.RESP_ONE
    
    # Background rewrite of the AOF file
    @staticmethod
    def __evalBGREWRITEAOF(args: List[str]) -> bytes:
        from .aof import AOF
        process = Process(target = AOF.dumpAllAOF)
        process.start()
        return RESP_RESPONSES.RESP_OK

    # Connectivity check
    @staticmethod
    def __evalPING(args: List[str]) -> bytes:
        if len(args) >= 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'ping' command")
        
        if len(args) == 0:
            return b"+PONG\r\n"
        else:
            return Encoder.encode(args[0], bulk=True)

    # Increments the integer value of a key
    @staticmethod
    def __evalINCR(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'incr' command")
        
        key = args[0]
        val = Store.get(key)

        if val is None:
            Store.put(key, RedisObject("1", -1, REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.INT))
            return Encoder.encode(1)
        
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_STRING):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if not RedisAssertions.assertObjectEncoding(val.getEncoding(), REDIS_OBJECT_ENCODINGS.INT):
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
        
        try:
            i = int(val.getValue())
            i += 1
            val.val = str(i)
            Store.put(key, val)
            return Encoder.encode(i)
        except (ValueError, TypeError):
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
