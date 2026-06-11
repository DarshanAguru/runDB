from typing import Protocol, List, Any
from .RedisCmd import RedisCmd
import time
from .Store import Store
from multiprocessing import Process
from .RedisObject import RedisObject, REDIS_OBJECT_ENCODINGS, REDIS_OBJECT_TYPES
from .assertions import RedisAssertions
from .encoding import Encoder
from .Stats import Stats
from .Client import Client

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
    def evalAndRespond(cmds: list[RedisCmd], conn: Client) -> Exception | None:
        buffer = bytearray()
        for cmd in cmds:
            db = getattr(conn, "db", 0)
            # If transaction is active, queue the command unless it is a control command
            if hasattr(conn, 'isTrans') and conn.isTrans and cmd.cmd not in ("EXEC", "DISCARD", "MULTI"):
                conn.cqueue.append(cmd)
                res = b"+QUEUED\r\n"
            else:
                if cmd.cmd == "PING":
                    res = Evaluator.__evalPING(cmd.args)
                elif cmd.cmd == "SET":
                    res = Evaluator.__evalSET(db, cmd.args)
                elif cmd.cmd == "GET":
                    res = Evaluator.__evalGET(db, cmd.args)
                elif cmd.cmd == "TTL":
                    res = Evaluator.__evalTTL(db, cmd.args)
                elif cmd.cmd == "DEL":
                    res = Evaluator.__evalDEL(db, cmd.args)
                elif cmd.cmd == "EXPIRE":
                    res = Evaluator.__evalEXPIRE(db, cmd.args)
                elif cmd.cmd == "BGREWRITEAOF":
                    res = Evaluator.__evalBGREWRITEAOF(cmd.args)
                elif cmd.cmd == "INCR":
                    res = Evaluator.__evalINCR(db, cmd.args)
                elif cmd.cmd == "INFO":
                    res = Evaluator.__evalINFO(cmd.args)
                elif cmd.cmd == "CLIENT":
                    res = Evaluator.__evalCLIENT(cmd.args)
                elif cmd.cmd == "LATENCY":
                    res = Evaluator.__evalLATENCY(cmd.args)
                elif cmd.cmd == "SELECT":
                    res = Evaluator.__evalSELECT(conn, cmd.args)
                elif cmd.cmd == "MULTI":
                    res = Evaluator.__evalMULTI(conn, cmd.args)
                elif cmd.cmd == "EXEC":
                    res = Evaluator.__evalEXEC(conn, cmd.args)
                elif cmd.cmd == "DISCARD":
                    res = Evaluator.__evalDISCARD(conn, cmd.args)
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

    # Handles SELECT command to change client database index
    @staticmethod
    def __evalSELECT(conn: Client, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'select' command")
        try:
            db_idx = int(args[0])
        except Exception:
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
        
        from config import Config
        if db_idx < 0 or db_idx >= Config.DB_COUNT:
            return Evaluator.__getErrorResponse("ERR DB index is out of range")
        
        conn.db = db_idx
        return RESP_RESPONSES.RESP_OK

    # Handles SET command with optional EX duration
    @staticmethod
    def __evalSET(db: int, args: List[str]) -> bytes:
        if len(args) <= 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'set' command")

        ex_duration_sec = -1
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
                except Exception:
                    return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
            else:
                return Evaluator.__getErrorResponse("ERR syntax error")
            i += 1

        Store.put(key, RedisObject(value, o_type, o_encoding), ex_duration_sec, db)
        return RESP_RESPONSES.RESP_OK

    # Handles GET command with expiration check
    @staticmethod
    def __evalGET(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'get' command")
        
        key = args[0]
        val = Store.get(key, db)

        if val is None:
            return RESP_RESPONSES.RESP_NIL
        
        if Store.hasExpired(val, db):
            return RESP_RESPONSES.RESP_NIL
        
        return Encoder.encode(val.getValue(), bulk=True)
    
    # Returns remaining TTL for a key
    @staticmethod
    def __evalTTL(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'ttl' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_MINUS_TWO
    
        if Store.getExpiry(val, db) == -1:
            return RESP_RESPONSES.RESP_MINUS_ONE
    
        if Store.hasExpired(val, db):
            return RESP_RESPONSES.RESP_MINUS_TWO
 
        duration_sec = Store.getExpiry(val, db) - int(time.time())
        return Encoder.encode(duration_sec)
    
    # Deletes keys and returns count of deleted items
    @staticmethod
    def __evalDEL(db: int, args: List[str]) -> bytes:
        count_deleted = 0
        for key in args:
            if Store.delete(key, db):
                count_deleted += 1
        return Encoder.encode(count_deleted)
    
    # Sets expiration for an existing key
    @staticmethod
    def __evalEXPIRE(db: int, args: List[str]) -> bytes:
        if len(args) <= 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'expire' command")
        
        key = args[0]
        try:
            ex_duration_sec = int(args[1])
        except Exception:
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
        
        val = Store.get(key, db)
        if val is None:
            return RESP_RESPONSES.RESP_ZERO
        
        Store.setExpiry(val, ex_duration_sec, db)
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
    def __evalINCR(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'incr' command")
        
        key = args[0]
        val = Store.get(key, db)

        if val is None:
            Store.put(key, RedisObject("1", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.INT), -1, db)
            return Encoder.encode(1)
        
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_STRING):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if not RedisAssertions.assertObjectEncoding(val.getEncoding(), REDIS_OBJECT_ENCODINGS.INT):
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
        
        try:
            i = int(val.getValue())
            i += 1
            val.val = str(i)
            exp = Store.getExpiry(val, db)
            Store.put(key, val, exp, db)
            return Encoder.encode(i)
        except (ValueError, TypeError):
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
    
    # Evaluates the INFO command to report keyspace metrics
    @staticmethod
    def __evalINFO(args: List[str]) -> bytes:
        info_str = "# Keyspace\r\n"
        keyspace_stats = Stats.get_keyspace_stats()
        for i, db_stats in enumerate(keyspace_stats):
            info_str += f"db{i}:keys={db_stats['keys']},expires={db_stats['expires']},avg_ttl={db_stats['avg_ttl']}\r\n"
        
        info_str += "# Memory\r\n"
        mem_stats = Stats.getMemoryStats()
        info_str += f"used_memory:{mem_stats['used_memory']} Bytes\r\n"
        info_str += f"max_memory:{mem_stats['max_memory']} Bytes\r\n"
        info_str += f"available_memory:{mem_stats['available_memory']} Bytes\r\n"
        
        return Encoder.encode(info_str, bulk=True)
    
    # TODO: Provides info about client based on what is asked for
    @staticmethod
    def __evalCLIENT(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'client' command")
        
        return RESP_RESPONSES.RESP_OK
    
    # TODO: Provides info related to latency.
    @staticmethod
    def __evalLATENCY(args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'latency' command")
        
        return Encoder.encode([])
    
    @staticmethod
    def __evalMULTI(conn: Client, args: List[str]) -> bytes:
        if conn.isTrans:
            return Evaluator.__getErrorResponse("ERR MULTI calls can't be nested")
        conn.TransBegin()
        return b"+OK\r\n"
    
    @staticmethod
    def __evalEXEC(conn: Client, args: List[str]) -> bytes:
        if not conn.isTrans:
            return Evaluator.__getErrorResponse("ERR EXEC without MULTI")
        return conn.TransExec()
    
    @staticmethod
    def __evalDISCARD(conn: Client, args: List[str]) -> bytes:
        if not conn.isTrans:
            return Evaluator.__getErrorResponse("ERR DISCARD without MULTI")
        conn.TransDiscard()
        return b"+OK\r\n"

    