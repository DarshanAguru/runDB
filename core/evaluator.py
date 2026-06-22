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

# Evaluator acts as the command dispatching and execution engine:
# - Command Routing: Decodes and routes inbound commands to specific handlers (SET, GET, MULTI, LPUSH, etc.).
# - Transaction Queueing: Queues commands in connection buffer if a transaction (MULTI) is active.
# - Pipelined Responses: Aggregates multiple command responses into a single byte buffer for transmission.
# - Polymorphic Command Logic: Integrates validation assertions for object type/encoding compatibility.
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
                elif cmd.cmd == "LPUSH":
                    res = Evaluator.__evalLPUSH(db, cmd.args)
                elif cmd.cmd == "RPUSH":
                    res = Evaluator.__evalRPUSH(db, cmd.args)
                elif cmd.cmd == "LPOP":
                    res = Evaluator.__evalLPOP(db, cmd.args)
                elif cmd.cmd == "RPOP":
                    res = Evaluator.__evalRPOP(db, cmd.args)
                elif cmd.cmd == "LLEN":
                    res = Evaluator.__evalLLEN(db, cmd.args)
                elif cmd.cmd == "LINDEX":
                    res = Evaluator.__evalLINDEX(db, cmd.args)
                elif cmd.cmd == "LRANGE":
                    res = Evaluator.__evalLRANGE(db, cmd.args)
                elif cmd.cmd == "SADD":
                    res = Evaluator.__evalSADD(db, cmd.args)
                elif cmd.cmd == "SREM":
                    res = Evaluator.__evalSREM(db, cmd.args)
                elif cmd.cmd == "SISMEMBER":
                    res = Evaluator.__evalSISMEMBER(db, cmd.args)
                elif cmd.cmd == "SMEMBERS":
                    res = Evaluator.__evalSMEMBERS(db, cmd.args)
                elif cmd.cmd == "SCARD":
                    res = Evaluator.__evalSCARD(db, cmd.args)
                elif cmd.cmd == "SRANDMEMBER":
                    res = Evaluator.__evalSRANDMEMBER(db, cmd.args)
                elif cmd.cmd == "DEBUG":
                    res = Evaluator.__evalDEBUG(db, cmd.args)
                elif cmd.cmd == "GEOADD":
                    res = Evaluator.__evalGEOADD(db, cmd.args)
                elif cmd.cmd == "GEOPOS":
                    res = Evaluator.__evalGEOPOS(db, cmd.args)
                elif cmd.cmd == "GEODIST":
                    res = Evaluator.__evalGEODIST(db, cmd.args)
                elif cmd.cmd == "GEOSEARCH":
                    res = Evaluator.__evalGEOSEARCH(db, cmd.args)
                elif cmd.cmd == "GEOHASH":
                    res = Evaluator.__evalGEOHASH(db, cmd.args)
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

    @staticmethod
    def __evalLPUSH(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'lpush' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is not None:
            if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
                return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            ql = val.getValue()
        else:
            from .internals.QuickList import QuickList
            ql = QuickList()
            
        for element in args[1:]:
            ql.lpush(element.encode())
            
        if val is None:
            Store.put(key, RedisObject(ql, REDIS_OBJECT_TYPES.TYPE_LIST, REDIS_OBJECT_ENCODINGS.QUICKLIST), -1, db)
        return Encoder.encode(len(ql))

    @staticmethod
    def __evalRPUSH(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'rpush' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is not None:
            if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
                return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            ql = val.getValue()
        else:
            from .internals.QuickList import QuickList
            ql = QuickList()
            
        for element in args[1:]:
            ql.rpush(element.encode())
            
        if val is None:
            Store.put(key, RedisObject(ql, REDIS_OBJECT_TYPES.TYPE_LIST, REDIS_OBJECT_ENCODINGS.QUICKLIST), -1, db)
        return Encoder.encode(len(ql))

    @staticmethod
    def __evalLPOP(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'lpop' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_NIL
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        ql = val.getValue()
        res = ql.lpop()
        
        if len(ql) == 0:
            Store.delete(key, db)
            
        if res is None:
            return RESP_RESPONSES.RESP_NIL
        return Encoder.encode(res.decode(), bulk=True)

    @staticmethod
    def __evalRPOP(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'rpop' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_NIL
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        ql = val.getValue()
        res = ql.rpop()
        
        if len(ql) == 0:
            Store.delete(key, db)
            
        if res is None:
            return RESP_RESPONSES.RESP_NIL
        return Encoder.encode(res.decode(), bulk=True)

    @staticmethod
    def __evalLLEN(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'llen' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_ZERO
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        ql = val.getValue()
        return Encoder.encode(len(ql))

    @staticmethod
    def __evalLINDEX(db: int, args: List[str]) -> bytes:
        if len(args) != 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'lindex' command")
        
        key = args[0]
        try:
            idx = int(args[1])
        except ValueError:
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
            
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_NIL
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        ql = val.getValue()
        try:
            res = ql[idx]
            return Encoder.encode(res.decode(), bulk=True)
        except IndexError:
            return RESP_RESPONSES.RESP_NIL

    @staticmethod
    def __evalLRANGE(db: int, args: List[str]) -> bytes:
        if len(args) != 3:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'lrange' command")
        
        key = args[0]
        try:
            start = int(args[1])
            stop = int(args[2])
        except ValueError:
            return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
            
        val = Store.get(key, db)
        
        if val is None:
            return Encoder.encode([])
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_LIST):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        ql = val.getValue()
        total = len(ql)
        
        if start < 0:
            start = total + start
        if start < 0:
            start = 0
            
        if stop < 0:
            stop = total + stop
        if stop < 0:
            stop = 0
            
        if start > stop or start >= total:
            return Encoder.encode([])
            
        if stop >= total:
            stop = total - 1
            
        res = []
        for i in range(start, stop + 1):
            res.append(ql[i].decode())
            
        return Encoder.encode(res)

    @staticmethod
    def __evalDEBUG(db: int, args: List[str]) -> bytes:
        if len(args) < 2 or args[0].upper() != "OBJECT":
            return Evaluator.__getErrorResponse("ERR unknown subcommand or wrong number of arguments for 'DEBUG'")
        
        key = args[1]
        val = Store.get(key, db)
        if val is None:
            return Evaluator.__getErrorResponse("ERR no such key")
            
        struct = val.val
        ptr_addr = struct.ptr if struct.ptr else 0
        
        enc = val.getEncoding()
        if enc == REDIS_OBJECT_ENCODINGS.RAW:
            encoding_name = "raw"
        elif enc == REDIS_OBJECT_ENCODINGS.INT:
            encoding_name = "int"
        elif enc == REDIS_OBJECT_ENCODINGS.EMBSTR:
            encoding_name = "embstr"
        elif enc == REDIS_OBJECT_ENCODINGS.QUICKLIST:
            encoding_name = "quicklist"
        elif enc == REDIS_OBJECT_ENCODINGS.INTSET:
            encoding_name = "intset"
        elif enc == REDIS_OBJECT_ENCODINGS.HT:
            encoding_name = "hashtable"
        else:
            encoding_name = "unknown"
            
        from config import Config
        current_clock = int(time.time()) & Config.LRU_BITS_MASK
        lru = struct.lat
        idle = (current_clock - lru) & Config.LRU_BITS_MASK
        
        msg = f"Value at:0x{ptr_addr:x} refcount:1 encoding:{encoding_name} serializedlength:{struct.size} lru:{lru} lru_seconds_idle:{idle}"
        return Encoder.encode(msg, bulk=False)

    @staticmethod
    def __evalSADD(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'sadd' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is not None:
            if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
                return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            s = val.getValue()
        else:
            from .internals.Set import Set
            s = Set()
            
        added_count = 0
        for element in args[1:]:
            try:
                if element == "0" or (element.startswith("-") and element[1:].isdigit() and element[1] != "0") or (element.isdigit() and element[0] != "0"):
                    parsed = int(element)
                else:
                    parsed = element
            except ValueError:
                parsed = element
                
            if s.add(parsed):
                added_count += 1
                
        if val is None:
            Store.put(key, RedisObject(s, REDIS_OBJECT_TYPES.TYPE_SET, s.encoding), -1, db)
        else:
            struct = val.val
            old_enc = val.getEncoding()
            if old_enc != s.encoding:
                struct.typeEncoding = ((REDIS_OBJECT_TYPES.TYPE_SET & 0x0F) << 4) | (s.encoding & 0x0F)
                struct.ptr = s.release()
                struct.size = s.underlying.map.size
            else:
                struct.ptr = s.release()
                struct.size = s.underlying.size if s.encoding == 11 else s.underlying.map.size
            Store.put(key, val, Store.getExpiry(val, db), db)
            
        return Encoder.encode(added_count)

    @staticmethod
    def __evalSREM(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'srem' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_ZERO
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        s = val.getValue()
        removed_count = 0
        for element in args[1:]:
            try:
                if element == "0" or (element.startswith("-") and element[1:].isdigit() and element[1] != "0") or (element.isdigit() and element[0] != "0"):
                    parsed = int(element)
                else:
                    parsed = element
            except ValueError:
                parsed = element
                
            if s.remove(parsed):
                removed_count += 1
                
        if len(s) == 0:
            Store.delete(key, db)
        else:
            struct = val.val
            struct.ptr = s.release()
            struct.size = s.underlying.size if s.encoding == 11 else s.underlying.map.size
            Store.put(key, val, Store.getExpiry(val, db), db)
            
        return Encoder.encode(removed_count)

    @staticmethod
    def __evalSISMEMBER(db: int, args: List[str]) -> bytes:
        if len(args) != 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'sismember' command")
        
        key = args[0]
        member = args[1]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_ZERO
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        s = val.getValue()
        try:
            if member == "0" or (member.startswith("-") and member[1:].isdigit() and member[1] != "0") or (member.isdigit() and member[0] != "0"):
                parsed = int(member)
            else:
                parsed = member
        except ValueError:
            parsed = member
            
        if parsed in s:
            return RESP_RESPONSES.RESP_ONE
        else:
            return RESP_RESPONSES.RESP_ZERO

    @staticmethod
    def __evalSMEMBERS(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'smembers' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return Encoder.encode([])
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        s = val.getValue()
        members = [str(m) for m in s]
        return Encoder.encode(members)

    @staticmethod
    def __evalSCARD(db: int, args: List[str]) -> bytes:
        if len(args) != 1:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'scard' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        if val is None:
            return RESP_RESPONSES.RESP_ZERO
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        s = val.getValue()
        return Encoder.encode(len(s))

    @staticmethod
    def __evalSRANDMEMBER(db: int, args: List[str]) -> bytes:
        if len(args) < 1 or len(args) > 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'srandmember' command")
        
        key = args[0]
        val = Store.get(key, db)
        
        has_count = len(args) == 2
        count = 0
        if has_count:
            try:
                count = int(args[1])
            except ValueError:
                return Evaluator.__getErrorResponse("ERR value is not an integer or is out of range")
                
        if val is None:
            return RESP_RESPONSES.RESP_NIL if not has_count else Encoder.encode([])
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_SET):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        s = val.getValue()
        if len(s) == 0:
            return RESP_RESPONSES.RESP_NIL if not has_count else Encoder.encode([])
            
        if not has_count:
            res = s.get_random_member()
            return Encoder.encode(str(res), bulk=True)
        else:
            res = []
            if count >= 0:
                count = min(count, len(s))
                members = list(s)
                import random
                sampled = random.sample(members, count)
                res = [str(m) for m in sampled]
            else:
                count = abs(count)
                members = list(s)
                import random
                for _ in range(count):
                    res.append(str(random.choice(members)))
            return Encoder.encode(res)

    @staticmethod
    def __toBase32Geohash(lon: float, lat: float) -> str:
        from .internals.Geohash import GeoHashHelper
        return GeoHashHelper.toBase32Geohash(lon, lat)

    @staticmethod
    def __evalGEOADD(db: int, args: List[str]) -> bytes:
        from .internals.Geohash import GeoHashHelper
        if len(args) < 4 or (len(args) - 1) % 3 != 0:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'geoadd' command")
        
        key = args[0]
        # Validate coordinates first
        triples = []
        for i in range(1, len(args), 3):
            try:
                lon = float(args[i])
                lat = float(args[i+1])
            except ValueError:
                return Evaluator.__getErrorResponse("ERR value is not a valid float")
            
            # Check range
            if lon < GeoHashHelper.LON_MIN or lon > GeoHashHelper.LON_MAX or lat < GeoHashHelper.LAT_MIN or lat > GeoHashHelper.LAT_MAX:
                return Evaluator.__getErrorResponse(f"ERR invalid longitude,latitude pair {lon:.6f},{lat:.6f}")
            
            member = args[i+2]
            triples.append((lon, lat, member))
            
        val = Store.get(key, db)
        if val is not None:
            if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_GEO):
                return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            hm = val.getValue()
        else:
            from .internals.HashMap import HashMap
            hm = HashMap("string", "int64")
            
        from .internals.Geohash import GeoHashStruct
        from .internals.Malloc_internal import MallocInternal
        import ctypes
        
        added_count = 0
        for lon, lat, member in triples:
            old_ptr = hm.get(member)
            is_new = old_ptr is None
            if not is_new:
                # Reuse/Overwrite: free the old pointer first
                MallocInternal.zfree(old_ptr)
            else:
                added_count += 1
                
            new_ptr = MallocInternal.zcalloc(ctypes.sizeof(GeoHashStruct))
            struct_obj = ctypes.cast(new_ptr, ctypes.POINTER(GeoHashStruct)).contents
            struct_obj.lat = lat
            struct_obj.lon = lon
            
            hm.set(member, new_ptr)
            
        if val is None:
            Store.put(key, RedisObject(hm, REDIS_OBJECT_TYPES.TYPE_GEO, REDIS_OBJECT_ENCODINGS.HT), -1, db)
        else:
            struct = val.val
            struct.ptr = hm.release()
            struct.size = hm.size
            Store.put(key, val, Store.getExpiry(val, db), db)
            
        return Encoder.encode(added_count)

    @staticmethod
    def __evalGEOPOS(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'geopos' command")
        
        key = args[0]
        val = Store.get(key, db)
        if val is None:
            res = [None] * (len(args) - 1)
            return Encoder.encode(res)
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_GEO):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        hm = val.getValue()
        from .internals.Geohash import GeoHashStruct
        import ctypes
        
        res = []
        for member in args[1:]:
            ptr = hm.get(member)
            if ptr is None:
                res.append(None)
            else:
                struct_obj = ctypes.cast(ptr, ctypes.POINTER(GeoHashStruct)).contents
                res.append([f"{struct_obj.lon:.6f}", f"{struct_obj.lat:.6f}"])
                
        return Encoder.encode(res)

    @staticmethod
    def __evalGEODIST(db: int, args: List[str]) -> bytes:
        if len(args) < 3 or len(args) > 4:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'geodist' command")
            
        key = args[0]
        member1 = args[1]
        member2 = args[2]
        
        unit = "m"
        if len(args) == 4:
            unit = args[3].lower()
            
        units = {
            "m": 1.0,
            "km": 1000.0,
            "ft": 0.3048,
            "mi": 1609.34
        }
        if unit not in units:
            return Evaluator.__getErrorResponse("ERR unsupported unit provided. please use M, KM, FT, MI")
            
        val = Store.get(key, db)
        if val is None:
            return RESP_RESPONSES.RESP_NIL
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_GEO):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        hm = val.getValue()
        ptr1 = hm.get(member1)
        ptr2 = hm.get(member2)
        if ptr1 is None or ptr2 is None:
            return RESP_RESPONSES.RESP_NIL
            
        from .internals.Geohash import GeoHashStruct, GeoHashHelper
        import ctypes
        
        struct1 = ctypes.cast(ptr1, ctypes.POINTER(GeoHashStruct)).contents
        struct2 = ctypes.cast(ptr2, ctypes.POINTER(GeoHashStruct)).contents
        
        dist = GeoHashHelper.geohashGetDistance(struct1.lon, struct1.lat, struct2.lon, struct2.lat)
        converted = dist / units[unit]
        
        return Encoder.encode(f"{converted:.4f}", bulk=True)

    @staticmethod
    def __evalGEOHASH(db: int, args: List[str]) -> bytes:
        if len(args) < 2:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'geohash' command")
            
        key = args[0]
        val = Store.get(key, db)
        if val is None:
            return Encoder.encode([None] * (len(args) - 1))
            
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_GEO):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
            
        hm = val.getValue()
        from .internals.Geohash import GeoHashStruct
        import ctypes
        
        res = []
        for member in args[1:]:
            ptr = hm.get(member)
            if ptr is None:
                res.append(None)
            else:
                struct_obj = ctypes.cast(ptr, ctypes.POINTER(GeoHashStruct)).contents
                hash_str = Evaluator.__toBase32Geohash(struct_obj.lon, struct_obj.lat)
                res.append(hash_str)
                
        return Encoder.encode(res)

    @staticmethod
    def __evalGEOSEARCH(db: int, args: List[str]) -> bytes:
        from .internals.Geohash import GeoHashStruct, GeoHashHelper, GeoHashRange, GeoHashBits
        import ctypes
        import math

        # Validate arguments length
        if len(args) < 5:
            return Evaluator.__getErrorResponse("ERR wrong number of arguments for 'geosearch' command")

        key = args[0]
        val = Store.get(key, db)
        if val is None:
            return Encoder.encode([]) # Key doesn't exist, return empty list

        # Ensure object type is TYPE_GEO
        if not RedisAssertions.assertObjectType(val.getType(), REDIS_OBJECT_TYPES.TYPE_GEO):
            return Evaluator.__getErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")

        hm = val.getValue()

        # 1. Parse center point option: FROMMEMBER or FROMLONLAT
        args_lower = [a.lower() for a in args]
        
        center_lon = None
        center_lat = None
        
        if "frommember" in args_lower:
            m_idx = args_lower.index("frommember")
            if m_idx + 1 >= len(args):
                return Evaluator.__getErrorResponse("ERR syntax error")
            member_name = args[m_idx + 1]
            member_ptr = hm.get(member_name)
            if member_ptr is None:
                return Evaluator.__getErrorResponse("ERR could not decode requested zset member")
            struct_obj = ctypes.cast(member_ptr, ctypes.POINTER(GeoHashStruct)).contents
            center_lon = struct_obj.lon
            center_lat = struct_obj.lat
        elif "fromlonlat" in args_lower:
            l_idx = args_lower.index("fromlonlat")
            if l_idx + 2 >= len(args):
                return Evaluator.__getErrorResponse("ERR syntax error")
            try:
                center_lon = float(args[l_idx + 1])
                center_lat = float(args[l_idx + 2])
            except ValueError:
                return Evaluator.__getErrorResponse("ERR value is not a valid float")
            
            # Validate coordinates range
            if center_lon < -180.0 or center_lon > 180.0 or \
               center_lat < GeoHashHelper.LAT_MIN or center_lat > GeoHashHelper.LAT_MAX:
                return Evaluator.__getErrorResponse("ERR invalid longitude,latitude pair")
        else:
            return Evaluator.__getErrorResponse("ERR syntax error")

        # 2. Parse search shape: BYRADIUS or BYBOX
        is_radius = False
        is_box = False
        radius = 0.0
        width = 0.0
        height = 0.0
        unit = None
        
        units = {"m": 1.0, "km": 1000.0, "ft": 0.3048, "mi": 1609.34}
        
        if "byradius" in args_lower:
            is_radius = True
            r_idx = args_lower.index("byradius")
            if r_idx + 2 >= len(args):
                return Evaluator.__getErrorResponse("ERR syntax error")
            try:
                radius = float(args[r_idx + 1])
            except ValueError:
                return Evaluator.__getErrorResponse("ERR value is not a valid float")
            unit = args_lower[r_idx + 2]
            if unit not in units:
                return Evaluator.__getErrorResponse("ERR unsupported unit")
        elif "bybox" in args_lower:
            is_box = True
            b_idx = args_lower.index("bybox")
            if b_idx + 3 >= len(args):
                return Evaluator.__getErrorResponse("ERR syntax error")
            try:
                width = float(args[b_idx + 1])
                height = float(args[b_idx + 2])
            except ValueError:
                return Evaluator.__getErrorResponse("ERR value is not a valid float")
            unit = args_lower[b_idx + 3]
            if unit not in units:
                return Evaluator.__getErrorResponse("ERR unsupported unit")
        else:
            return Evaluator.__getErrorResponse("ERR syntax error")

        # 3. Parse options: ASC/DESC, COUNT count [ANY], WITHCOORD, WITHDIST, WITHHASH
        withdist = False
        withhash = False
        withcoord = False
        sort_order = None
        count_limit = None
        
        i = 1
        while i < len(args):
            arg_lower = args_lower[i]
            if arg_lower == "frommember":
                i += 2
            elif arg_lower == "fromlonlat":
                i += 3
            elif arg_lower == "byradius":
                i += 3
            elif arg_lower == "bybox":
                i += 4
            elif arg_lower == "withdist":
                withdist = True
                i += 1
            elif arg_lower == "withhash":
                withhash = True
                i += 1
            elif arg_lower == "withcoord":
                withcoord = True
                i += 1
            elif arg_lower == "asc":
                sort_order = "ASC"
                i += 1
            elif arg_lower == "desc":
                sort_order = "DESC"
                i += 1
            elif arg_lower == "count":
                if i + 1 >= len(args):
                    return Evaluator.__getErrorResponse("ERR syntax error")
                try:
                    count_limit = int(args[i + 1])
                except ValueError:
                    return Evaluator.__getErrorResponse("ERR value is not an integer or out of range")
                if count_limit <= 0:
                    return Evaluator.__getErrorResponse("ERR COUNT must be greater than 0")
                i += 2
                if i < len(args) and args_lower[i] == "any":
                    i += 1
            else:
                i += 1

        matches = []
        R = 6372797.560856
        
        for member, ptr in hm.items():
            struct_obj = ctypes.cast(ptr, ctypes.POINTER(GeoHashStruct)).contents
            
            # Calculate distance between center and member
            dist = GeoHashHelper.geohashGetDistance(center_lon, center_lat, struct_obj.lon, struct_obj.lat)
            
            inside = False
            if is_radius:
                inside = (dist <= radius * units[unit])
            elif is_box:
                # Bounding box filter:
                # 1. Latitude check
                lat_dist = abs(struct_obj.lat - center_lat) * (math.pi / 180.0) * R
                # 2. Longitude check with wrap-around
                diff_lon = abs(struct_obj.lon - center_lon)
                if diff_lon > 180.0:
                    diff_lon = 360.0 - diff_lon
                lon_dist = diff_lon * (math.pi / 180.0) * R * math.cos(math.radians(center_lat))
                
                inside = (lat_dist <= (height * units[unit]) / 2.0) and (lon_dist <= (width * units[unit]) / 2.0)
                
            if inside:
                score = 0
                if withhash:
                    r_long = GeoHashRange(min=GeoHashHelper.LON_MIN, max=GeoHashHelper.LON_MAX)
                    r_lat = GeoHashRange(min=GeoHashHelper.LAT_MIN, max=GeoHashHelper.LAT_MAX)
                    bits = GeoHashBits()
                    GeoHashHelper.geohashEncode(ctypes.byref(r_long), ctypes.byref(r_lat), struct_obj.lon, struct_obj.lat, 26, ctypes.byref(bits))
                    score = bits.bits
                matches.append({
                    "member": member,
                    "dist": dist / units[unit],
                    "score": score,
                    "lon": struct_obj.lon,
                    "lat": struct_obj.lat
                })

        # Apply sorting
        if sort_order == "ASC":
            matches.sort(key=lambda x: x["dist"])
        elif sort_order == "DESC":
            matches.sort(key=lambda x: x["dist"], reverse=True)
            
        # Apply count limit
        if count_limit is not None:
            matches = matches[:count_limit]

        # Format results
        res = []
        for m in matches:
            if not (withdist or withhash or withcoord):
                res.append(m["member"])
            else:
                item = [m["member"]]
                if withdist:
                    item.append(f"{m['dist']:.4f}")
                if withhash:
                    item.append(m["score"])
                if withcoord:
                    item.append([f"{m['lon']:.6f}", f"{m['lat']:.6f}"])
                res.append(item)
                
        return Encoder.encode(res)


    